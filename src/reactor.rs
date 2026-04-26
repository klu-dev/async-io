use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::panic;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::task::{ready, Context, Poll, Waker};
use std::time::{Duration, Instant};

use concurrent_queue::ConcurrentQueue;
use polling::{Event, Events, Poller};
use slab::Slab;

// Choose the proper implementation of `Registration` based on the target platform.
cfg_if::cfg_if! {
    if #[cfg(windows)] {
        mod windows;
        pub use windows::Registration;
    } else if #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))] {
        mod kqueue;
        pub use kqueue::Registration;
    } else if #[cfg(unix)] {
        mod unix;
        pub use unix::Registration;
    } else {
        compile_error!("unsupported platform");
    }
}

#[cfg(not(target_os = "espidf"))]
const TIMER_QUEUE_SIZE: usize = 1000;

/// ESP-IDF - being an embedded OS - does not need so many timers
/// and this saves ~ 20K RAM which is a lot for an MCU with RAM < 400K
#[cfg(target_os = "espidf")]
const TIMER_QUEUE_SIZE: usize = 100;

const READ: usize = 0;
const WRITE: usize = 1;

/// The reactor.
///
/// There is only one global instance of this type, accessible by [`Reactor::get()`].
pub(crate) struct Reactor {
    /// Portable bindings to epoll/kqueue/event ports/IOCP.
    ///
    /// This is where I/O is polled, producing I/O events.
    pub(crate) poller: Poller,

    /// Ticker bumped before polling.
    ///
    /// This is useful for checking what is the current "round" of `ReactorLock::react()` when
    /// synchronizing things in `Source::readable()` and `Source::writable()`. Both of those
    /// methods must make sure they don't receive stale I/O events - they only accept events from a
    /// fresh "round" of `ReactorLock::react()`.
    ticker: AtomicUsize,

    /// Registered sources.
    sources: Mutex<Slab<Arc<Source>>>,

    /// Temporary storage for I/O events when polling the reactor.
    ///
    /// Holding a lock on this event list implies the exclusive right to poll I/O.
    events: Mutex<Events>,

    /// An ordered map of registered timers.
    ///
    /// Timers are in the order in which they fire. The `usize` in this type is a timer ID used to
    /// distinguish timers that fire at the same time. The `Waker` represents the task awaiting the
    /// timer.
    timers: Mutex<BTreeMap<(Instant, usize), Waker>>,

    /// A queue of timer operations (insert and remove).
    ///
    /// When inserting or removing a timer, we don't process it immediately - we just push it into
    /// this queue. Timers actually get processed when the queue fills up or the reactor is polled.
    timer_ops: ConcurrentQueue<TimerOp>,
}

impl Reactor {
    /// Returns a reference to the reactor.
    pub(crate) fn get() -> &'static Reactor {
        static REACTOR: OnceLock<Reactor> = OnceLock::new();

        REACTOR.get_or_init(|| {
            crate::driver::init();
            Reactor {
                poller: Poller::new().expect("cannot initialize I/O event notification"),
                ticker: AtomicUsize::new(0),
                sources: Mutex::new(Slab::new()),
                events: Mutex::new(Events::new()),
                timers: Mutex::new(BTreeMap::new()),
                timer_ops: ConcurrentQueue::bounded(TIMER_QUEUE_SIZE),
            }
        })
    }

    /// Returns the current ticker.
    pub(crate) fn ticker(&self) -> usize {
        self.ticker.load(Ordering::SeqCst)
    }

    /// Registers an I/O source in the reactor.
    pub(crate) fn insert_io(&self, raw: Registration) -> io::Result<Arc<Source>> {
        // Create an I/O source for this file descriptor.
        let source = {
            let mut sources = self.sources.lock().unwrap();
            let key = sources.vacant_entry().key();
            let source = Arc::new(Source {
                registration: raw,
                key,
                state: Default::default(),
                #[cfg(windows)]
                wake_mode: WakeMode::Level,
            });
            sources.insert(source.clone());
            source
        };

        // Register the file descriptor.
        if let Err(err) = source.registration.add(&self.poller, source.key) {
            let mut sources = self.sources.lock().unwrap();
            sources.remove(source.key);
            return Err(err);
        }

        Ok(source)
    }

    /// Registers an I/O source with `WakeMode::Edge` semantics. Windows-only:
    /// edge-triggered readiness only matters for IOCP-backed sources (named pipes).
    #[cfg(windows)]
    pub(crate) fn insert_io_edge(&self, raw: Registration) -> io::Result<Arc<Source>> {
        let source = {
            let mut sources = self.sources.lock().unwrap();
            let key = sources.vacant_entry().key();
            let source = Arc::new(Source {
                registration: raw,
                key,
                state: Default::default(),
                wake_mode: WakeMode::Edge,
            });
            sources.insert(source.clone());
            source
        };

        if let Err(err) = source.registration.add(&self.poller, source.key) {
            let mut sources = self.sources.lock().unwrap();
            sources.remove(source.key);
            return Err(err);
        }

        Ok(source)
    }

    /// Deregisters an I/O source from the reactor.
    pub(crate) fn remove_io(&self, source: &Source) -> io::Result<()> {
        let mut sources = self.sources.lock().unwrap();
        sources.remove(source.key);
        source.registration.delete(&self.poller)
    }

    /// Registers a timer in the reactor.
    ///
    /// Returns the inserted timer's ID.
    pub(crate) fn insert_timer(&self, when: Instant, waker: &Waker) -> usize {
        // Generate a new timer ID.
        static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);
        let id = ID_GENERATOR.fetch_add(1, Ordering::Relaxed);

        // Push an insert operation.
        while self
            .timer_ops
            .push(TimerOp::Insert(when, id, waker.clone()))
            .is_err()
        {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.lock().unwrap();
            self.process_timer_ops(&mut timers);
        }

        // Notify that a timer has been inserted.
        self.notify();

        id
    }

    /// Deregisters a timer from the reactor.
    pub(crate) fn remove_timer(&self, when: Instant, id: usize) {
        // Push a remove operation.
        while self.timer_ops.push(TimerOp::Remove(when, id)).is_err() {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.lock().unwrap();
            self.process_timer_ops(&mut timers);
        }
    }

    /// Notifies the thread blocked on the reactor.
    pub(crate) fn notify(&self) {
        self.poller.notify().expect("failed to notify reactor");
    }

    /// Locks the reactor, potentially blocking if the lock is held by another thread.
    pub(crate) fn lock(&self) -> ReactorLock<'_> {
        let reactor = self;
        let events = self.events.lock().unwrap();
        ReactorLock { reactor, events }
    }

    /// Attempts to lock the reactor.
    pub(crate) fn try_lock(&self) -> Option<ReactorLock<'_>> {
        self.events.try_lock().ok().map(|events| {
            let reactor = self;
            ReactorLock { reactor, events }
        })
    }

    /// Processes ready timers and extends the list of wakers to wake.
    ///
    /// Returns the duration until the next timer before this method was called.
    fn process_timers(&self, wakers: &mut Vec<Waker>) -> Option<Duration> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!("process_timers");
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        let mut timers = self.timers.lock().unwrap();
        self.process_timer_ops(&mut timers);

        let now = Instant::now();

        // Split timers into ready and pending timers.
        //
        // Careful to split just *after* `now`, so that a timer set for exactly `now` is considered
        // ready.
        let pending = timers.split_off(&(now + Duration::from_nanos(1), 0));
        let ready = mem::replace(&mut *timers, pending);

        // Calculate the duration until the next event.
        let dur = if ready.is_empty() {
            // Duration until the next timer.
            timers
                .keys()
                .next()
                .map(|(when, _)| when.saturating_duration_since(now))
        } else {
            // Timers are about to fire right now.
            Some(Duration::from_secs(0))
        };

        // Drop the lock before waking.
        drop(timers);

        // Add wakers to the list.
        #[cfg(feature = "tracing")]
        tracing::trace!("{} ready wakers", ready.len());

        for (_, waker) in ready {
            wakers.push(waker);
        }

        dur
    }

    /// Processes queued timer operations.
    fn process_timer_ops(&self, timers: &mut MutexGuard<'_, BTreeMap<(Instant, usize), Waker>>) {
        // Process only as much as fits into the queue, or else this loop could in theory run
        // forever.
        self.timer_ops
            .try_iter()
            .take(self.timer_ops.capacity().unwrap())
            .for_each(|op| match op {
                TimerOp::Insert(when, id, waker) => {
                    timers.insert((when, id), waker);
                }
                TimerOp::Remove(when, id) => {
                    timers.remove(&(when, id));
                }
            });
    }
}

/// A lock on the reactor.
pub(crate) struct ReactorLock<'a> {
    reactor: &'a Reactor,
    events: MutexGuard<'a, Events>,
}

impl ReactorLock<'_> {
    /// Processes new events, blocking until the first event or the timeout.
    pub(crate) fn react(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!("react");
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        let mut wakers = Vec::new();

        // Process ready timers.
        let next_timer = self.reactor.process_timers(&mut wakers);

        // compute the timeout for blocking on I/O events.
        let timeout = match (next_timer, timeout) {
            (None, None) => None,
            (Some(t), None) | (None, Some(t)) => Some(t),
            (Some(a), Some(b)) => Some(a.min(b)),
        };

        // Bump the ticker before polling I/O.
        let tick = self
            .reactor
            .ticker
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);

        self.events.clear();

        // Block on I/O events.
        let res = match self.reactor.poller.wait(&mut self.events, timeout) {
            // No I/O events occurred.
            Ok(0) => {
                if timeout != Some(Duration::from_secs(0)) {
                    // The non-zero timeout was hit so fire ready timers.
                    self.reactor.process_timers(&mut wakers);
                }
                Ok(())
            }

            // At least one I/O event occurred.
            Ok(_) => {
                // Iterate over sources in the event list.
                let sources = self.reactor.sources.lock().unwrap();

                for ev in self.events.iter() {
                    // Check if there is a source in the table with this key.
                    if let Some(source) = sources.get(ev.key) {
                        let mut state = source.state.lock().unwrap();

                        // Collect wakers if any event was emitted.
                        for &(dir, emitted) in &[(WRITE, ev.writable), (READ, ev.readable)] {
                            if emitted {
                                state[dir].tick = tick;
                                // Monotonic per-direction delivery counter. Used by `WakeMode::Edge`
                                // sources to detect delivery without depending on the global ticker,
                                // which can alias across same-cycle race windows.
                                state[dir].events = state[dir].events.wrapping_add(1);
                                state[dir].drain_into(&mut wakers);
                            }
                        }

                        // Re-register if there are still writers or readers. This can happen if
                        // e.g. we were previously interested in both readability and writability,
                        // but only one of them was emitted.
                        if !state[READ].is_empty() || !state[WRITE].is_empty() {
                            // Create the event that we are interested in.
                            let event = {
                                let mut event = Event::none(source.key);
                                event.readable = !state[READ].is_empty();
                                event.writable = !state[WRITE].is_empty();
                                event
                            };

                            // Register interest in this event.
                            source.registration.modify(&self.reactor.poller, event)?;
                        }
                    }
                }

                Ok(())
            }

            // The syscall was interrupted.
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(()),

            // An actual error occureed.
            Err(err) => Err(err),
        };

        // Wake up ready tasks.
        #[cfg(feature = "tracing")]
        tracing::trace!("{} ready wakers", wakers.len());
        for waker in wakers {
            // Don't let a panicking waker blow everything up.
            panic::catch_unwind(|| waker.wake()).ok();
        }

        res
    }
}

/// A single timer operation.
enum TimerOp {
    Insert(Instant, usize, Waker),
    Remove(Instant, usize),
}

/// Indicates how a `Source` reports readiness in `Source::poll_ready()`.
///
/// On non-Windows targets every source is implicitly `Level` and the variant
/// is supplied by `Source::wake_mode()` without storing a per-source field.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum WakeMode {
    /// Events are reported as long as the underlying source is ready (sockets on epoll/kqueue,
    /// the default for this crate). Readiness uses the global reactor tick: a freshly registered
    /// poller becomes ready when the source's `tick` advances past the value captured at
    /// registration.
    Level,

    /// Events are delivered exactly once per readiness transition (Windows IOCP completion
    /// packets for named pipes). Readiness uses a per-direction monotonic delivery counter
    /// (`Direction::events`) instead of the global reactor tick. The counter is incremented only
    /// by `ReactorLock::react()` on actual delivery, so it cannot alias with other reactor work
    /// in the same cycle the way `tick` can.
    ///
    /// Only constructible on Windows; non-Windows code paths cannot observe this variant.
    #[cfg(windows)]
    Edge,
}

/// A registered source of I/O events.
#[derive(Debug)]
pub(crate) struct Source {
    /// This source's registration into the reactor.
    registration: Registration,

    /// The key of this source obtained during registration.
    key: usize,

    /// Inner state with registered wakers.
    state: Mutex<[Direction; 2]>,

    /// Indicates how I/O events are emitted for this source. Only stored on
    /// Windows where IOCP-backed sources may opt into edge-triggered semantics;
    /// on other platforms every source is `WakeMode::Level` so the field is elided.
    #[cfg(windows)]
    wake_mode: WakeMode,
}

/// A read or write direction.
#[derive(Debug, Default)]
struct Direction {
    /// Last reactor tick that delivered an event.
    tick: usize,

    /// Monotonic count of events delivered for this direction by `ReactorLock::react()`.
    /// Used by `WakeMode::Edge` to detect delivery race-free.
    events: u64,

    /// Ticks remembered by `Async::poll_readable()` or `Async::poll_writable()` (level mode).
    ticks: Option<(usize, usize)>,

    /// Event count remembered by `Async::poll_readable()` or `Async::poll_writable()` (edge mode).
    captured_events: Option<u64>,

    /// Waker stored by `Async::poll_readable()` or `Async::poll_writable()`.
    waker: Option<Waker>,

    /// Wakers of tasks waiting for the next event.
    ///
    /// Registered by `Async::readable()` and `Async::writable()`.
    wakers: Slab<Option<Waker>>,
}

impl Direction {
    /// Returns `true` if there are no wakers interested in this direction.
    fn is_empty(&self) -> bool {
        self.waker.is_none() && self.wakers.iter().all(|(_, opt)| opt.is_none())
    }

    /// Moves all wakers into a `Vec`.
    fn drain_into(&mut self, dst: &mut Vec<Waker>) {
        if let Some(w) = self.waker.take() {
            dst.push(w);
        }
        for (_, opt) in self.wakers.iter_mut() {
            if let Some(w) = opt.take() {
                dst.push(w);
            }
        }
    }
}

impl Source {
    /// Returns the wake mode for this source. Always `Level` on non-Windows targets;
    /// the per-source field only exists on Windows (see [`Source::wake_mode`]).
    #[inline]
    fn wake_mode(&self) -> WakeMode {
        #[cfg(windows)]
        {
            self.wake_mode
        }
        #[cfg(not(windows))]
        {
            WakeMode::Level
        }
    }

    /// Polls the I/O source for readability.
    pub(crate) fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_ready(READ, cx)
    }

    /// Polls the I/O source for writability.
    pub(crate) fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_ready(WRITE, cx)
    }

    /// Registers a waker from `poll_readable()` or `poll_writable()`.
    ///
    /// If a different waker is already registered, it gets replaced and woken.
    fn poll_ready(&self, dir: usize, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();

        // Check if the reactor has delivered an event since registration.
        if Self::check_ready(&state[dir], self.wake_mode()) {
            state[dir].ticks = None;
            state[dir].captured_events = None;
            return Poll::Ready(Ok(()));
        }

        let was_empty = state[dir].is_empty();

        // Register the current task's waker.
        if let Some(w) = state[dir].waker.take() {
            if w.will_wake(cx.waker()) {
                state[dir].waker = Some(w);
                return Poll::Pending;
            }
            // Wake the previous waker because it's going to get replaced.
            panic::catch_unwind(|| w.wake()).ok();
        }
        state[dir].waker = Some(cx.waker().clone());
        Self::capture(&mut state[dir], self.wake_mode());

        // Update interest in this I/O handle.
        if was_empty {
            // Create the event that we are interested in.
            let event = {
                let mut event = Event::none(self.key);
                event.readable = !state[READ].is_empty();
                event.writable = !state[WRITE].is_empty();
                event
            };

            // Register interest in it.
            self.registration.modify(&Reactor::get().poller, event)?;
        }

        Poll::Pending
    }

    /// Waits until the I/O source is readable.
    pub(crate) fn readable<T>(handle: &crate::Async<T>) -> Readable<'_, T> {
        Readable(Self::ready(handle, READ))
    }

    /// Waits until the I/O source is readable.
    pub(crate) fn readable_owned<T>(handle: Arc<crate::Async<T>>) -> ReadableOwned<T> {
        ReadableOwned(Self::ready(handle, READ))
    }

    /// Waits until the I/O source is writable.
    pub(crate) fn writable<T>(handle: &crate::Async<T>) -> Writable<'_, T> {
        Writable(Self::ready(handle, WRITE))
    }

    /// Waits until the I/O source is writable.
    pub(crate) fn writable_owned<T>(handle: Arc<crate::Async<T>>) -> WritableOwned<T> {
        WritableOwned(Self::ready(handle, WRITE))
    }

    /// Waits until the I/O source is readable or writable.
    fn ready<H: Borrow<crate::Async<T>> + Clone, T>(handle: H, dir: usize) -> Ready<H, T> {
        let wake_mode = handle.borrow().source.wake_mode();
        Ready {
            handle,
            wake_mode,
            dir,
            ticks: None,
            captured_events: None,
            index: None,
            _capture: PhantomData,
        }
    }

    pub(crate) fn registration(&self) -> &Registration {
        &self.registration
    }

    /// Returns `true` if a delivery has occurred since the last `capture` call on this direction.
    ///
    /// `WakeMode::Level` decision table — `(a, b) = dir.ticks` (captured at registration:
    /// `a` = reactor ticker snapshot, `b` = `dir.tick` snapshot), `t = dir.tick` now:
    ///
    /// | `dir.ticks`      | `t == a` | `t == b` | ready? | rationale                        |
    /// |------------------|----------|----------|--------|----------------------------------|
    /// | `None`           | —        | —        | false  | never registered                 |
    /// | `Some((a, b))`   | true     | —        | false  | tick aliases reactor ticker only |
    /// | `Some((a, b))`   | —        | true     | false  | tick unchanged since registration|
    /// | `Some((a, b))`   | false    | false    | true   | direction tick advanced          |
    ///
    /// `WakeMode::Edge` uses the per-direction monotonic `events` counter exclusively (only
    /// bumped by `ReactorLock::react()` on actual delivery): ready iff `dir.events !=
    /// dir.captured_events`. This avoids the level-mode same-cycle aliasing race where
    /// `dir.tick == a` without any actual delivery for this direction (which would otherwise
    /// cause spurious `Ready` returns and, in tight drain loops like
    /// `NamedPipeStream::poll_read`, an infinite busy-loop).
    fn check_ready(dir: &Direction, mode: WakeMode) -> bool {
        match mode {
            WakeMode::Level => {
                if let Some((a, b)) = dir.ticks {
                    dir.tick != a && dir.tick != b
                } else {
                    false
                }
            }
            #[cfg(windows)]
            WakeMode::Edge => match dir.captured_events {
                Some(captured) => dir.events != captured,
                None => false,
            },
        }
    }

    /// Records the state needed by `check_ready` to detect future delivery.
    fn capture(dir: &mut Direction, mode: WakeMode) {
        match mode {
            WakeMode::Level => {
                dir.ticks = Some((Reactor::get().ticker(), dir.tick));
                dir.captured_events = None;
            }
            #[cfg(windows)]
            WakeMode::Edge => {
                dir.captured_events = Some(dir.events);
                dir.ticks = None;
            }
        }
    }
}

/// Future for [`Async::readable`](crate::Async::readable).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Readable<'a, T>(Ready<&'a crate::Async<T>, T>);

impl<T> Future for Readable<'_, T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        #[cfg(feature = "tracing")]
        tracing::trace!(fd = ?self.0.handle.source.registration, "readable");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for Readable<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Readable").finish()
    }
}

/// Future for [`Async::readable_owned`](crate::Async::readable_owned).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadableOwned<T>(Ready<Arc<crate::Async<T>>, T>);

impl<T> Future for ReadableOwned<T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        #[cfg(feature = "tracing")]
        tracing::trace!(fd = ?self.0.handle.source.registration, "readable_owned");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for ReadableOwned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadableOwned").finish()
    }
}

/// Future for [`Async::writable`](crate::Async::writable).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Writable<'a, T>(Ready<&'a crate::Async<T>, T>);

impl<T> Future for Writable<'_, T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        #[cfg(feature = "tracing")]
        tracing::trace!(fd = ?self.0.handle.source.registration, "writable");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for Writable<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writable").finish()
    }
}

/// Future for [`Async::writable_owned`](crate::Async::writable_owned).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WritableOwned<T>(Ready<Arc<crate::Async<T>>, T>);

impl<T> Future for WritableOwned<T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(Pin::new(&mut self.0).poll(cx))?;
        #[cfg(feature = "tracing")]
        tracing::trace!(fd = ?self.0.handle.source.registration, "writable_owned");
        Poll::Ready(Ok(()))
    }
}

impl<T> fmt::Debug for WritableOwned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WritableOwned").finish()
    }
}

struct Ready<H: Borrow<crate::Async<T>>, T> {
    handle: H,
    wake_mode: WakeMode,
    dir: usize,
    /// Captured `(reactor_tick, dir_tick)` pair for `WakeMode::Level`.
    ticks: Option<(usize, usize)>,
    /// Captured per-direction event counter for `WakeMode::Edge`.
    captured_events: Option<u64>,
    index: Option<usize>,
    _capture: PhantomData<fn() -> T>,
}

impl<H: Borrow<crate::Async<T>>, T> Unpin for Ready<H, T> {}

impl<H: Borrow<crate::Async<T>> + Clone, T> Future for Ready<H, T> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            ref handle,
            wake_mode,
            dir,
            ticks,
            // Only read in the `WakeMode::Edge` arms below, which are themselves Windows-only.
            #[cfg_attr(not(windows), allow(unused_variables))]
            captured_events,
            index,
            ..
        } = &mut *self;

        let mut state = handle.borrow().source.state.lock().unwrap();

        // Check if the reactor has delivered an event since registration.
        let ready = match *wake_mode {
            WakeMode::Level => match *ticks {
                Some((a, b)) => state[*dir].tick != a && state[*dir].tick != b,
                None => false,
            },
            #[cfg(windows)]
            WakeMode::Edge => match *captured_events {
                Some(captured) => state[*dir].events != captured,
                None => false,
            },
        };
        if ready {
            return Poll::Ready(Ok(()));
        }

        let was_empty = state[*dir].is_empty();

        // Register the current task's waker.
        let i = match *index {
            Some(i) => i,
            None => {
                let i = state[*dir].wakers.insert(None);
                *index = Some(i);
                match *wake_mode {
                    WakeMode::Level => {
                        *ticks = Some((Reactor::get().ticker(), state[*dir].tick));
                    }
                    #[cfg(windows)]
                    WakeMode::Edge => {
                        *captured_events = Some(state[*dir].events);
                    }
                }
                i
            }
        };
        state[*dir].wakers[i] = Some(cx.waker().clone());

        // Update interest in this I/O handle.
        if was_empty {
            // Create the event that we are interested in.
            let event = {
                let mut event = Event::none(handle.borrow().source.key);
                event.readable = !state[READ].is_empty();
                event.writable = !state[WRITE].is_empty();
                event
            };

            // Indicate that we are interested in this event.
            handle
                .borrow()
                .source
                .registration
                .modify(&Reactor::get().poller, event)?;
        }

        Poll::Pending
    }
}

impl<H: Borrow<crate::Async<T>>, T> Drop for Ready<H, T> {
    fn drop(&mut self) {
        // Remove our waker when dropped.
        if let Some(key) = self.index {
            let mut state = self.handle.borrow().source.state.lock().unwrap();
            let wakers = &mut state[self.dir].wakers;
            if wakers.contains(key) {
                wakers.remove(key);
            }
        }
    }
}

#[cfg(test)]
mod ready_tests {
    //! Unit tests for the `WakeMode`-aware readiness logic in `Source::check_ready` /
    //! `Source::capture`. These exercise the pure state-machine and do not require any
    //! OS handle or running reactor.

    use super::{Direction, Source, WakeMode};

    /// Simulates a delivery from `ReactorLock::react()` for the given direction:
    /// bumps the per-direction event counter and updates the last-delivery tick.
    fn deliver(dir: &mut Direction, reactor_tick: usize) {
        dir.tick = reactor_tick;
        dir.events = dir.events.wrapping_add(1);
    }

    // ---- Level mode ----

    #[test]
    fn level_no_capture_is_not_ready() {
        let dir = Direction::default();
        assert!(!Source::check_ready(&dir, WakeMode::Level));
    }

    #[test]
    fn level_no_delivery_after_capture_is_not_ready() {
        let mut dir = Direction::default();
        dir.tick = 5;
        // Simulate registration at reactor ticker = 7, with last delivery tick 5.
        dir.ticks = Some((7, 5));
        assert!(!Source::check_ready(&dir, WakeMode::Level));
    }

    #[test]
    fn level_delivery_after_capture_is_ready() {
        let mut dir = Direction::default();
        dir.tick = 5;
        dir.ticks = Some((7, 5));
        // Reactor advances and delivers our event at tick 9.
        deliver(&mut dir, 9);
        assert!(Source::check_ready(&dir, WakeMode::Level));
    }

    #[test]
    fn level_same_cycle_delivery_is_not_detected() {
        // Documents existing level-mode behavior: if delivery happens in the same
        // reactor cycle whose ticker we captured (`dir.tick == a`), level mode does
        // NOT report ready. This is acceptable for level sources because they keep
        // re-firing on every subsequent react() pass.
        let mut dir = Direction::default();
        dir.tick = 5;
        dir.ticks = Some((7, 5));
        deliver(&mut dir, 7); // tick == a
        assert!(!Source::check_ready(&dir, WakeMode::Level));
    }

    // ---- Edge mode ----

    #[cfg(windows)]
    #[test]
    fn edge_no_capture_is_not_ready() {
        let dir = Direction::default();
        assert!(!Source::check_ready(&dir, WakeMode::Edge));
    }

    #[cfg(windows)]
    #[test]
    fn edge_no_delivery_after_capture_is_not_ready() {
        let mut dir = Direction::default();
        dir.events = 3;
        Source::capture(&mut dir, WakeMode::Edge);
        assert_eq!(dir.captured_events, Some(3));
        assert!(!Source::check_ready(&dir, WakeMode::Edge));
    }

    #[cfg(windows)]
    #[test]
    fn edge_delivery_after_capture_is_ready() {
        let mut dir = Direction::default();
        dir.events = 3;
        Source::capture(&mut dir, WakeMode::Edge);
        deliver(&mut dir, 42);
        assert!(Source::check_ready(&dir, WakeMode::Edge));
    }

    #[cfg(windows)]
    #[test]
    fn edge_same_cycle_delivery_is_detected() {
        // Regression: previously the edge path used `state.tick == a` to catch the
        // same-cycle race. With the per-direction event counter, delivery is detected
        // unambiguously regardless of how the reactor tick aliases.
        let mut dir = Direction::default();
        dir.tick = 7; // last-delivery tick happens to equal current reactor ticker
        dir.events = 1;
        Source::capture(&mut dir, WakeMode::Edge);
        // Delivery in the same cycle: reactor tick stays 7, but events advances.
        deliver(&mut dir, 7);
        assert!(Source::check_ready(&dir, WakeMode::Edge));
    }

    #[cfg(windows)]
    #[test]
    fn edge_no_spurious_ready_when_tick_equals_capture() {
        // Regression for the bug fixed by this change: the previous edge logic
        // returned `Ready` whenever `state.tick == a`, even when no delivery had
        // occurred. The most concrete impact was the drain loop in
        // `NamedPipeStream::poll_read`, which would busy-loop forever whenever
        // `state.tick == reactor.ticker` happened to hold at registration time.
        //
        // With the counter-based check, a capture taken when `dir.tick` equals the
        // reactor ticker must NOT be reported as ready until a real delivery
        // increments `dir.events`.
        let mut dir = Direction::default();
        dir.tick = 7;
        dir.events = 1;
        Source::capture(&mut dir, WakeMode::Edge);
        // No delivery yet -- not ready.
        assert!(!Source::check_ready(&dir, WakeMode::Edge));
        // Spurious wake (no delivery): a tight drain loop would re-check, must still
        // observe "not ready".
        for _ in 0..1000 {
            assert!(!Source::check_ready(&dir, WakeMode::Edge));
        }
    }

    #[cfg(windows)]
    #[test]
    fn edge_each_delivery_consumed_exactly_once() {
        // Each call sequence (capture -> deliver -> check_ready -> consume) must
        // report ready exactly once per delivery.
        let mut dir = Direction::default();
        for n in 1..=10u64 {
            Source::capture(&mut dir, WakeMode::Edge);
            assert!(!Source::check_ready(&dir, WakeMode::Edge));
            deliver(&mut dir, n as usize);
            assert!(Source::check_ready(&dir, WakeMode::Edge));
            // Consume: clear capture (mirrors what poll_ready does on Ready).
            dir.captured_events = None;
            assert!(!Source::check_ready(&dir, WakeMode::Edge));
        }
        assert_eq!(dir.events, 10);
    }

    #[cfg(windows)]
    #[test]
    fn edge_capture_clears_level_state_and_vice_versa() {
        let mut dir = Direction::default();
        dir.tick = 3;
        dir.events = 4;

        Source::capture(&mut dir, WakeMode::Level);
        assert!(dir.ticks.is_some());
        assert!(dir.captured_events.is_none());

        Source::capture(&mut dir, WakeMode::Edge);
        assert!(dir.ticks.is_none());
        assert_eq!(dir.captured_events, Some(4));
    }

    #[cfg(windows)]
    #[test]
    fn edge_counter_wraps_without_panic() {
        // Wrapping is fine: the only operation we do is equality, which is safe
        // around a wrap. The test documents the intent.
        let mut dir = Direction::default();
        dir.events = u64::MAX;
        Source::capture(&mut dir, WakeMode::Edge);
        deliver(&mut dir, 0); // wraps to 0
        assert_eq!(dir.events, 0);
        assert!(Source::check_ready(&dir, WakeMode::Edge));
    }
}
