//! Port of `ipcz/trap_event_dispatcher.{h,cc}`.

use super::ipcz_types::{PortalStatus, TrapConditionFlags, TrapEvent, TrapEventHandler};

struct DeferredEvent {
    handler: TrapEventHandler,
    event: TrapEvent,
}

/// Accumulates trap event dispatches to specific handlers. Handler invocation is
/// deferred until `dispatch_all()` is called or the dispatcher is dropped. This
/// allows event dispatches to be accumulated while e.g. Node and Router locks
/// are held, and dispatched later, once such locks are released.
///
/// This object is not thread-safe but is generally constructed on the stack and
/// passed into whatever might want to accumulate events for dispatch.
#[derive(Default)]
pub struct TrapEventDispatcher {
    events: Vec<DeferredEvent>,
}

impl TrapEventDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Schedules a new event for dispatch as soon as `dispatch_all()` is
    /// explicitly called or the dispatcher is dropped.
    pub fn defer_event(
        &mut self,
        handler: TrapEventHandler,
        context: u64,
        flags: TrapConditionFlags,
        status: PortalStatus,
    ) {
        self.events.push(DeferredEvent {
            handler,
            event: TrapEvent {
                context,
                condition_flags: flags,
                status,
            },
        });
    }

    /// Dispatches any events deferred by `defer_event()`.
    pub fn dispatch_all(&mut self) {
        // Handlers may re-enter the API and so may create dispatchers of their
        // own; take the batch first so nothing is borrowed while it runs.
        std::mem::take(&mut self.events)
            .into_iter()
            .for_each(|deferred| (deferred.handler)(&deferred.event));
    }
}

impl Drop for TrapEventDispatcher {
    fn drop(&mut self) {
        self.dispatch_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn dispatches_on_drop_in_order() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        {
            let mut dispatcher = TrapEventDispatcher::new();
            for context in 0..3 {
                let seen = seen.clone();
                dispatcher.defer_event(
                    Arc::new(move |event: &TrapEvent| seen.lock().unwrap().push(event.context)),
                    context,
                    TrapConditionFlags::PEER_CLOSED,
                    PortalStatus::default(),
                );
            }
            assert!(seen.lock().unwrap().is_empty());
        }
        assert_eq!(*seen.lock().unwrap(), [0, 1, 2]);
    }

    #[test]
    fn explicit_dispatch_runs_once() {
        let count = Arc::new(Mutex::new(0));
        let mut dispatcher = TrapEventDispatcher::new();
        let counter = count.clone();
        dispatcher.defer_event(
            Arc::new(move |_| *counter.lock().unwrap() += 1),
            0,
            TrapConditionFlags::REMOVED,
            PortalStatus::default(),
        );
        dispatcher.dispatch_all();
        dispatcher.dispatch_all();
        drop(dispatcher);
        assert_eq!(*count.lock().unwrap(), 1);
    }
}
