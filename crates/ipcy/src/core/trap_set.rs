//! Port of `ipcz/trap_set.{h,cc}`.

use super::api_context::ApiContext;
use super::ipcz_types::{
    PortalStatus, PortalStatusFlags, TrapConditionFlags, TrapConditions, TrapEventHandler,
};
use super::parcel::ParcelQueue;
use super::trap_event_dispatcher::TrapEventDispatcher;

struct Trap {
    conditions: TrapConditions,
    handler: TrapEventHandler,
    context: u64,
}

/// Returned by [`TrapSet::add`] when the new trap's conditions are already met.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrapAlreadySatisfied {
    pub satisfied_condition_flags: TrapConditionFlags,
    pub status: PortalStatus,
}

/// The reason for each status update when something happens that might interest
/// a trap.
#[derive(Clone, Copy, PartialEq, Eq)]
enum UpdateReason {
    /// A new trap is being installed and this is an initial state query.
    InstallTrap,
    /// A new inbound parcel has arrived for retrieval.
    NewLocalParcel,
    /// We just discovered that the remote portal is gone.
    PeerClosed,
    /// A previously queued inbound parcel has been fully or partially retrieved
    /// by the application.
    LocalParcelConsumed,
}

fn portal_status(status_flags: PortalStatusFlags, inbound_parcel_queue: &ParcelQueue) -> PortalStatus {
    PortalStatus {
        flags: status_flags,
        num_local_parcels: inbound_parcel_queue.get_num_available_elements(),
        num_local_bytes: inbound_parcel_queue.get_total_available_element_size(),
    }
}

/// A set of traps installed on a portal.
#[derive(Default)]
pub struct TrapSet {
    traps: Vec<Trap>,
}

impl TrapSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.traps.is_empty()
    }

    /// Attempts to install a new trap in the set. This effectively implements
    /// the ipcz Trap() API. `status_flags` and `inbound_parcel_queue` convey the
    /// current status of the portal. If `conditions` are already met, returns
    /// the satisfied condition flags and the portal status instead.
    pub fn add(
        &mut self,
        conditions: TrapConditions,
        handler: TrapEventHandler,
        context: u64,
        status_flags: PortalStatusFlags,
        inbound_parcel_queue: &ParcelQueue,
    ) -> Result<(), TrapAlreadySatisfied> {
        let flags = Self::get_satisfied_conditions_for_update(
            &conditions,
            status_flags,
            inbound_parcel_queue,
            UpdateReason::InstallTrap,
        );
        if !flags.is_empty() {
            return Err(TrapAlreadySatisfied {
                satisfied_condition_flags: flags,
                status: portal_status(status_flags, inbound_parcel_queue),
            });
        }

        self.traps.push(Trap {
            conditions,
            handler,
            context,
        });
        Ok(())
    }

    /// Notifies the TrapSet that a new local parcel has arrived on its portal.
    /// Any trap interested in this is removed from the set, and its event
    /// handler invocation is appended to `dispatcher`.
    pub fn notify_new_local_parcel(
        &mut self,
        status_flags: PortalStatusFlags,
        inbound_parcel_queue: &ParcelQueue,
        dispatcher: &mut TrapEventDispatcher,
    ) {
        self.update_portal_status(status_flags, inbound_parcel_queue, UpdateReason::NewLocalParcel, dispatcher);
    }

    /// Notifies the TrapSet that a local parcel has been consumed from its
    /// portal.
    pub fn notify_local_parcel_consumed(
        &mut self,
        status_flags: PortalStatusFlags,
        inbound_parcel_queue: &ParcelQueue,
        dispatcher: &mut TrapEventDispatcher,
    ) {
        self.update_portal_status(status_flags, inbound_parcel_queue, UpdateReason::LocalParcelConsumed, dispatcher);
    }

    /// Notifies the TrapSet that its portal's peer has been closed.
    pub fn notify_peer_closed(
        &mut self,
        status_flags: PortalStatusFlags,
        inbound_parcel_queue: &ParcelQueue,
        dispatcher: &mut TrapEventDispatcher,
    ) {
        self.update_portal_status(status_flags, inbound_parcel_queue, UpdateReason::PeerClosed, dispatcher);
    }

    /// Immediately removes all traps from the set. Every trap present appends a
    /// `REMOVED` event to `dispatcher` before removal.
    pub fn remove_all(&mut self, dispatcher: &mut TrapEventDispatcher) {
        let mut flags = TrapConditionFlags::REMOVED;
        if ApiContext::is_current_thread_within_api_call() {
            flags |= TrapConditionFlags::WITHIN_API_CALL;
        }

        // Forced trap removal implies the portal has been invalidated by closure,
        // transfer, or merging. In any case, this status is meaningless.
        for trap in self.traps.drain(..) {
            dispatcher.defer_event(trap.handler, trap.context, flags, PortalStatus::default());
        }
    }

    // Determines which trap condition flags would be set if an event fired for a
    // trap watching the given `conditions`, given the most recent state change.
    fn get_satisfied_conditions_for_update(
        conditions: &TrapConditions,
        status_flags: PortalStatusFlags,
        inbound_parcel_queue: &ParcelQueue,
        reason: UpdateReason,
    ) -> TrapConditionFlags {
        let mut event_flags = TrapConditionFlags::empty();
        if conditions.flags.contains(TrapConditionFlags::PEER_CLOSED)
            && status_flags.contains(PortalStatusFlags::PEER_CLOSED)
        {
            event_flags |= TrapConditionFlags::PEER_CLOSED;
        }
        if conditions.flags.contains(TrapConditionFlags::DEAD) && status_flags.contains(PortalStatusFlags::DEAD) {
            event_flags |= TrapConditionFlags::DEAD;
        }
        if conditions.flags.contains(TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS)
            && inbound_parcel_queue.get_num_available_elements() > conditions.min_local_parcels
        {
            event_flags |= TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS;
        }
        if conditions.flags.contains(TrapConditionFlags::ABOVE_MIN_LOCAL_BYTES)
            && inbound_parcel_queue.get_total_available_element_size() > conditions.min_local_bytes
        {
            event_flags |= TrapConditionFlags::ABOVE_MIN_LOCAL_BYTES;
        }
        if conditions.flags.contains(TrapConditionFlags::NEW_LOCAL_PARCEL) && reason == UpdateReason::NewLocalParcel {
            event_flags |= TrapConditionFlags::NEW_LOCAL_PARCEL;
        }
        event_flags
    }

    // Helper used by notify_* methods to carry out common update work.
    fn update_portal_status(
        &mut self,
        status_flags: PortalStatusFlags,
        inbound_parcel_queue: &ParcelQueue,
        reason: UpdateReason,
        dispatcher: &mut TrapEventDispatcher,
    ) {
        let (fired, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.traps)
            .into_iter()
            .map(|trap| {
                let flags = Self::get_satisfied_conditions_for_update(
                    &trap.conditions,
                    status_flags,
                    inbound_parcel_queue,
                    reason,
                );
                (trap, flags)
            })
            .partition(|(_, flags)| !flags.is_empty());
        self.traps = kept.into_iter().map(|(trap, _)| trap).collect();

        let status = portal_status(status_flags, inbound_parcel_queue);
        for (trap, mut flags) in fired {
            if ApiContext::is_current_thread_within_api_call() {
                flags |= TrapConditionFlags::WITHIN_API_CALL;
            }
            dispatcher.defer_event(trap.handler, trap.context, flags, status);
        }
    }
}

impl Drop for TrapSet {
    fn drop(&mut self) {
        // A TrapSet must be empty before it can be destroyed.
        debug_assert!(self.is_empty() || std::thread::panicking());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::parcel::Parcel;
    use crate::core::sequence_number::SequenceNumber;
    use std::sync::{Arc, Mutex};

    type Events = Arc<Mutex<Vec<(u64, TrapConditionFlags, PortalStatus)>>>;

    fn handler(events: &Events) -> TrapEventHandler {
        let events = events.clone();
        Arc::new(move |event| {
            events.lock().unwrap().push((event.context, event.condition_flags, event.status))
        })
    }

    fn queue_with_parcels(sizes: &[usize]) -> ParcelQueue {
        let mut queue = ParcelQueue::default();
        for (i, &size) in sizes.iter().enumerate() {
            let mut parcel = Box::new(Parcel::new(SequenceNumber::new(i as u64)));
            parcel.allocate_data(size, false, None);
            assert!(queue.push(SequenceNumber::new(i as u64), parcel));
        }
        queue
    }

    #[test]
    fn already_satisfied_traps_are_not_installed() {
        let events = Events::default();
        let mut traps = TrapSet::new();
        let queue = queue_with_parcels(&[4]);

        let conditions = TrapConditions {
            flags: TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS | TrapConditionFlags::PEER_CLOSED,
            min_local_parcels: 0,
            min_local_bytes: 0,
        };
        let err = traps
            .add(conditions, handler(&events), 1, PortalStatusFlags::empty(), &queue)
            .unwrap_err();
        assert_eq!(err.satisfied_condition_flags, TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS);
        assert_eq!(err.status.num_local_parcels, 1);
        assert_eq!(err.status.num_local_bytes, 4);
        assert!(traps.is_empty());
    }

    #[test]
    fn fires_once_for_interesting_updates() {
        let events = Events::default();
        let mut traps = TrapSet::new();
        let empty = ParcelQueue::default();
        let conditions = TrapConditions {
            flags: TrapConditionFlags::NEW_LOCAL_PARCEL,
            ..TrapConditions::default()
        };
        traps.add(conditions, handler(&events), 7, PortalStatusFlags::empty(), &empty).unwrap();

        let mut dispatcher = TrapEventDispatcher::new();
        // Consumption isn't a new parcel, so nothing fires.
        traps.notify_local_parcel_consumed(PortalStatusFlags::empty(), &empty, &mut dispatcher);
        dispatcher.dispatch_all();
        assert!(events.lock().unwrap().is_empty());

        let queue = queue_with_parcels(&[3, 5]);
        traps.notify_new_local_parcel(PortalStatusFlags::empty(), &queue, &mut dispatcher);
        dispatcher.dispatch_all();
        let seen = events.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, 7);
        assert_eq!(seen[0].1, TrapConditionFlags::NEW_LOCAL_PARCEL);
        assert_eq!(seen[0].2.num_local_parcels, 2);
        assert_eq!(seen[0].2.num_local_bytes, 8);

        // The trap fired and was removed, so a second notification is silent.
        traps.notify_new_local_parcel(PortalStatusFlags::empty(), &queue, &mut dispatcher);
        dispatcher.dispatch_all();
        assert_eq!(events.lock().unwrap().len(), 1);
        assert!(traps.is_empty());
    }

    #[test]
    fn peer_closed_and_dead() {
        let events = Events::default();
        let mut traps = TrapSet::new();
        let empty = ParcelQueue::default();
        let conditions = TrapConditions {
            flags: TrapConditionFlags::PEER_CLOSED | TrapConditionFlags::DEAD,
            ..TrapConditions::default()
        };
        traps.add(conditions, handler(&events), 1, PortalStatusFlags::empty(), &empty).unwrap();

        let mut dispatcher = TrapEventDispatcher::new();
        let status = PortalStatusFlags::PEER_CLOSED | PortalStatusFlags::DEAD;
        traps.notify_peer_closed(status, &empty, &mut dispatcher);
        dispatcher.dispatch_all();
        let seen = events.lock().unwrap().clone();
        assert_eq!(seen[0].1, TrapConditionFlags::PEER_CLOSED | TrapConditionFlags::DEAD);
        assert_eq!(seen[0].2.flags, status);
    }

    #[test]
    fn remove_all_fires_removed_events() {
        let events = Events::default();
        let mut traps = TrapSet::new();
        let empty = ParcelQueue::default();
        let conditions = TrapConditions {
            flags: TrapConditionFlags::PEER_CLOSED,
            ..TrapConditions::default()
        };
        traps.add(conditions, handler(&events), 1, PortalStatusFlags::empty(), &empty).unwrap();
        traps.add(conditions, handler(&events), 2, PortalStatusFlags::empty(), &empty).unwrap();

        {
            let _within_api_call = ApiContext::new();
            let mut dispatcher = TrapEventDispatcher::new();
            traps.remove_all(&mut dispatcher);
        }
        assert!(traps.is_empty());
        let seen = events.lock().unwrap().clone();
        assert_eq!(seen.iter().map(|e| e.0).collect::<Vec<_>>(), [1, 2]);
        assert!(seen.iter().all(|e| e.1 == TrapConditionFlags::REMOVED | TrapConditionFlags::WITHIN_API_CALL));
    }
}
