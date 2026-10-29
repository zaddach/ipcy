//! Port of `ipcz/router_link_state.{h,cc}`.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, Ordering};

use super::link_side::LinkSide;
use super::node_name::NodeName;
use super::ref_counted_fragment::RefCountedFragment;

/// Link status which both sides atomically update to coordinate orderly proxy
/// bypass, route closure propagation, and other operations.
pub type Status = u32;

/// Structure shared between both Routers connected by RouterLink. This is used
/// to synchronously query and reflect the state of each Router to the other,
/// and ultimately to facilitate orderly state changes across the route. This
/// may live in shared memory, where it should be managed as a
/// RefCountedFragment.
///
/// Note that RouterLinkStates are effectively only used by central links.
#[derive(Debug)]
#[repr(C, align(8))]
pub struct RouterLinkState {
    pub ref_counted_fragment: RefCountedFragment,

    pub status: AtomicU32,

    /// In a situation with three routers A-B-C and a central link between A and
    /// B, B will eventually ask C to connect directly to A and bypass B along
    /// the route. In order to facilitate this, B will also first stash C's name
    /// in this field on the central link between A and B. This is sufficient
    /// for A to validate that C is an appropriate source of such a bypass
    /// request.
    pub allowed_bypass_request_source: UnsafeCell<NodeName>,

    /// More reserved slots, padding out this structure to 64 bytes.
    pub reserved1: [u32; 10],
}

// The size of this structure is fixed at 64 bytes to ensure that it fits the
// smallest block allocation size supported by NodeLinkMemory.
// SAFETY: all mutation is through atomics; the NodeName is only accessed through
// the release/acquire accessors below.
unsafe impl Sync for RouterLinkState {}
unsafe impl Send for RouterLinkState {}

const _: () = assert!(std::mem::size_of::<RouterLinkState>() == 64);
const _: () = assert!(std::mem::align_of::<RouterLinkState>() == 8);

impl RouterLinkState {
    /// This is a fresh central link established to bypass a proxy. The Routers
    /// on either side both still have decaying links and therefore cannot yet
    /// support another bypass operation.
    pub const UNSTABLE: Status = 0;

    /// Set if side A or B of this link is stable, respectively, meaning it has
    /// no decaying router links. If both bits are set, the link itself is
    /// considered to be stable.
    pub const SIDE_A_STABLE: Status = 1 << 0;
    pub const SIDE_B_STABLE: Status = 1 << 1;
    pub const STABLE: Status = Self::SIDE_A_STABLE | Self::SIDE_B_STABLE;

    /// When either side attempts to lock this link and fails because the other
    /// side is still unstable, they set their corresponding "waiting" bit
    /// instead. Once the other side is stable, this bit informs the other side
    /// that they should send a flush notification back to this side to unblock
    /// whatever operation was waiting for a stable link.
    pub const SIDE_A_WAITING: Status = 1 << 2;
    pub const SIDE_B_WAITING: Status = 1 << 3;

    /// Set if this link has been locked by side A or B, respectively. These
    /// bits are always mutually exclusive and may only be set once `STABLE` are
    /// set. A link may be locked to initiate bypass of one side, or to
    /// propagate route closure from one side.
    pub const LOCKED_BY_SIDE_A: Status = 1 << 4;
    pub const LOCKED_BY_SIDE_B: Status = 1 << 5;

    pub const fn new() -> Self {
        Self {
            ref_counted_fragment: RefCountedFragment::new(),
            status: AtomicU32::new(Self::UNSTABLE),
            allowed_bypass_request_source: UnsafeCell::new(NodeName::new(0, 0)),
            reserved1: [0; 10],
        }
    }

    /// In-place initialization of a new RouterLinkState at `where_`.
    ///
    /// # Safety
    /// `where_` must be valid for writes of a `RouterLinkState` and 8-byte
    /// aligned, and must not be concurrently accessed during initialization.
    pub unsafe fn initialize<'a>(where_: *mut RouterLinkState) -> &'a mut RouterLinkState {
        // SAFETY: guaranteed by the caller.
        unsafe { where_.write(RouterLinkState::new()) };
        std::sync::atomic::fence(Ordering::Release);
        unsafe { &mut *where_ }
    }

    /// Publishes the name of the only node allowed to request bypass of the
    /// side which just locked this link.
    pub fn store_allowed_bypass_request_source(&self, name: NodeName) {
        // SAFETY: the field is in a live RouterLinkState and only accessed
        // atomically through this and the matching load.
        unsafe { NodeName::store_release(self.allowed_bypass_request_source.get(), name) }
    }

    /// Balanced with a release in `store_allowed_bypass_request_source()`.
    pub fn load_allowed_bypass_request_source(&self) -> NodeName {
        // SAFETY: as above.
        unsafe { NodeName::load_acquire(self.allowed_bypass_request_source.get()) }
    }

    pub fn is_locked_by(&self, side: LinkSide) -> bool {
        let s = self.status.load(Ordering::Relaxed);
        let locked = match side {
            LinkSide::A => Self::LOCKED_BY_SIDE_A,
            LinkSide::B => Self::LOCKED_BY_SIDE_B,
        };
        s & locked != 0
    }

    /// Updates the status to reflect that the given `side` is stable, meaning
    /// that it's no longer holding onto any decaying links.
    pub fn set_side_stable(&self, side: LinkSide) {
        let this_side_stable = match side {
            LinkSide::A => Self::SIDE_A_STABLE,
            LinkSide::B => Self::SIDE_B_STABLE,
        };
        let mut expected = Self::UNSTABLE;
        while let Err(actual) = self.status.compare_exchange_weak(
            expected,
            expected | this_side_stable,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            expected = actual;
            if expected & this_side_stable != 0 {
                break;
            }
        }
    }

    /// Attempts to lock the state of this link from one side, so that the
    /// Router on that side can coordinate its own bypass or propagate its own
    /// side's closure. In order for this to succeed, both `STABLE` bits must be
    /// set and the link must not already be locked. Returns true iff locked
    /// successfully.
    ///
    /// If the opposite side is still unstable, this sets the waiting bit for
    /// `from_side` and returns false. In any other situation, the status is
    /// unmodified and this returns false.
    #[must_use]
    pub fn try_lock(&self, from_side: LinkSide) -> bool {
        let (this_side_stable, other_side_stable, locked_by_this_side, this_side_waiting) =
            match from_side {
                LinkSide::A => (
                    Self::SIDE_A_STABLE,
                    Self::SIDE_B_STABLE,
                    Self::LOCKED_BY_SIDE_A,
                    Self::SIDE_A_WAITING,
                ),
                LinkSide::B => (
                    Self::SIDE_B_STABLE,
                    Self::SIDE_A_STABLE,
                    Self::LOCKED_BY_SIDE_B,
                    Self::SIDE_B_WAITING,
                ),
            };
        let locked_by_either_side = Self::LOCKED_BY_SIDE_A | Self::LOCKED_BY_SIDE_B;

        let mut expected = Self::STABLE;
        let mut desired_bit = locked_by_this_side;
        while let Err(actual) = self.status.compare_exchange_weak(
            expected,
            expected | desired_bit,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            expected = actual;
            if expected & locked_by_either_side != 0 || expected & this_side_stable == 0 {
                return false;
            }

            if desired_bit == locked_by_this_side && expected & other_side_stable == 0 {
                // If we were trying to lock but the other side isn't stable, try
                // to set our waiting bit instead.
                desired_bit = this_side_waiting;
            } else if desired_bit == this_side_waiting && expected & Self::STABLE == Self::STABLE {
                // Otherwise if we were trying to set our waiting bit and the
                // other side is now stable, go back to trying to lock the link.
                desired_bit = locked_by_this_side;
            }
        }
        desired_bit == locked_by_this_side
    }

    /// Unlocks a link previously locked by [`Self::try_lock`].
    pub fn unlock(&self, from_side: LinkSide) {
        let locked_by_this_side = match from_side {
            LinkSide::A => Self::LOCKED_BY_SIDE_A,
            LinkSide::B => Self::LOCKED_BY_SIDE_B,
        };
        let mut expected = Self::STABLE | locked_by_this_side;
        let mut desired = Self::STABLE;
        while let Err(actual) = self.status.compare_exchange_weak(
            expected,
            desired,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            expected = actual;
            if expected & locked_by_this_side == 0 {
                break;
            }
            desired = expected & !locked_by_this_side;
        }
    }

    /// If both sides of the link are stable AND `side` was marked as waiting
    /// before that happened, this resets the waiting bit and returns true.
    /// Otherwise the link's status is unchanged and this returns false.
    ///
    /// Note that the waiting bit for `side` will have only been set if a prior
    /// attempt was made to `try_lock()` from that side, while the other side was
    /// still unstable.
    pub fn reset_waiting_bit(&self, side: LinkSide) -> bool {
        let this_side_waiting = match side {
            LinkSide::A => Self::SIDE_A_WAITING,
            LinkSide::B => Self::SIDE_B_WAITING,
        };
        let locked_by_either_side = Self::LOCKED_BY_SIDE_A | Self::LOCKED_BY_SIDE_B;

        let mut expected = Self::STABLE | this_side_waiting;
        let mut desired = Self::STABLE;
        while let Err(actual) = self.status.compare_exchange_weak(
            expected,
            desired,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            expected = actual;
            if expected & Self::STABLE != Self::STABLE
                || expected & this_side_waiting == 0
                || expected & locked_by_either_side != 0
            {
                // If the link isn't stable yet, or `side` wasn't waiting on it,
                // or the link is already locked, there's no point changing the
                // status here.
                return false;
            }

            // At this point we know the link is stable, the identified side is
            // waiting, and the link is not locked. Regardless of what other bits
            // are set, mask off the waiting bit and try again.
            desired = expected & !this_side_waiting;
        }
        true
    }
}

impl Default for RouterLinkState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;

    #[test]
    fn layout_matches_cpp() {
        // Mirrors the C++ layout: RefCountedFragment (4) | status (4) |
        // NodeName (16, align 8) | reserved1 (40) = 64 bytes.
        assert_eq!(offset_of!(RouterLinkState, ref_counted_fragment), 0);
        assert_eq!(offset_of!(RouterLinkState, status), 4);
        assert_eq!(offset_of!(RouterLinkState, allowed_bypass_request_source), 8);
        assert_eq!(offset_of!(RouterLinkState, reserved1), 24);
    }

    #[test]
    fn initialize_in_place() {
        #[repr(align(8))]
        struct Storage([u8; 64]);
        let mut storage = Storage([0xff; 64]);
        let state = unsafe { RouterLinkState::initialize(storage.0.as_mut_ptr().cast()) };
        assert_eq!(state.ref_counted_fragment.ref_count_for_testing(), 1);
        assert_eq!(state.status.load(Ordering::Relaxed), RouterLinkState::UNSTABLE);
        assert!(!state.load_allowed_bypass_request_source().is_valid());
    }

    #[test]
    fn cannot_lock_until_stable() {
        let s = RouterLinkState::new();
        assert!(!s.try_lock(LinkSide::A)); // A isn't stable: nothing changes.
        assert_eq!(s.status.load(Ordering::Relaxed), RouterLinkState::UNSTABLE);

        s.set_side_stable(LinkSide::A);
        // A is stable but B isn't: A is left waiting.
        assert!(!s.try_lock(LinkSide::A));
        assert_eq!(
            s.status.load(Ordering::Relaxed),
            RouterLinkState::SIDE_A_STABLE | RouterLinkState::SIDE_A_WAITING
        );
        assert!(!s.reset_waiting_bit(LinkSide::A)); // B still unstable.

        s.set_side_stable(LinkSide::B);
        assert!(s.reset_waiting_bit(LinkSide::A));
        assert_eq!(s.status.load(Ordering::Relaxed), RouterLinkState::STABLE);
        assert!(!s.reset_waiting_bit(LinkSide::A)); // No longer waiting.
    }

    #[test]
    fn lock_is_exclusive() {
        let s = RouterLinkState::new();
        s.set_side_stable(LinkSide::A);
        s.set_side_stable(LinkSide::B);

        assert!(s.try_lock(LinkSide::A));
        assert!(s.is_locked_by(LinkSide::A));
        assert!(!s.is_locked_by(LinkSide::B));
        assert!(!s.try_lock(LinkSide::B));
        assert!(!s.try_lock(LinkSide::A));

        s.unlock(LinkSide::A);
        assert!(!s.is_locked_by(LinkSide::A));
        assert!(s.try_lock(LinkSide::B));
        assert!(s.is_locked_by(LinkSide::B));
        s.unlock(LinkSide::B);
        assert_eq!(s.status.load(Ordering::Relaxed), RouterLinkState::STABLE);
    }

    #[test]
    fn set_side_stable_is_idempotent() {
        let s = RouterLinkState::new();
        s.set_side_stable(LinkSide::B);
        s.set_side_stable(LinkSide::B);
        assert_eq!(s.status.load(Ordering::Relaxed), RouterLinkState::SIDE_B_STABLE);
    }
}
