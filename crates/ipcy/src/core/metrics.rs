//! Port of `ipcz/metrics.{h,cc}`.
//!
//! DEVIATION: upstream reports to Chromium's UMA histograms. Here the embedder
//! installs a [`MetricsRecorder`]; with none installed (the default) metrics
//! cost nothing. The subsample rate is chosen along with the recorder.

use std::cell::Cell;
use std::sync::{Arc, RwLock};

/// The fraction of events upstream's UMA reporting samples.
pub const DEFAULT_SUBSAMPLE_RATE: f64 = 0.001;

/// Histogram bucket for the size of a block allocation request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockAllocationSize {
    Other = 0,
    B64 = 1,
    B128 = 2,
    B256 = 3,
    B512 = 4,
    KB1 = 5,
    KB2 = 6,
    KB4 = 7,
    KB8 = 8,
    KB16 = 9,
    KB32 = 10,
    KB64 = 11,
    KB128 = 12,
    KB256 = 13,
    KB512 = 14,
    MB1 = 15,
}

impl BlockAllocationSize {
    /// The bucket for a request of `block_size` bytes; sizes which aren't an
    /// exact power of two from 64 B to 1 MB fall in `Other`.
    pub fn from_block_size(block_size: usize) -> Self {
        match block_size {
            64 => Self::B64,
            128 => Self::B128,
            256 => Self::B256,
            512 => Self::B512,
            1024 => Self::KB1,
            2048 => Self::KB2,
            4096 => Self::KB4,
            8192 => Self::KB8,
            16384 => Self::KB16,
            32768 => Self::KB32,
            65536 => Self::KB64,
            131072 => Self::KB128,
            262144 => Self::KB256,
            524288 => Self::KB512,
            1048576 => Self::MB1,
            _ => Self::Other,
        }
    }
}

/// Which allocation path an event describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockAllocationSource {
    BufferPool,
    Parcel,
}

/// Receives block allocation outcomes. Called on the allocating thread.
pub trait MetricsRecorder: Send + Sync {
    fn record_allocate_block_result(&self, source: BlockAllocationSource, size: BlockAllocationSize, success: bool);
}

struct Installed {
    recorder: Arc<dyn MetricsRecorder>,
    sample_rate: f64,
}

static INSTALLED: RwLock<Option<Installed>> = RwLock::new(None);

/// Installs (or, with `None`, removes) the process-wide recorder. Only
/// `sample_rate` of all events, chosen at random, are reported to it.
pub fn set_recorder(recorder: Option<Arc<dyn MetricsRecorder>>, sample_rate: f64) {
    *INSTALLED.write().unwrap_or_else(|e| e.into_inner()) =
        recorder.map(|recorder| Installed { recorder, sample_rate });
}

thread_local! {
    static RNG_STATE: Cell<u64> = const { Cell::new(0) };
}

/// Returns true for `rate` of all calls. A xorshift generator seeded once per
/// thread keeps this cheap on allocation paths.
fn should_sample(rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    RNG_STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            let mut seed = [0u8; 8];
            let _ = getrandom::getrandom(&mut seed);
            x = u64::from_le_bytes(seed) | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        (x >> 11) as f64 / (1u64 << 53) as f64 <= rate
    })
}

/// Records whether an attempt to allocate a block of `block_size` bytes
/// succeeded.
pub fn record_allocate_block_result(source: BlockAllocationSource, block_size: usize, success: bool) {
    #[cfg(test)]
    if let Some(recorder) = testing::current() {
        recorder.record_allocate_block_result(source, BlockAllocationSize::from_block_size(block_size), success);
        return;
    }

    let installed = INSTALLED.read().unwrap_or_else(|e| e.into_inner());
    let Some(installed) = installed.as_ref() else {
        return;
    };
    if should_sample(installed.sample_rate) {
        installed.recorder.record_allocate_block_result(
            source,
            BlockAllocationSize::from_block_size(block_size),
            success,
        );
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Mutex;

    /// One recorded event.
    pub type Event = (BlockAllocationSource, BlockAllocationSize, bool);

    thread_local! {
        static CURRENT: std::cell::RefCell<Option<Arc<RecordedEvents>>> = const { std::cell::RefCell::new(None) };
    }

    pub(super) fn current() -> Option<Arc<RecordedEvents>> {
        CURRENT.with(|current| current.borrow().clone())
    }

    /// Collects every event recorded on this thread while alive, unsampled,
    /// like upstream's `ScopedAlwaysSampleForTesting` plus `HistogramTester`.
    #[derive(Default)]
    pub struct RecordedEvents {
        events: Mutex<Vec<Event>>,
    }

    impl MetricsRecorder for RecordedEvents {
        fn record_allocate_block_result(
            &self,
            source: BlockAllocationSource,
            size: BlockAllocationSize,
            success: bool,
        ) {
            self.events.lock().unwrap().push((source, size, success));
        }
    }

    impl RecordedEvents {
        pub fn capture() -> CaptureGuard {
            let events = Arc::new(RecordedEvents::default());
            CURRENT.with(|current| *current.borrow_mut() = Some(events.clone()));
            CaptureGuard(events)
        }

        /// Number of events for `source` with the given `success`.
        pub fn count_result(&self, source: BlockAllocationSource, success: bool) -> usize {
            self.count(|event| event.0 == source && event.2 == success)
        }

        /// Number of events for `source`, `success` and `size`.
        pub fn count_size(&self, source: BlockAllocationSource, success: bool, size: BlockAllocationSize) -> usize {
            self.count(|event| *event == (source, size, success))
        }

        /// Number of events for `source`.
        pub fn total(&self, source: BlockAllocationSource) -> usize {
            self.count(|event| event.0 == source)
        }

        fn count(&self, matches: impl Fn(&Event) -> bool) -> usize {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches(event))
                .count()
        }
    }

    pub struct CaptureGuard(Arc<RecordedEvents>);

    impl std::ops::Deref for CaptureGuard {
        type Target = RecordedEvents;

        fn deref(&self) -> &RecordedEvents {
            &self.0
        }
    }

    impl Drop for CaptureGuard {
        fn drop(&mut self) {
            CURRENT.with(|current| *current.borrow_mut() = None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_by_exact_power_of_two() {
        assert_eq!(BlockAllocationSize::B64, BlockAllocationSize::from_block_size(64));
        assert_eq!(BlockAllocationSize::KB16, BlockAllocationSize::from_block_size(16384));
        assert_eq!(BlockAllocationSize::MB1, BlockAllocationSize::from_block_size(1 << 20));
        assert_eq!(BlockAllocationSize::Other, BlockAllocationSize::from_block_size(0));
        assert_eq!(BlockAllocationSize::Other, BlockAllocationSize::from_block_size(96));
        assert_eq!(
            BlockAllocationSize::Other,
            BlockAllocationSize::from_block_size(2 << 20)
        );
    }

    #[test]
    fn subsampling() {
        assert!(should_sample(1.0));
        assert!(!(0..1000).any(|_| should_sample(0.0)));
        let hits = (0..10_000).filter(|_| should_sample(0.5)).count();
        assert!((4_000..6_000).contains(&hits), "{hits}");
    }
}
