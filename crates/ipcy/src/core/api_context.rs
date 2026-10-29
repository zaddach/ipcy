//! Port of `ipcz/api_context.{h,cc}`.

use std::cell::Cell;

thread_local! {
    static IS_THREAD_WITHIN_API_CALL: Cell<bool> = const { Cell::new(false) };
}

/// Constructed on the stack immediately upon entry to any API function. This is
/// used to surface reentrancy to trap event handlers.
#[derive(Debug)]
pub struct ApiContext {
    /// Whether or not the current thread already had an ApiContext on it when
    /// this one was constructed.
    was_thread_within_api_call: bool,
}

impl ApiContext {
    pub fn new() -> Self {
        Self {
            was_thread_within_api_call: IS_THREAD_WITHIN_API_CALL.replace(true),
        }
    }

    /// Indicates whether the current call stack contains an API entry point.
    pub fn is_current_thread_within_api_call() -> bool {
        IS_THREAD_WITHIN_API_CALL.get()
    }
}

impl Default for ApiContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ApiContext {
    fn drop(&mut self) {
        IS_THREAD_WITHIN_API_CALL.set(self.was_thread_within_api_call);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nesting() {
        assert!(!ApiContext::is_current_thread_within_api_call());
        {
            let _outer = ApiContext::new();
            assert!(ApiContext::is_current_thread_within_api_call());
            {
                let _inner = ApiContext::new();
                assert!(ApiContext::is_current_thread_within_api_call());
            }
            assert!(ApiContext::is_current_thread_within_api_call());
        }
        assert!(!ApiContext::is_current_thread_within_api_call());
    }

    #[test]
    fn per_thread() {
        let _context = ApiContext::new();
        std::thread::spawn(|| assert!(!ApiContext::is_current_thread_within_api_call()))
            .join()
            .unwrap();
    }
}
