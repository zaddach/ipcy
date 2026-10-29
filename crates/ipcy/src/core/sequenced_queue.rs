//! Port of `ipcz/sequenced_queue.h`.

use std::marker::PhantomData;

use super::sequence_number::{next_sequence_number, SequenceNumber};

/// Attributes a measurable size to each element stored in a [`SequencedQueue`].
pub trait SequencedQueueTraits<T> {
    fn get_element_size(_element: &T) -> usize {
        0
    }
}

/// Port of `DefaultSequencedQueueTraits`: every element has size zero.
pub struct DefaultSequencedQueueTraits;

impl<T> SequencedQueueTraits<T> for DefaultSequencedQueueTraits {}

#[derive(Debug)]
struct Entry<T> {
    element: T,

    // See the comment on `Entry` in sequenced_queue.h: the first and last entry
    // of each contiguous span carry accurate metadata about that span.
    num_entries_in_span: usize,
    total_span_size: usize,
    span_start: SequenceNumber,
    span_end: SequenceNumber,
}

/// Retains a queue of objects strictly ordered by [`SequenceNumber`]. This type
/// is not thread-safe.
///
/// Storage is linear from the last popped sequence number (exclusive) to the
/// highest queued (or anticipated) sequence number (inclusive), and is compacted
/// as elements are consumed.
#[derive(Debug)]
pub struct SequencedQueue<T, ElementTraits = DefaultSequencedQueueTraits> {
    /// Sparse storage. When non-empty, `entries[front_index]` corresponds to
    /// `base_sequence_number`; entries below `front_index` are always `None`.
    entries: Vec<Option<Entry<T>>>,
    front_index: usize,
    /// If and only if this is true, the final length of the sequence is known
    /// and is determined by the size of `entries` relative to `front_index`.
    is_final_length_known: bool,
    base_sequence_number: SequenceNumber,
    _traits: PhantomData<ElementTraits>,
}

impl<T, E> Default for SequencedQueue<T, E> {
    fn default() -> Self {
        Self::new(SequenceNumber::new(0))
    }
}

impl<T, E> SequencedQueue<T, E> {
    /// As a basic practical constraint, a SequencedQueue won't tolerate
    /// sequence gaps larger than this value.
    pub const fn get_max_sequence_gap() -> u64 {
        1_000_000
    }

    /// Constructs a queue starting at `initial_sequence_number`. It will not
    /// accept elements below this value, and this will be the sequence number
    /// of the first element to be popped.
    pub fn new(initial_sequence_number: SequenceNumber) -> Self {
        Self {
            entries: Vec::new(),
            front_index: 0,
            is_final_length_known: false,
            base_sequence_number: initial_sequence_number,
            _traits: PhantomData,
        }
    }

    /// The sequence number of the next element that is or will be available.
    pub fn current_sequence_number(&self) -> SequenceNumber {
        self.base_sequence_number
    }

    /// The final length of the sequence, if it has been set.
    pub fn final_sequence_length(&self) -> Option<SequenceNumber> {
        self.is_final_length_known.then(|| {
            SequenceNumber::new(
                self.base_sequence_number.value() + (self.entries.len() - self.front_index) as u64,
            )
        })
    }

    fn head(&self) -> Option<&Entry<T>> {
        self.entries.get(self.front_index)?.as_ref()
    }

    /// Number of contiguously sequenced elements ready for popping.
    pub fn get_num_available_elements(&self) -> usize {
        self.head().map_or(0, |head| head.num_entries_in_span)
    }

    /// Sum of element sizes over the elements counted by
    /// [`Self::get_num_available_elements`], in constant time.
    pub fn get_total_available_element_size(&self) -> usize {
        self.head().map_or(0, |head| head.total_span_size)
    }

    /// The total length of the contiguous sequence already pushed and/or popped.
    pub fn get_current_sequence_length(&self) -> SequenceNumber {
        SequenceNumber::new(
            self.current_sequence_number().value() + self.get_num_available_elements() as u64,
        )
    }

    /// Sets the final length of this queue's sequence. Fails if elements at or
    /// beyond `length` were already pushed, or if the length was already set.
    pub fn set_final_sequence_length(&mut self, length: SequenceNumber) -> bool {
        if self.is_final_length_known {
            return false;
        }

        debug_assert!(
            (self.entries.is_empty() && self.front_index == 0)
                || self.front_index < self.entries.len()
        );
        let min_gap = self.entries.len() - self.front_index;

        let gap = length.value().wrapping_sub(self.base_sequence_number.value());
        if gap < min_gap as u64 || gap > Self::get_max_sequence_gap() {
            return false;
        }

        self.is_final_length_known = true;

        // Resize storage to exactly fit whatever remaining entries are expected
        // after `front_index`.
        self.entries
            .resize_with(self.front_index + gap as usize, || None);
        if self.entries.is_empty() {
            self.reset_and_release_storage();
        }
        true
    }

    /// Forcibly sets the final length to the currently available length,
    /// destroying any non-contiguous elements pushed beyond it. Returns the
    /// purged elements.
    #[must_use]
    pub fn force_terminate_sequence(&mut self) -> Vec<T> {
        self.is_final_length_known = true;
        let length = self.get_current_sequence_length();
        let required_storage_size = (length.value() - self.base_sequence_number.value()) as usize;
        if required_storage_size == 0 {
            // We're not going to be pushing any more entries into this queue.
            self.reset_and_release_storage();
            return Vec::new();
        }

        // Drop entries pushed anywhere beyond the forced termination point.
        let final_storage_size = self.front_index + required_storage_size;
        self.entries
            .split_off(final_storage_size.min(self.entries.len()))
            .into_iter()
            .flatten()
            .map(|entry| entry.element)
            .collect()
    }

    /// True if more elements may still be pushed. Always true until the final
    /// sequence length has been set.
    pub fn expects_more_elements(&self) -> bool {
        self.final_sequence_length()
            .is_none_or(|length| self.get_current_sequence_length() < length)
    }

    /// True if the next element in sequence order is available to pop.
    pub fn has_next_element(&self) -> bool {
        self.head().is_some()
    }

    /// True if the final length is set and every element up to it was pushed
    /// and popped.
    pub fn is_sequence_fully_consumed(&self) -> bool {
        !self.has_next_element() && !self.expects_more_elements()
    }

    /// Resets this (empty) queue as if a sequence of length `n` had already been
    /// pushed and popped.
    pub fn reset_sequence(&mut self, n: SequenceNumber) {
        debug_assert!(self.entries.is_empty());
        self.base_sequence_number = n;
        self.is_final_length_known = false;
        self.reset_and_release_storage();
    }

    /// Skips sequence number `n` by advancing the current sequence number. Only
    /// succeeds if `n` is the current sequence number, no entry for it exists,
    /// and it is below the final length if one is set.
    pub fn skip_element(&mut self, n: SequenceNumber) -> bool {
        if self.base_sequence_number != n || self.has_next_element() {
            return false;
        }

        if self.final_sequence_length().is_some_and(|length| n >= length) {
            return false;
        }

        self.base_sequence_number = next_sequence_number(n);
        if self.entries.is_empty() {
            // Nothing else needs to change if storage is unoccupied.
            debug_assert_eq!(self.front_index, 0);
            return true;
        }

        self.front_index += 1;
        if self.front_index == self.entries.len() {
            // We've hit the end of storage, so all elements are null.
            self.reset_storage();
        }
        true
    }

    /// Gets a reference to the next element. Panics if there is none.
    pub fn next_element(&mut self) -> &mut T {
        let index = self.front_index;
        &mut self.entries[index]
            .as_mut()
            .expect("next_element() without has_next_element()")
            .element
    }

    // Grows `entries` to fit a new element at offset `gap` from `front_index`,
    // compacting away consumed prefix slots first if growth would exceed
    // capacity. Callers must recompute indices derived from `front_index`.
    fn grow_with_compaction(&mut self, gap: usize) {
        let mut required = self.front_index + gap + 1;

        // If growth would exceed capacity, compact first to reclaim front slack.
        if required > self.entries.capacity() && self.front_index > 0 {
            let old_front_index = self.front_index;
            let active_size = self.entries.len() - old_front_index;
            self.front_index = 0;
            required = gap + 1;
            debug_assert!(required > active_size);

            if required <= self.entries.capacity() {
                // Capacity is sufficient after compaction; shift active entries
                // to the front in place.
                self.entries.drain(..old_front_index);
            } else {
                // Compaction alone is insufficient; allocate a new buffer.
                // Reserve at least doubled effective capacity to avoid frequent
                // reallocations.
                let doubled = (self.entries.capacity() - old_front_index) * 2;
                let mut new_entries = Vec::with_capacity(required.max(doubled));
                new_entries.extend(self.entries.drain(old_front_index..));
                self.entries = new_entries;
            }
        }

        self.entries.resize_with(required, || None);
    }

    // Wipes out logical storage and resets `front_index`, keeping capacity in
    // anticipation of reuse.
    fn reset_storage(&mut self) {
        self.entries.clear();
        self.front_index = 0;
    }

    // Like `reset_storage`, but also releases capacity. Used when the queue will
    // no longer accept new elements.
    fn reset_and_release_storage(&mut self) {
        self.reset_storage();
        self.entries.shrink_to_fit();
    }
}

impl<T, E: SequencedQueueTraits<T>> SequencedQueue<T, E> {
    /// Pushes an element with the given sequence number. Fails if `n` is below
    /// the current sequence number, at or above the final length, too far ahead,
    /// or already occupied.
    pub fn push(&mut self, n: SequenceNumber, element: T) -> bool {
        if n < self.base_sequence_number {
            return false;
        }

        if self.final_sequence_length().is_some_and(|length| n >= length) {
            return false;
        }

        let gap = n.value() - self.base_sequence_number.value();
        if gap > Self::get_max_sequence_gap() {
            return false;
        }
        let gap = gap as usize;

        let mut index = self.front_index + gap;
        if index >= self.entries.len() {
            self.grow_with_compaction(gap);
            index = self.front_index + gap;
        } else if self.entries[index].is_some() {
            return false;
        }

        self.place_new_entry(index, n, element);
        true
    }

    /// Pops the next element in sequence order, if available.
    pub fn pop(&mut self) -> Option<T> {
        let head = self.entries.get_mut(self.front_index)?.take()?;
        let element = head.element;

        let sequence_number = self.base_sequence_number;
        self.base_sequence_number = next_sequence_number(sequence_number);

        // Make sure the next queued entry has up-to-date accounting, if present.
        let element_size = E::get_element_size(&element);
        let front_index = self.front_index;
        if let Some(Some(next)) = self.entries.get_mut(front_index + 1) {
            next.span_start = head.span_start;
            next.span_end = head.span_end;
            next.num_entries_in_span = head.num_entries_in_span - 1;
            next.total_span_size = head.total_span_size - element_size;
            let (span_end, num_entries, total_size) =
                (next.span_end, next.num_entries_in_span, next.total_span_size);

            // Find the tail entry for this span. If the offset is 1, it's the
            // same entry as the new head which we already updated above.
            let tail_offset = (span_end.value() - sequence_number.value()) as usize;
            if tail_offset > 1 {
                let tail = self.entries[front_index + tail_offset]
                    .as_mut()
                    .expect("span tail must be present");
                tail.num_entries_in_span = num_entries;
                tail.total_span_size = total_size;
            }
        }

        if self.front_index < self.entries.len() - 1 {
            self.front_index += 1;
        } else {
            // This was the last element in storage. Now we can reuse all capacity.
            self.reset_storage();
        }
        Some(element)
    }

    fn place_new_entry(&mut self, index: usize, n: SequenceNumber, element: T) {
        debug_assert!(index < self.entries.len());
        debug_assert!(self.entries[index].is_none());

        let mut entry = Entry {
            num_entries_in_span: 1,
            total_span_size: E::get_element_size(&element),
            element,
            span_start: n,
            span_end: n,
        };

        if let Some(Some(left)) = index.checked_sub(1).map(|i| &self.entries[i]) {
            entry.span_start = left.span_start;
            entry.num_entries_in_span += left.num_entries_in_span;
            entry.total_span_size += left.total_span_size;
        }

        if let Some(Some(right)) = self.entries.get(index + 1) {
            entry.span_end = right.span_end;
            entry.num_entries_in_span += right.num_entries_in_span;
            entry.total_span_size += right.total_span_size;
        }

        let (span_start, span_end, num, total) = (
            entry.span_start,
            entry.span_end,
            entry.num_entries_in_span,
            entry.total_span_size,
        );
        self.entries[index] = Some(entry);

        let start_index = if span_start <= self.base_sequence_number {
            self.front_index
        } else {
            self.front_index + (span_start.value() - self.base_sequence_number.value()) as usize
        };

        debug_assert!(span_end >= self.base_sequence_number);
        let end_index =
            self.front_index + (span_end.value() - self.base_sequence_number.value()) as usize;
        debug_assert!(end_index < self.entries.len());

        let start = self.entries[start_index].as_mut().expect("span head");
        start.span_end = span_end;
        start.num_entries_in_span = num;
        start.total_span_size = total;

        let end = self.entries[end_index].as_mut().expect("span tail");
        end.span_start = span_start;
        end.num_entries_in_span = num;
        end.total_span_size = total;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestQueueTraits;

    impl SequencedQueueTraits<String> for TestQueueTraits {
        fn get_element_size(s: &String) -> usize {
            s.len()
        }
    }

    type TestQueue = SequencedQueue<String>;
    type TestQueueWithSize = SequencedQueue<String, TestQueueTraits>;

    fn sn(n: u64) -> SequenceNumber {
        SequenceNumber::new(n)
    }

    #[test]
    fn empty() {
        let mut q = TestQueue::default();
        assert!(q.expects_more_elements());
        assert!(!q.has_next_element());
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn set_final_sequence_length() {
        let mut q = TestQueue::default();
        q.set_final_sequence_length(sn(3));
        assert!(q.expects_more_elements());
        assert!(!q.has_next_element());
        assert_eq!(q.pop(), None);

        let entries = ["zero", "one", "two"].map(String::from);
        assert!(q.push(sn(2), entries[2].clone()));
        assert!(!q.has_next_element());
        assert_eq!(q.pop(), None);
        assert!(q.expects_more_elements());

        assert!(q.push(sn(0), entries[0].clone()));
        assert!(q.has_next_element());
        assert!(q.expects_more_elements());
        assert_eq!(q.pop(), Some(entries[0].clone()));

        assert!(!q.has_next_element());
        assert_eq!(q.pop(), None);
        assert!(q.expects_more_elements());

        assert!(q.push(sn(1), entries[1].clone()));
        assert!(!q.expects_more_elements());
        assert!(q.has_next_element());
        assert_eq!(q.pop(), Some(entries[1].clone()));
        assert!(!q.expects_more_elements());
        assert!(q.has_next_element());
        assert_eq!(q.pop(), Some(entries[2].clone()));
        assert!(!q.expects_more_elements());
        assert!(!q.has_next_element());
    }

    #[test]
    fn force_terminate_sequence() {
        let mut q = TestQueue::default();
        q.set_final_sequence_length(sn(3));
        assert!(q.expects_more_elements());
        assert!(!q.has_next_element());

        // Push elements 0 and 2, leaving the current sequence length at 1, due
        // to the gap in element 1.
        assert!(q.push(sn(0), "woot.".into()));
        assert!(q.push(sn(2), "woot!".into()));
        assert!(q.expects_more_elements());
        assert!(q.has_next_element());
        assert_eq!(q.get_current_sequence_length(), sn(1));

        // We can't normally change the final sequence length once set.
        assert!(!q.set_final_sequence_length(sn(4)));
        assert!(!q.set_final_sequence_length(sn(0)));
        assert!(!q.set_final_sequence_length(sn(1)));

        // But we can still force it to terminate at its current length.
        assert_eq!(q.force_terminate_sequence(), ["woot!"]);
        assert!(!q.expects_more_elements());
        assert!(q.has_next_element());
        assert!(!q.push(sn(1), "woot?".into()));
    }

    #[test]
    fn sequence_too_low() {
        let mut q = TestQueue::default();
        let entries = ["a", "b", "c"].map(String::from);

        assert!(q.push(sn(0), entries[0].clone()));
        assert_eq!(q.pop(), Some(entries[0].clone()));

        // We can't push another element for sequence number 0.
        assert!(!q.push(sn(0), entries[0].clone()));

        // Out-of-order is of course fine.
        assert!(q.push(sn(2), entries[2].clone()));
        assert!(q.push(sn(1), entries[1].clone()));

        assert_eq!(q.pop(), Some(entries[1].clone()));
        assert_eq!(q.pop(), Some(entries[2].clone()));

        // But we can't revisit sequence number 1 or 2 either.
        assert!(!q.push(sn(2), entries[2].clone()));
        assert!(!q.push(sn(1), entries[1].clone()));
    }

    #[test]
    fn sequence_too_high() {
        let mut q = TestQueue::default();
        q.set_final_sequence_length(sn(5));
        assert!(!q.push(sn(5), "doesn't matter".into()));
    }

    #[test]
    fn sparse_sequence() {
        let mut q = TestQueue::default();

        // Push a sparse but eventually complete sequence of elements and ensure
        // they can only be popped out in sequence-order.
        let entries = "0123456789abcdef".chars().map(String::from).collect::<Vec<_>>();
        let mut expected = entries.iter();
        [5, 2, 1, 0, 4, 3, 9, 6, 8, 7, 10, 11, 12, 15, 13, 14]
            .into_iter()
            .for_each(|n| {
                assert!(q.push(sn(n), entries[n as usize].clone()));
                std::iter::from_fn(|| q.pop())
                    .for_each(|s| assert_eq!(expected.next(), Some(&s)));
            });

        assert_eq!(expected.next(), None);
    }

    #[test]
    fn fully_consumed() {
        let mut empty_queue = TestQueue::default();
        assert!(!empty_queue.is_sequence_fully_consumed());
        assert!(empty_queue.set_final_sequence_length(sn(0)));
        assert!(empty_queue.is_sequence_fully_consumed());

        let mut q = TestQueue::default();
        assert!(!q.is_sequence_fully_consumed());

        ["a", "b", "c"]
            .into_iter()
            .enumerate()
            .for_each(|(i, s)| assert!(q.push(sn(i as u64), s.into())));

        assert!(q.set_final_sequence_length(sn(3)));
        assert!(!q.is_sequence_fully_consumed());

        assert!(q.pop().is_some());
        assert!(q.pop().is_some());
        assert!(q.pop().is_some());
        assert!(q.is_sequence_fully_consumed());
    }

    #[test]
    fn skip_element() {
        let mut q = TestQueueWithSize::default();
        let entry = "woot".to_string();

        // Skipping an element should update accounting appropriately.
        assert!(q.skip_element(sn(0)));
        assert_eq!(q.get_total_available_element_size(), 0);

        // We can't skip or push an element that's already been skipped.
        assert!(!q.skip_element(sn(0)));
        assert!(!q.push(sn(0), entry.clone()));

        // And we can't skip an element that's already been pushed.
        assert!(q.push(sn(1), entry.clone()));
        assert!(!q.skip_element(sn(1)));
        assert_eq!(q.get_total_available_element_size(), entry.len());

        assert!(q.pop().is_some());
        assert_eq!(q.get_total_available_element_size(), 0);

        // Skip ahead past sequence numbers 2 and 3.
        assert!(q.skip_element(sn(2)));
        assert_eq!(q.get_total_available_element_size(), 0);
        assert!(q.skip_element(sn(3)));
        assert_eq!(q.get_total_available_element_size(), 0);

        // Sequence number 4 can now be pushed while 2 and 3 cannot.
        assert!(!q.push(sn(2), entry.clone()));
        assert!(!q.push(sn(3), entry.clone()));
        assert!(q.push(sn(4), entry.clone()));
        assert!(!q.skip_element(sn(4)));
        assert_eq!(q.get_total_available_element_size(), entry.len());

        // Cap the sequence at 6 elements and verify that accounting remains
        // intact when we skip the last element.
        assert!(q.set_final_sequence_length(sn(6)));
        assert!(!q.is_sequence_fully_consumed());
        assert!(q.pop().is_some());
        assert!(!q.is_sequence_fully_consumed());
        assert!(q.skip_element(sn(5)));
        assert_eq!(q.get_total_available_element_size(), 0);
        assert!(q.is_sequence_fully_consumed());

        // Fully consumed queue: skipping must fail.
        assert!(!q.skip_element(sn(6)));
    }

    #[test]
    fn accounting() {
        let mut q = TestQueueWithSize::default();
        let e = ["hello", "world", "one", "two", "three"].map(String::from);

        // Elements not at the head of the queue are not considered available.
        assert!(q.push(sn(3), e[3].clone()));
        assert_eq!(q.get_num_available_elements(), 0);
        assert_eq!(q.get_total_available_element_size(), 0);
        assert!(!q.has_next_element());

        assert!(q.push(sn(1), e[1].clone()));
        assert_eq!(q.get_num_available_elements(), 0);
        assert_eq!(q.get_total_available_element_size(), 0);
        assert!(!q.has_next_element());

        // Now insert at the head: elements 0 and 1 count, but not element 3.
        assert!(q.push(sn(0), e[0].clone()));
        assert_eq!(q.get_num_available_elements(), 2);
        assert_eq!(q.get_total_available_element_size(), e[0].len() + e[1].len());
        assert!(q.has_next_element());

        // Finally insert element 2, after which all 4 elements count.
        assert!(q.push(sn(2), e[2].clone()));
        assert_eq!(q.get_num_available_elements(), 4);
        assert_eq!(
            q.get_total_available_element_size(),
            e[0].len() + e[1].len() + e[2].len() + e[3].len()
        );

        // Popping should also update the accounting properly.
        assert_eq!(q.pop(), Some(e[0].clone()));
        assert_eq!(q.get_num_available_elements(), 3);
        assert_eq!(
            q.get_total_available_element_size(),
            e[1].len() + e[2].len() + e[3].len()
        );

        assert_eq!(q.pop(), Some(e[1].clone()));
        assert_eq!(q.get_num_available_elements(), 2);
        assert_eq!(q.get_total_available_element_size(), e[2].len() + e[3].len());

        // Insert another at the end after popping a few to verify below that
        // pops also update the tail of the leading span.
        assert!(q.push(sn(4), e[4].clone()));

        assert_eq!(q.pop(), Some(e[2].clone()));
        assert_eq!(q.get_num_available_elements(), 2);
        assert_eq!(q.get_total_available_element_size(), e[3].len() + e[4].len());

        assert_eq!(q.pop(), Some(e[3].clone()));
        assert_eq!(q.get_num_available_elements(), 1);
        assert_eq!(q.get_total_available_element_size(), e[4].len());

        assert_eq!(q.pop(), Some(e[4].clone()));
        assert_eq!(q.get_num_available_elements(), 0);
        assert_eq!(q.get_total_available_element_size(), 0);
    }

    #[test]
    fn grow_with_compaction_without_compaction() {
        let mut q = TestQueue::default();

        // `front_index == 0`, so growth cannot reclaim any front slack. Pushing
        // an entry just beyond the current logical extent must grow storage
        // without entering either compaction path.
        q.entries.reserve(4);
        assert!(q.push(sn(3), "3".into()));

        let initial_capacity = q.entries.capacity();
        let initial_size = q.entries.len();
        assert_eq!(q.front_index, 0);
        assert_eq!(q.entries.len(), 4);

        assert!(q.push(sn(4), "4".into()));

        // DEVIATION: the C++ test also asserts that the data pointer changed,
        // but Rust's `realloc` may legitimately grow the buffer in place.
        assert_eq!(q.front_index, 0);
        assert_eq!(q.entries.len(), initial_size + 1);
        assert!(q.entries.capacity() > initial_capacity);

        assert_eq!(q.pop(), None);
        assert!(q.push(sn(0), "0".into()));
        assert_eq!(q.pop().as_deref(), Some("0"));
    }

    #[test]
    fn grow_with_compaction_in_place() {
        let mut q = TestQueue::default();

        // Pop once to create front slack, then push far enough to require
        // growth. After compaction the required size still fits in the existing
        // capacity, so `grow_with_compaction()` should compact in place.
        q.entries.reserve(8);
        assert!(q.push(sn(0), "0".into()));
        assert!(q.push(sn(5), "5".into()));

        assert_eq!(q.pop().as_deref(), Some("0"));
        assert_eq!(q.front_index, 1);
        let old_capacity = q.entries.capacity();
        let old_data = q.entries.as_ptr();

        assert!(q.push(sn(7), "7".into()));

        assert_eq!(old_data, q.entries.as_ptr());
        assert_eq!(old_capacity, q.entries.capacity());
        assert!(q.entries.len() > 6);

        assert!(!q.push(sn(0), "again".into()));
        assert!(q.push(sn(1), "1".into()));
        assert_eq!(q.pop().as_deref(), Some("1"));
    }

    #[test]
    fn grow_with_compaction_without_compaction_with_front_slack() {
        let mut q = TestQueue::default();

        // Pop once to create front slack, but keep enough spare capacity that
        // the next push only needs `resize(required)`. This should bypass
        // compaction even though `front_index > 0`.
        q.entries.reserve(8);
        assert!(q.push(sn(0), "0".into()));
        assert!(q.push(sn(5), "5".into()));

        assert_eq!(q.pop().as_deref(), Some("0"));
        assert_eq!(q.front_index, 1);

        let old_data = q.entries.as_ptr();
        let old_capacity = q.entries.capacity();
        let old_size = q.entries.len();

        assert!(q.push(sn(6), "6".into()));

        assert_eq!(q.front_index, 1);
        assert_eq!(old_data, q.entries.as_ptr());
        assert_eq!(old_capacity, q.entries.capacity());
        assert_eq!(q.entries.len(), old_size + 1);

        assert!(q.push(sn(1), "1".into()));
        assert_eq!(q.pop().as_deref(), Some("1"));
    }

    #[test]
    fn grow_with_compaction_to_new_buffer() {
        let mut q = TestQueue::default();

        // Pop once to create front slack, then push far enough that compaction
        // alone still cannot satisfy the required size. This must take the
        // new-buffer compaction path.
        q.entries.reserve(4);
        assert!(q.push(sn(0), "0".into()));
        assert!(q.push(sn(3), "3".into()));

        assert_eq!(q.pop().as_deref(), Some("0"));
        assert_eq!(q.front_index, 1);

        let full_capacity = q.entries.capacity();
        let old_data = q.entries.as_ptr();
        assert!(q.push(sn(5), "5".into()));

        assert_eq!(q.front_index, 0);
        assert_ne!(old_data, q.entries.as_ptr());
        assert_eq!(q.entries.len(), 5);
        assert!(q.entries.capacity() > full_capacity);

        assert!(!q.push(sn(0), "again".into()));
        assert!(q.push(sn(1), "1".into()));
        assert!(q.push(sn(2), "2".into()));
        assert_eq!(q.pop().as_deref(), Some("1"));
        assert_eq!(q.pop().as_deref(), Some("2"));
        assert_eq!(q.pop().as_deref(), Some("3"));
    }
}
