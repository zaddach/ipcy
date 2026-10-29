//! Port of `ipcz/pending_transaction_set.{h,cc}`.

use std::collections::BTreeMap;

use super::ipcz_types::IpczTransaction;
use super::parcel::Parcel;

/// Wraps a set of pending Parcel objects. Every Parcel owned by this set has a
/// stable address throughout its lifetime (it's boxed), exposed as an opaque
/// [`IpczTransaction`] value.
#[derive(Default)]
pub struct PendingTransactionSet {
    transactions: BTreeMap<IpczTransaction, Box<Parcel>>,
}

fn as_ipcz_transaction(parcel: &Parcel) -> IpczTransaction {
    parcel as *const Parcel as IpczTransaction
}

impl PendingTransactionSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Adds `parcel` to this set, returning an opaque IpczTransaction value to
    /// reference it.
    pub fn add(&mut self, parcel: Box<Parcel>) -> IpczTransaction {
        let transaction = as_ipcz_transaction(&parcel);
        self.transactions.insert(transaction, parcel);
        transaction
    }

    /// Finalizes the transaction identified by `transaction`, returning its
    /// underlying Parcel. Only succeeds if `transaction` is a valid transaction.
    pub fn finalize_for_get(&mut self, transaction: IpczTransaction) -> Option<Box<Parcel>> {
        self.transactions.remove(&transaction)
    }

    /// Finalizes the transaction identified by `transaction`, returning its
    /// underlying Parcel so that data can be committed to it and it can be put
    /// into a portal. Only succeeds if `transaction` is a valid transaction and
    /// `num_data_bytes` does not exceed the total capacity of the underlying
    /// Parcel. Note that this does not actually commit any data to the parcel.
    pub fn finalize_for_put(&mut self, transaction: IpczTransaction, num_data_bytes: usize) -> Option<Box<Parcel>> {
        match self.transactions.get(&transaction) {
            Some(parcel) if parcel.data_view().len() >= num_data_bytes => self.transactions.remove(&transaction),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sequence_number::SequenceNumber;

    #[test]
    fn add_and_finalize() {
        let mut set = PendingTransactionSet::new();
        assert!(set.is_empty());

        let first = set.add(Box::new(Parcel::new(SequenceNumber::new(1))));
        let second = set.add(Box::new(Parcel::new(SequenceNumber::new(2))));
        assert_ne!(first, second);
        assert!(!set.is_empty());

        assert!(set.finalize_for_get(0).is_none());
        assert_eq!(set.finalize_for_get(second).unwrap().sequence_number(), SequenceNumber::new(2));
        // A transaction can only be finalized once.
        assert!(set.finalize_for_get(second).is_none());
        assert_eq!(set.finalize_for_get(first).unwrap().sequence_number(), SequenceNumber::new(1));
        assert!(set.is_empty());
    }

    #[test]
    fn put_respects_capacity() {
        let mut set = PendingTransactionSet::new();
        let mut parcel = Box::new(Parcel::default());
        parcel.allocate_data(16, false, None);
        let transaction = set.add(parcel);

        // Claiming more data than the parcel can hold is rejected and leaves the
        // transaction pending.
        assert!(set.finalize_for_put(transaction, 17).is_none());
        assert!(!set.is_empty());
        assert!(set.finalize_for_put(transaction, 16).is_some());
        assert!(set.is_empty());
    }
}
