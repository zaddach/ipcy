//! Port of `ipcz/features.{h,cc}`.
//!

use super::message::Message;

/// Mirrors `IpczFeature` from ipcz.h.
pub type IpczFeature = u32;
pub const IPCZ_FEATURE_MEM_V2: IpczFeature = 0xA110_C002;

type Bitfield = u64;
type BitIndex = (usize, u64);

// Internal bit indices for different features.
const MEM_V2_BIT: BitIndex = (0, 0);

/// Flags for any available ipcz features that might be enabled on a node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Features {
    // Features are serialized to a variable-length array of 64-bit bitfields.
    // This lends some flexibility over time, retaining wire compatibility as
    // the number of features increases.
    bitfield_values: [Bitfield; 1],
}

impl Features {
    /// Computes the intersection of two feature sets.
    pub fn intersect(&self, rhs: &Features) -> Features {
        let mut features = Features::default();
        features
            .bitfield_values
            .iter_mut()
            .zip(self.bitfield_values.iter().zip(&rhs.bitfield_values))
            .for_each(|(out, (a, b))| *out = a & b);
        features
    }

    /// Extracts a Features value from the enabled and disabled feature lists of
    /// node options. A feature in both lists is disabled.
    pub fn from_node_options(enabled: &[IpczFeature], disabled: &[IpczFeature]) -> Features {
        let mut features = Features::default();
        enabled
            .iter()
            .for_each(|&id| features.set_feature_enabled(id, true));
        disabled
            .iter()
            .for_each(|&id| features.set_feature_enabled(id, false));
        features
    }

    pub fn mem_v2(&self) -> bool {
        self.bit(MEM_V2_BIT)
    }

    /// The raw bitfield array, as serialized on the wire.
    pub fn bitfields(&self) -> &[u64] {
        &self.bitfield_values
    }

    /// Builds Features from a serialized bitfield array. The serialized features
    /// may be smaller (or larger) than the current version; excess is ignored.
    pub fn from_bitfields(view: &[u64]) -> Features {
        let mut features = Features::default();
        features
            .bitfield_values
            .iter_mut()
            .zip(view)
            .for_each(|(out, &v)| *out = v);
        features
    }

    /// Serializes this object into an internal ipcz message and returns the
    /// offset of the encoded data within the message, to be assigned to a field
    /// value.
    pub fn serialize(&self, message: &mut Message) -> u32 {
        message.allocate_and_set_array(&self.bitfield_values)
    }

    /// Deserializes from an internal ipcz message given the encoded offset of an
    /// array of Bitfields. `offset` must already be validated as the location of
    /// a valid Bitfield array header (see `Message`'s parameter validation).
    pub fn deserialize(message: &Message, offset: u32) -> Features {
        // NOTE: The serialized features may be smaller than the current version.
        Features::from_bitfields(message.get_array_view::<u64>(offset as usize))
    }

    fn bit(&self, (index, bit): BitIndex) -> bool {
        self.bitfield_values[index] & (1 << bit) != 0
    }

    fn set_bit(&mut self, (index, bit): BitIndex, enabled: bool) {
        let value = 1u64 << bit;
        let bitfield = &mut self.bitfield_values[index];
        *bitfield = (*bitfield & !value) | if enabled { value } else { 0 };
    }

    #[allow(clippy::single_match)] // A `match` so new features slot in alongside.
    fn set_feature_enabled(&mut self, feature: IpczFeature, enabled: bool) {
        match feature {
            IPCZ_FEATURE_MEM_V2 => self.set_bit(MEM_V2_BIT, enabled),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_disabled() {
        assert!(!Features::default().mem_v2());
        assert!(!Features::from_node_options(&[], &[]).mem_v2());
    }

    #[test]
    fn disabled_wins_over_enabled() {
        let on = Features::from_node_options(&[IPCZ_FEATURE_MEM_V2], &[]);
        assert!(on.mem_v2());
        let off = Features::from_node_options(&[IPCZ_FEATURE_MEM_V2], &[IPCZ_FEATURE_MEM_V2]);
        assert!(!off.mem_v2());
    }

    #[test]
    fn unknown_features_ignored() {
        assert_eq!(Features::from_node_options(&[0x1234], &[]), Features::default());
    }

    #[test]
    fn message_roundtrip() {
        use crate::core::node_messages::ConnectFromNonBrokerToBroker;
        let features = Features::from_node_options(&[IPCZ_FEATURE_MEM_V2], &[]);

        let mut message = ConnectFromNonBrokerToBroker::new();
        let offset = features.serialize(&mut message);
        message.v1_mut().unwrap().features = offset;
        assert_ne!(offset, 0);

        // Features absent from the message deserialize as no features.
        assert_eq!(Features::deserialize(&message, 0), Features::default());
        assert_eq!(Features::deserialize(&message, offset), features);
    }

    #[test]
    fn intersect_and_roundtrip() {
        let on = Features::from_node_options(&[IPCZ_FEATURE_MEM_V2], &[]);
        assert!(on.intersect(&on).mem_v2());
        assert!(!on.intersect(&Features::default()).mem_v2());
        assert_eq!(Features::from_bitfields(on.bitfields()), on);
        assert_eq!(Features::from_bitfields(&[]), Features::default());
        assert_eq!(Features::from_bitfields(&[1, 7, 7]), on);
    }
}
