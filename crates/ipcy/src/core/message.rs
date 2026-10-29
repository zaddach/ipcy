//! Port of `ipcz/message.{h,cc}`.
//!
//! DEVIATIONS from upstream:
//! - Message storage is a single 8-byte-aligned buffer ([`AlignedBytes`]) for
//!   both locally built and received messages (upstream distinguishes inlined
//!   and malloc'd storage), so typed views of arrays are always well aligned.
//!   [`Message::take_received_data`] hands back the bytes as a `Vec<u8>`.
//! - Array offsets read from incoming messages must additionally be 8-byte
//!   aligned to be valid. Conforming senders always align them.
//! - Parameter structs are accessed through bounds-checked accessors generated
//!   by [`define_messages!`] rather than a reference to the whole latest-version
//!   struct, because a received message may be shorter than the latest version.
//! - Enum-typed parameters are stored as their raw wire integers and converted
//!   with [`WireEnum`], so malformed bytes never form an invalid enum value.

use std::mem::size_of;
use std::ptr;

use super::driver::{DriverHandle, RawMessage};
use super::driver_object::DriverObject;
use super::driver_transport::DriverTransport;
use super::safe_math::{check_add, check_mul, checked_cast};
use super::sequence_number::SequenceNumber;

/// Marker for plain-old-data types which may be read from (and written to)
/// arbitrary bytes of a message: every bit pattern is a valid value, and there
/// are no padding bytes.
///
/// # Safety
/// The implementing type must be `repr(C)`/`repr(transparent)`/integer-like
/// with no padding and no invalid bit patterns.
pub unsafe trait Pod: Copy + 'static {}

unsafe impl Pod for u8 {}
unsafe impl Pod for u16 {}
unsafe impl Pod for u32 {}
unsafe impl Pod for u64 {}

/// An enum which may be encoded as a message parameter. The wire value is a
/// `Raw` integer of one or four bytes, valid in the range `0..=MAX_VALUE`.
pub trait WireEnum: Copy {
    type Raw: Pod + Into<u32>;
    const MAX_VALUE: u32;
    fn from_raw(raw: Self::Raw) -> Option<Self>;
    fn to_raw(self) -> Self::Raw;
}

/// Wire structures. All of these are packed and so have alignment 1.
pub mod internal {
    use super::*;

    /// Header which begins all messages. The header layout is versioned for
    /// extensibility and long-term support.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    #[repr(C, packed)]
    pub struct MessageHeader {
        /// The size of the header in bytes.
        pub size: u8,

        /// The header version in use by this message.
        pub version: u8,

        /// Message ID assigned as part of a message's type definition.
        pub message_id: u8,

        /// Reserved for future use. Must be zero.
        pub reserved0: [u8; 5],

        /// Used for sequencing messages along a NodeLink to preserve end-to-end
        /// ordering, as NodeLink messages may be transmitted either across a
        /// driver transport or queued in shared memory.
        pub node_sequence_number: SequenceNumber,

        /// Offset into the message where the unified array of DriverObjectData
        /// lives, or zero if there are no driver objects attached.
        pub driver_object_data_array: u32,

        /// Reserved for future use. Must be zero.
        pub reserved1: u32,
    }

    const _: () = assert!(size_of::<MessageHeader>() == 24, "Unexpected size");
    pub type MessageHeaderV0 = MessageHeader;
    pub type LatestMessageHeaderVersion = MessageHeaderV0;

    /// Header encoding metadata about a structure within a message.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    #[repr(C, packed)]
    pub struct StructHeader {
        /// The size of the structure in bytes. Used for versioning.
        pub size: u32,

        /// Unused. Must be zero.
        pub padding: u32,
    }

    const _: () = assert!(size_of::<StructHeader>() == 8, "Unexpected size");

    /// Header encoding metadata about any array within a message.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    #[repr(C, packed)]
    pub struct ArrayHeader {
        /// The total number of bytes occupied by the array, including this
        /// header and any padding for 8-byte alignment.
        pub num_bytes: u32,

        /// The number of elements encoded for the array. Elements are packed
        /// into the message immediately following this header.
        pub num_elements: u32,
    }

    /// Each serialized driver object is represented as an array of bytes and an
    /// array of driver handles. This structure describes both arrays for a
    /// single driver object. For every object attached to a message, there is
    /// one of these structs.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    #[repr(C, packed)]
    pub struct DriverObjectData {
        /// Byte index into the enclosing message where the ArrayHeader of the
        /// byte array which contains serialized data for this driver object can
        /// be found.
        pub driver_data_array: u32,

        /// Every message carries a single unified array of attached driver
        /// handles. This is the index of the first driver handle relevant to a
        /// specific driver object attachment.
        pub first_driver_handle: u16,

        /// The number of driver handles belonging to this driver object,
        /// starting at the index of `first_driver_handle` within the message's
        /// driver handle array.
        pub num_driver_handles: u16,
    }

    /// Encodes information about a range of driver objects. Used to encode
    /// DriverObject array parameters.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    #[repr(C, packed)]
    pub struct DriverObjectArrayData {
        /// Index into the message's unified DriverObject array which
        /// corresponds to the first DriverObject belonging to the array
        /// described by this structure.
        pub first_object_index: u32,

        /// The length of this DriverObject array.
        pub num_objects: u32,
    }

    unsafe impl Pod for MessageHeader {}
    unsafe impl Pod for StructHeader {}
    unsafe impl Pod for ArrayHeader {}
    unsafe impl Pod for DriverObjectData {}
    unsafe impl Pod for DriverObjectArrayData {}
    unsafe impl Pod for SequenceNumber {}

    /// Encodes an invalid driver object index. Any driver object field encoded
    /// as this value will deserialize to an invalid DriverObject.
    pub const INVALID_DRIVER_OBJECT_INDEX: u32 = 0xffff_ffff;

    /// The generic type of a message parameter.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ParamType {
        /// A parameter encoded inline within the message's primary parameter
        /// struct.
        Data,

        /// An inline parameter with min value 0 and a max value that is
        /// validated.
        Enum,

        /// A parameter encoded as a 32-bit index elsewhere in the message. This
        /// index points to encoded array contents, beginning with an
        /// ArrayHeader.
        DataArray,

        /// A parameter encoded as a single 32-bit value indexing the message's
        /// unified DriverObject array.
        DriverObject,

        /// A parameter encoded as a single DriverObjectArrayData structure,
        /// referring to a span of contiguous DriverObjects within the message's
        /// unified DriverObject array.
        DriverObjectArray,
    }

    /// Metadata about a single parameter declared within a message. Constants
    /// of this type are generated by [`define_messages!`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct ParamMetadata {
        /// The offset of this parameter from the start of its version's field
        /// block.
        pub offset: usize,

        /// The size of the data expected at `offset` in order for the field to
        /// be deserializable.
        pub size: usize,

        /// If this is an array-typed field, this is the encoded size of each
        /// array element expected.
        pub array_element_size: usize,

        /// Max value of an enum-typed field (whose min value is zero).
        pub enum_max_value: u32,

        /// The generic type of this parameter.
        pub param_type: ParamType,
    }

    /// Metadata about a single message version. Note that versions are
    /// additive: "version 0" refers only to the fields defined for version 0 of
    /// a message; "version 1" refers only to the fields added for version 1.
    #[derive(Clone, Copy, Debug)]
    pub struct VersionMetadata {
        /// The version number for the described version.
        pub version_number: i32,

        /// The offset of this version's parameter data from the start of the
        /// parameters structure, including the StructHeader.
        pub offset: usize,

        /// The size of the parameter data which comprises this version.
        pub size: usize,

        /// Metadata about all the parameters in the parameter data.
        pub params: &'static [ParamMetadata],
    }
}

use internal::*;

/// Byte storage with a guaranteed 8-byte-aligned base address.
#[derive(Clone, Debug, Default)]
struct AlignedBytes {
    words: Vec<u64>,
    len: usize,
}

impl AlignedBytes {
    fn zeroed(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(8)],
            len,
        }
    }

    fn from_slice(bytes: &[u8]) -> Self {
        let mut aligned = Self::zeroed(bytes.len());
        aligned.as_mut_slice().copy_from_slice(bytes);
        aligned
    }

    /// Grows to `new_len` bytes; new bytes are zero.
    fn grow(&mut self, new_len: usize) {
        assert!(new_len >= self.len);
        self.words.resize(new_len.div_ceil(8), 0);
        self.len = new_len;
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `words` owns at least `len` initialized bytes.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as above, with unique access.
        unsafe { std::slice::from_raw_parts_mut(self.words.as_mut_ptr().cast(), self.len) }
    }
}

fn align(x: usize) -> usize {
    (x + 7) & !7
}

fn is_aligned(n: usize) -> bool {
    n.is_multiple_of(8)
}

/// Returns `true` if and only if it will be safe to use `get_array_view()` to
/// access the contents of a serialized array beginning at `array_offset` bytes
/// from the start of `message`, where each element is `element_size` bytes wide.
fn is_array_valid(message: &Message, array_offset: u32, element_size: usize) -> bool {
    if array_offset == 0 {
        return true;
    }

    let data = message.data_view();
    let array_offset = array_offset as usize;
    if array_offset >= data.len() || !is_aligned(array_offset) {
        return false;
    }

    let bytes_available = data.len() - array_offset;
    if bytes_available < size_of::<ArrayHeader>() {
        return false;
    }

    let header: ArrayHeader = message.read_at(array_offset);
    let (num_bytes, num_elements) = (header.num_bytes as usize, header.num_elements as usize);
    if bytes_available < num_bytes || num_bytes < size_of::<ArrayHeader>() {
        return false;
    }

    let max_num_elements = (num_bytes - size_of::<ArrayHeader>()) / element_size;
    num_elements <= max_num_elements
}

/// Transforms a driver object attached to `message` into its serialized form
/// within the message by running it through the driver's serializer.
///
/// Metadata is placed into a DriverObjectData returned in `data`. Serialized
/// data bytes are stored in an array appended to `message` and referenced by
/// the DriverObjectData, and any transmissible handles emitted by the driver are
/// appended to `transmissible_handles`, with relevant index and count also
/// stashed in the DriverObjectData.
fn serialize_driver_object(
    mut object: DriverObject,
    transport: &DriverTransport,
    message: &mut Message,
    data: &mut DriverObjectData,
    transmissible_handles: &mut Vec<DriverHandle>,
) -> bool {
    if !object.is_valid() {
        // This is not a valid driver handle and it cannot be serialized.
        data.num_driver_handles = 0;
        return false;
    }

    let dimensions = object.get_serialized_dimensions(transport);
    let driver_data_array = if dimensions.num_bytes > 0 {
        message.allocate_array::<u8>(dimensions.num_bytes)
    } else {
        0
    };

    let first_handle = transmissible_handles.len();
    data.driver_data_array = driver_data_array;
    data.num_driver_handles = checked_cast(dimensions.num_driver_handles);
    data.first_driver_handle = checked_cast(first_handle);

    transmissible_handles.resize(first_handle + dimensions.num_driver_handles, DriverHandle::default());
    let driver_data = message.get_array_view_mut::<u8>(driver_data_array as usize);
    object.serialize(
        transport,
        driver_data,
        &mut transmissible_handles[first_handle..],
    )
}

/// Deserializes a driver object encoded within `message`, returning the object
/// on success and marking its constituent handles as consumed. On failure, an
/// invalid DriverObject is returned.
fn deserialize_driver_object(
    message: &Message,
    object_data: &DriverObjectData,
    handles: &[DriverHandle],
    is_handle_consumed: &mut [bool],
    transport: &DriverTransport,
) -> DriverObject {
    let (driver_data_array, first_handle, num_handles) = (
        object_data.driver_data_array,
        object_data.first_driver_handle as usize,
        object_data.num_driver_handles as usize,
    );
    if !is_array_valid(message, driver_data_array, size_of::<u8>()) {
        return DriverObject::default();
    }

    let driver_data = message.get_array_view::<u8>(driver_data_array as usize);
    if num_handles > handles.len() || handles.len() - num_handles < first_handle {
        return DriverObject::default();
    }

    // If any driver handles have already been claimed, the message is invalid.
    if is_handle_consumed[first_handle..first_handle + num_handles]
        .iter()
        .any(|&consumed| consumed)
    {
        return DriverObject::default();
    }

    // These two steps cannot be merged: returning early if some handles are
    // already consumed can result in a handle marked as consumed even though it
    // was never passed to `DriverObject::deserialize()`. The cleanup logic in
    // `deserialize_unknown_type()` would also skip the already-marked handle,
    // leaking the object's resources.
    is_handle_consumed[first_handle..first_handle + num_handles].fill(true);
    DriverObject::deserialize(
        transport,
        driver_data,
        &handles[first_handle..first_handle + num_handles],
    )
}

/// Helps build, serialize, and deserialize ipcz-internal messages.
#[derive(Debug, Default)]
pub struct Message {
    /// The raw serialized data for this message. It always begins with a
    /// MessageHeader (or potentially some newer or older version thereof), whose
    /// actual size is determined by the header's `size` field. After that many
    /// bytes, a parameters structure immediately follows, as generated by
    /// [`define_messages!`]. After fixed parameters, any number of dynamically
    /// inlined allocations may follow (e.g. for array contents, driver objects,
    /// etc.)
    data: AlignedBytes,

    /// Collection of DriverObjects attached to this message. These are attached
    /// while building a message (e.g. by calling `append_driver_object`), and
    /// they are consumed by `serialize()` to encode the objects for
    /// transmission. Serialized objects are represented by some combination of
    /// data within the message, and zero or more transmissible driver handles
    /// which accumulate in `transmissible_driver_handles` during serialization.
    ///
    /// On deserialization, driver object data and transmissible handles are fed
    /// back to the driver and used to reconstruct this list. Deserialized
    /// objects may be extracted from the message by calling
    /// `take_driver_object()`.
    driver_objects: Vec<DriverObject>,

    /// Driver handles which the driver knows how to transmit as-is, in
    /// conjunction with (but out-of-band from) the payload in `data`. This is
    /// populated by `serialize()` if the driver emits any transmissible handles
    /// as outputs when serializing any of the objects in `driver_objects`.
    transmissible_driver_handles: Vec<DriverHandle>,

    envelope: DriverObject,
}

impl Message {
    /// An empty message, to be populated by one of the deserialization methods.
    pub fn new_incoming() -> Self {
        Self::default()
    }

    /// A new outgoing message with a header for `message_id` followed by zeroed
    /// storage for `params_size` bytes of parameters.
    pub fn new(message_id: u8, params_size: usize) -> Self {
        let mut message = Message {
            data: AlignedBytes::zeroed(size_of::<MessageHeader>() + params_size),
            ..Message::default()
        };
        let header = message.header_mut();
        header.size = size_of::<MessageHeader>() as u8;
        header.version = 0;
        header.message_id = message_id;
        header.driver_object_data_array = 0;
        assert!(is_aligned(message.data.len));
        message
    }

    pub fn header(&self) -> MessageHeader {
        self.read_at(0)
    }

    pub fn header_mut(&mut self) -> &mut MessageHeader {
        assert!(self.data.len >= size_of::<MessageHeader>());
        // SAFETY: bounds checked above; `MessageHeader` has alignment 1.
        unsafe { &mut *self.data.as_mut_slice().as_mut_ptr().cast() }
    }

    pub fn data_view(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn data_view_mut(&mut self) -> &mut [u8] {
        self.data.as_mut_slice()
    }

    pub fn params_data_view(&self) -> &[u8] {
        &self.data.as_slice()[self.header().size as usize..]
    }

    pub fn driver_objects(&mut self) -> &mut [DriverObject] {
        &mut self.driver_objects
    }

    pub fn transmissible_driver_handles(&self) -> &[DriverHandle] {
        &self.transmissible_driver_handles
    }

    /// Reads a `T` at `offset` bytes from the start of the message. The offset
    /// must be validated ahead of time.
    pub fn read_at<T: Pod>(&self, offset: usize) -> T {
        assert!(check_add(offset, size_of::<T>()) <= self.data.len);
        // SAFETY: bounds checked; `T: Pod` accepts any bytes; unaligned read.
        unsafe { ptr::read_unaligned(self.data.as_slice().as_ptr().add(offset).cast()) }
    }

    /// Writes a `T` at `offset` bytes from the start of the message.
    pub fn write_at<T: Pod>(&mut self, offset: usize, value: T) {
        assert!(check_add(offset, size_of::<T>()) <= self.data.len);
        // SAFETY: bounds checked; unaligned write.
        unsafe { ptr::write_unaligned(self.data.as_mut_slice().as_mut_ptr().add(offset).cast(), value) }
    }

    /// Reads a `T` at `param_offset` bytes from the start of the message's
    /// parameter data. Note the distinction between this and `read_at()`: this
    /// offset is taken from the end of the message header.
    pub fn get_param_value_at<T: Pod>(&self, param_offset: usize) -> T {
        assert!(check_add(param_offset, size_of::<T>()) <= self.params_data_view().len());
        self.read_at(self.header().size as usize + param_offset)
    }

    /// Allocates additional storage in this message to hold an array of
    /// `num_elements`, each with a size of `element_size` bytes. The allocated
    /// storage includes space for an ArrayHeader and padding to an 8-byte
    /// boundary. Returns the offset into the message payload where the
    /// ArrayHeader begins. Storage for each element follows contiguously.
    pub fn allocate_generic_array(&mut self, element_size: usize, num_elements: usize) -> u32 {
        if num_elements == 0 {
            return 0;
        }

        let offset = align(self.data.len);
        let num_bytes = align(check_add(
            size_of::<ArrayHeader>(),
            check_mul(element_size, num_elements),
        ));
        self.data.grow(check_add(offset, num_bytes));
        self.write_at(
            offset,
            ArrayHeader {
                num_bytes: checked_cast(num_bytes),
                num_elements: checked_cast(num_elements),
            },
        );
        checked_cast(offset)
    }

    /// Simple helper for `allocate_generic_array`, using the size of the type
    /// argument as the element size.
    pub fn allocate_array<T>(&mut self, num_elements: usize) -> u32 {
        self.allocate_generic_array(size_of::<T>(), num_elements)
    }

    /// Allocates an array and populates its elements in-place, returning the
    /// array offset to use as a field value.
    pub fn allocate_and_set_array<T: Pod>(&mut self, elements: &[T]) -> u32 {
        let offset = self.allocate_array::<T>(elements.len());
        self.get_array_view_mut::<T>(offset as usize).copy_from_slice(elements);
        offset
    }

    fn array_bounds<T: Pod>(&self, offset: usize) -> usize {
        // The offset must have been validated ahead of time. The ArrayHeader
        // itself must also have been validated already to ensure that the span
        // of array contents will not exceed the bounds of the data.
        assert!(check_add(offset, size_of::<ArrayHeader>()) <= self.data.len);
        assert!(is_aligned(offset) || size_of::<T>() == 1);
        let header: ArrayHeader = self.read_at(offset);
        let (num_bytes, num_elements) = (header.num_bytes as usize, header.num_elements as usize);
        assert!(
            check_add(
                check_add(offset, size_of::<ArrayHeader>()),
                check_mul(size_of::<T>(), num_elements)
            ) <= self.data.len
        );
        assert!(check_mul(size_of::<T>(), num_elements) <= num_bytes);
        num_elements
    }

    /// Returns a view into a serialized array's contents, given an array whose
    /// header begins at `offset` bytes from the beginning of this message. If
    /// `offset` is zero, this returns an empty slice.
    ///
    /// Note that this always returns a slice of `num_elements`. If the array was
    /// allocated with `allocate_array::<u32>(8)`, `get_array_view::<u8>()` will
    /// return a slice of size 8, not 32.
    pub fn get_array_view<T: Pod>(&self, offset: usize) -> &[T] {
        if offset == 0 {
            return &[];
        }
        let num_elements = self.array_bounds::<T>(offset);
        let start = offset + size_of::<ArrayHeader>();
        assert!(start.is_multiple_of(std::mem::align_of::<T>()));
        // SAFETY: bounds and alignment checked (the base is 8-aligned); `T` is
        // `Pod`.
        unsafe { std::slice::from_raw_parts(self.data.as_slice().as_ptr().add(start).cast(), num_elements) }
    }

    /// Mutable variant of [`Self::get_array_view`].
    pub fn get_array_view_mut<T: Pod>(&mut self, offset: usize) -> &mut [T] {
        if offset == 0 {
            return &mut [];
        }
        let num_elements = self.array_bounds::<T>(offset);
        let start = offset + size_of::<ArrayHeader>();
        assert!(start.is_multiple_of(std::mem::align_of::<T>()));
        // SAFETY: as above, with unique access.
        unsafe {
            std::slice::from_raw_parts_mut(self.data.as_mut_slice().as_mut_ptr().add(start).cast(), num_elements)
        }
    }

    /// Appends a single driver object to this message, and returns its index
    /// into the message's DriverObject array. This index should be stored as the
    /// value for whatever driver object parameter corresponds to the appended
    /// object.
    ///
    /// Note that this does NOT serialize `object` yet. Serialization of all
    /// attached objects occurs during `serialize()`.
    pub fn append_driver_object(&mut self, object: DriverObject) -> u32 {
        if !object.is_valid() {
            return INVALID_DRIVER_OBJECT_INDEX;
        }

        let index = checked_cast(self.driver_objects.len());
        self.driver_objects.push(object);
        index
    }

    /// Appends all driver objects in `objects` to this message and returns a
    /// DriverObjectArrayData describing the starting index and length of a span
    /// within the message's DriverObject array. Taken objects are left invalid.
    pub fn append_driver_objects(&mut self, objects: &mut [DriverObject]) -> DriverObjectArrayData {
        let data = DriverObjectArrayData {
            first_object_index: checked_cast(self.driver_objects.len()),
            num_objects: checked_cast(objects.len()),
        };
        self.driver_objects.reserve(objects.len());
        for object in objects {
            assert!(object.is_valid());
            self.driver_objects.push(std::mem::take(object));
        }
        data
    }

    /// Takes ownership of a DriverObject that was attached to this message,
    /// given an index into the message's unified DriverObject array. This should
    /// be the same index returned by a prior call to `append_driver_object()`
    /// when serializing the original message.
    pub fn take_driver_object(&mut self, index: u32) -> DriverObject {
        if index == INVALID_DRIVER_OBJECT_INDEX {
            return DriverObject::default();
        }

        // Note that `index` has already been validated by now.
        std::mem::take(&mut self.driver_objects[index as usize])
    }

    /// Returns the DriverObjects corresponding to the span described by `data`.
    pub fn get_driver_object_array_view(&mut self, data: &DriverObjectArrayData) -> &mut [DriverObject] {
        let (first, num) = (data.first_object_index as usize, data.num_objects as usize);
        &mut self.driver_objects[first..first + num]
    }

    /// Checks and indicates whether this message can be transmitted over
    /// `transport`, which depends on whether the driver is able to transmit all
    /// of the attached driver objects over that transport.
    pub fn can_transmit_on(&self, transport: &DriverTransport) -> bool {
        self.driver_objects.iter().all(|object| object.can_transmit_on(transport))
    }

    /// Attempts to finalize a message for transmission over `transport`. If the
    /// message has no DriverObjects attached, this is trivially a no-op.
    ///
    /// Otherwise, any attached DriverObjects are serialized into inlined data
    /// and/or transmissible handles at this time. This may result in
    /// re-allocation of the Message's underlying storage.
    ///
    /// In any case, upon return the resulting Message can be transmitted to
    /// another node and be deserialized from there. If the message has at least
    /// one element in `transmissible_driver_handles()`, it must be transmitted
    /// over `transport`.
    ///
    /// NOTE: It is invalid to call this on a message for which
    /// `can_transmit_on(transport)` does not return true.
    pub fn serialize(&mut self, transport: &DriverTransport) -> bool {
        assert!(self.can_transmit_on(transport));
        if self.driver_objects.is_empty() {
            return true;
        }

        let num_objects = self.driver_objects.len();
        let array_offset = self.allocate_array::<DriverObjectData>(num_objects);
        self.header_mut().driver_object_data_array = array_offset;

        let mut transmissible_handles = Vec::new();
        let mut ok = true;
        for i in 0..num_objects {
            let object = std::mem::take(&mut self.driver_objects[i]);
            let mut data = DriverObjectData::default();
            ok &= serialize_driver_object(object, transport, self, &mut data, &mut transmissible_handles);
            self.get_array_view_mut::<DriverObjectData>(array_offset as usize)[i] = data;
        }

        if ok {
            self.transmissible_driver_handles = transmissible_handles;
        }
        ok
    }

    /// Validates and deserializes a Message of an unrecognized type.
    /// DriverObjects are deserialized and much of the message structure is
    /// validated, but the parameter layout is not known and is therefore neither
    /// validated nor exposed.
    pub fn deserialize_unknown_type(&mut self, message: &RawMessage<'_>, transport: &DriverTransport) -> bool {
        // Ensure that upon return we explicitly close any handles that weren't
        // consumed by some object deserialization below.
        let mut is_handle_consumed = vec![false; message.handles.len()];
        let ok = self.deserialize_unknown_type_inner(message, transport, &mut is_handle_consumed);

        if let Some(driver) = transport.driver() {
            message
                .handles
                .iter()
                .zip(&is_handle_consumed)
                .filter(|(_, consumed)| !**consumed)
                .for_each(|(&handle, _)| {
                    driver.close(handle);
                });
        }
        ok
    }

    fn deserialize_unknown_type_inner(
        &mut self,
        message: &RawMessage<'_>,
        transport: &DriverTransport,
        is_handle_consumed: &mut [bool],
    ) -> bool {
        if !self.copy_data_and_validate_header(message.data) {
            return false;
        }

        // Validate and deserialize the DriverObject array.
        let driver_object_array_offset = self.header().driver_object_data_array;
        let mut all_driver_objects_ok = true;
        if driver_object_array_offset > 0 {
            if !is_array_valid(self, driver_object_array_offset, size_of::<DriverObjectData>()) {
                // The header specified an invalid DriverObjectData array offset,
                // or the array itself was invalid or out-of-bounds.
                return false;
            }

            let driver_object_data =
                self.get_array_view::<DriverObjectData>(driver_object_array_offset as usize).to_vec();
            let mut objects = Vec::with_capacity(driver_object_data.len());
            for object_data in &driver_object_data {
                let object =
                    deserialize_driver_object(self, object_data, message.handles, is_handle_consumed, transport);
                if object.is_valid() {
                    objects.push(object);
                } else {
                    // We don't fail immediately so we can try to deserialize the
                    // remaining objects anyway, since doing so may free
                    // additional resources.
                    all_driver_objects_ok = false;
                }
            }
            self.driver_objects.extend(objects);
        }
        all_driver_objects_ok
    }

    /// For a Message whose contents were received from another node, this takes
    /// ownership of the heap-allocated copy of those contents.
    pub fn take_received_data(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.data).as_slice().to_vec()
    }

    pub fn set_envelope(&mut self, envelope: DriverObject) {
        self.envelope = envelope;
    }

    pub fn take_envelope(&mut self) -> DriverObject {
        std::mem::take(&mut self.envelope)
    }

    // Common helper for validation of an incoming message header and basic data
    // payload size.
    fn copy_data_and_validate_header(&mut self, data: &[u8]) -> bool {
        // Copy the data into the message to avoid any TOCTOU issues in case
        // `data` is in unsafe shared memory.
        self.data = AlignedBytes::from_slice(data);

        // The message must at least be large enough to encode a v0
        // MessageHeader.
        if self.data.len < size_of::<MessageHeaderV0>() {
            return false;
        }

        // Version 0 header must match MessageHeaderV0 size exactly. Newer
        // unknown versions must not be smaller than that.
        let header = self.header();
        if header.version == 0 {
            if header.size as usize != size_of::<MessageHeaderV0>() {
                return false;
            }
        } else if (header.size as usize) < size_of::<MessageHeaderV0>() {
            return false;
        }

        // The header's stated size (and thus the start of the parameter payload)
        // must not run over the edge of the message and must be
        // 8-byte-aligned.
        !(header.size as usize > self.data.len || !is_aligned(header.size as usize))
    }

    // Common helper to validate an encoded parameter structure against a
    // specific message definition. Must only be called on a Message with `data`
    // already populated, the header already validated, and DriverObjects already
    // deserialized.
    fn validate_parameters(&self, versions: &[VersionMetadata]) -> bool {
        // Validate parameter data. There must be at least enough bytes following
        // the header to encode a StructHeader and to account for all parameter
        // data for some known version of the message.
        let minimum_size = self.header().size as usize + size_of::<StructHeader>();
        if self.data.len < minimum_size {
            return false;
        }

        let params_data = self.params_data_view();
        if params_data.len() < size_of::<StructHeader>() {
            return false;
        }

        let params_header: StructHeader = self.get_param_value_at(0);
        let params_size = params_header.size as usize;

        // The param struct's header claims to consist of more data than is
        // present in the message. Not good.
        if params_data.len() < params_size {
            return false;
        }

        // Parameter struct sizes must be 8-byte-aligned.
        if !is_aligned(params_size) {
            return false;
        }

        // Validate each parameter and claim driver objects. We track the index
        // of every object claimed by a parameter to ensure that no object is
        // claimed more than once.
        //
        // It is not an error for some objects to go unclaimed, as they may have
        // been provided for fields from a newer version of the message that
        // isn't known to this recipient.
        //
        // NOTE: All VersionMetadata and ParamMetadata structures are generated
        // constants used to reflect message layouts. They are not received over
        // the wire and do not require validation themselves.
        let mut is_object_claimed = vec![false; self.driver_objects.len()];
        for (index, version) in versions.iter().enumerate() {
            if version.offset >= params_size || version.offset + version.size > params_size {
                // It's not an error to fall short of any version above 0.
                // Higher-versioned fields are inaccessible to message consumers
                // in this case.
                return index != 0;
            }

            for param in version.params {
                let offset = version.offset + param.offset;
                if param.array_element_size > 0 {
                    let array_offset: u32 = self.get_param_value_at(offset);
                    if !is_array_valid(self, array_offset, param.array_element_size) {
                        return false;
                    }
                }

                match param.param_type {
                    ParamType::Enum => {
                        // Only support u8 and u32 enums at present.
                        let value: u32 = if param.size == 1 {
                            u32::from(self.get_param_value_at::<u8>(offset))
                        } else {
                            self.get_param_value_at::<u32>(offset)
                        };
                        if value > param.enum_max_value {
                            return false;
                        }
                    }
                    ParamType::DriverObject => {
                        let index: u32 = self.get_param_value_at(offset);
                        if index != INVALID_DRIVER_OBJECT_INDEX {
                            let index = index as usize;
                            if index >= is_object_claimed.len() || is_object_claimed[index] {
                                return false;
                            }
                            is_object_claimed[index] = true;
                        }
                    }
                    ParamType::DriverObjectArray => {
                        let array_data: DriverObjectArrayData = self.get_param_value_at(offset);
                        let begin = array_data.first_object_index as usize;
                        if begin > is_object_claimed.len() {
                            return false;
                        }

                        let max_num_objects = is_object_claimed.len() - begin;
                        let num_objects = array_data.num_objects as usize;
                        if num_objects > max_num_objects {
                            return false;
                        }

                        let claimed = &mut is_object_claimed[begin..begin + num_objects];
                        if claimed.iter().any(|&c| c) {
                            return false;
                        }
                        claimed.fill(true);
                    }
                    ParamType::Data | ParamType::DataArray => {}
                }
            }
        }
        true
    }

    /// Attempts to deserialize a message from raw `message` data and handles
    /// into `self`, given the `versions` metadata generated for a specific
    /// ipcz-internal message. `transport` is the transport from which the
    /// incoming data and handles were received.
    pub fn deserialize_from_transport(
        &mut self,
        versions: &[VersionMetadata],
        message: &RawMessage<'_>,
        transport: &DriverTransport,
    ) -> bool {
        self.deserialize_unknown_type(message, transport) && self.validate_parameters(versions)
    }

    /// Attempts to deserialize a message from raw `data`, given a set of already
    /// deserialized DriverObjects in `objects`. The objects and data here have
    /// been extracted from a message relayed opaquely through the broker. While
    /// each DriverObject has already been validated and deserialized, the
    /// message-specific parameter data and object-field assignments must be
    /// validated here. Objects are moved out of `objects`.
    pub fn deserialize_from_relay(
        &mut self,
        versions: &[VersionMetadata],
        data: &[u8],
        objects: &mut [DriverObject],
    ) -> bool {
        if !self.copy_data_and_validate_header(data) {
            return false;
        }

        self.driver_objects = objects.iter_mut().map(std::mem::take).collect();
        self.validate_parameters(versions)
    }
}

/// Static description of a macro-generated message type, for diagnostics and
/// layout checks against the C++ implementation.
#[derive(Clone, Copy, Debug)]
pub struct MessageInfo {
    pub name: &'static str,
    pub id: u8,
    pub params_size: usize,
    pub versions: &'static [VersionMetadata],
    /// The serialized bytes of a freshly constructed outgoing message.
    pub default_bytes: fn() -> Vec<u8>,
}

/// Renders the layout of `infos` in the same text format printed by the C++
/// program `tests/layout/node_messages_layout.cc`.
pub fn layout_dump(infos: &[MessageInfo]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for info in infos {
        writeln!(out, "message {} id={} params_size={}", info.name, info.id, info.params_size).unwrap();
        for version in info.versions {
            writeln!(
                out,
                "  version {} offset={} size={}",
                version.version_number, version.offset, version.size
            )
            .unwrap();
            for param in version.params {
                writeln!(
                    out,
                    "    param offset={} size={} array_element_size={} enum_max={} type={}",
                    param.offset,
                    param.size,
                    param.array_element_size,
                    param.enum_max_value,
                    param.param_type as i32
                )
                .unwrap();
            }
        }
        let hex = (info.default_bytes)().iter().map(|b| format!("{b:02x}")).collect::<String>();
        writeln!(out, "  default {hex}").unwrap();
    }
    out
}

/// The parameter structure of a macro-generated message type.
pub trait MessageParams: Copy + Default + 'static {
    const ID: u8;
    const VERSIONS: &'static [VersionMetadata];
}

/// Wraps the Message type for a specific macro-generated parameter structure.
/// This primarily exists for safe, convenient construction of message payloads
/// with correct header information and no leaky padding bits, as well as for
/// convenient access to parameters within size-validated, deserialized
/// messages.
#[derive(Debug)]
pub struct MessageWithParams<P: MessageParams> {
    message: Message,
    _params: std::marker::PhantomData<P>,
}

impl<P: MessageParams> Default for MessageWithParams<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: MessageParams> MessageWithParams<P> {
    /// Constructs an outgoing message with zeroed (or per-field default)
    /// parameters.
    pub fn new() -> Self {
        let mut message = Message::new(P::ID, size_of::<P>());
        let offset = message.header().size as usize;
        let mut params = P::default();
        // SAFETY: every generated parameter struct is `repr(C, packed)` and
        // begins with a `StructHeader`.
        let header = unsafe { &mut *(ptr::addr_of_mut!(params).cast::<StructHeader>()) };
        header.size = size_of::<P>() as u32;
        header.padding = 0;
        // SAFETY: `Message::new` reserved `size_of::<P>()` bytes at `offset`.
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(params).cast::<u8>(),
                message.data.as_mut_slice().as_mut_ptr().add(offset),
                size_of::<P>(),
            );
        }
        Self {
            message,
            _params: std::marker::PhantomData,
        }
    }

    /// An empty message, to be filled by `deserialize()`.
    pub fn new_incoming() -> Self {
        Self {
            message: Message::new_incoming(),
            _params: std::marker::PhantomData,
        }
    }

    pub fn into_message(self) -> Message {
        self.message
    }

    /// The header of the parameters structure, whose location depends on the
    /// size of the message header. Note that because this may be used to access
    /// parameters within messages using a newer or older header than what's
    /// defined above, we index based on the header's encoded size rather than
    /// the compile-time size of MessageHeader.
    pub fn params_header(&self) -> StructHeader {
        self.message.read_at(self.message.header().size as usize)
    }

    pub fn params_header_mut(&mut self) -> &mut StructHeader {
        let offset = self.message.header().size as usize;
        assert!(offset + size_of::<StructHeader>() <= self.message.data.len);
        // SAFETY: bounds checked above; `StructHeader` has alignment 1.
        unsafe { &mut *self.message.data.as_mut_slice().as_mut_ptr().add(offset).cast() }
    }

    /// Returns a reference to the version struct `V` located `version_offset`
    /// bytes into the parameters, if the message actually contains it: the
    /// parameters header must claim enough size, and the bytes must lie within
    /// the message.
    pub fn version_ref<V: Pod>(&self, version_offset: usize) -> Option<&V> {
        let start = self.version_start::<V>(version_offset)?;
        // SAFETY: bounds checked by `version_start`; version structs are packed
        // (alignment 1) `Pod` types.
        Some(unsafe { &*self.message.data.as_slice().as_ptr().add(start).cast() })
    }

    /// Mutable variant of [`Self::version_ref`].
    pub fn version_mut<V: Pod>(&mut self, version_offset: usize) -> Option<&mut V> {
        let start = self.version_start::<V>(version_offset)?;
        // SAFETY: as above, with unique access.
        Some(unsafe { &mut *self.message.data.as_mut_slice().as_mut_ptr().add(start).cast() })
    }

    fn version_start<V: Pod>(&self, version_offset: usize) -> Option<usize> {
        debug_assert_eq!(std::mem::align_of::<V>(), 1);
        let params_start = self.message.header().size as usize;
        if self.message.data.len < params_start + size_of::<StructHeader>() {
            return None;
        }
        let required = version_offset + size_of::<V>();
        let start = params_start + version_offset;
        (self.params_header().size as usize >= required && start + size_of::<V>() <= self.message.data.len)
            .then_some(start)
    }

    /// Validates and deserializes an incoming message of this type.
    pub fn deserialize(&mut self, message: &RawMessage<'_>, transport: &DriverTransport) -> bool {
        self.message.deserialize_from_transport(P::VERSIONS, message, transport)
    }

    /// Validates and deserializes a message of this type relayed through a
    /// broker, with already-deserialized `objects`.
    pub fn deserialize_relayed(&mut self, data: &[u8], objects: &mut [DriverObject]) -> bool {
        self.message.deserialize_from_relay(P::VERSIONS, data, objects)
    }
}

impl<P: MessageParams> std::ops::Deref for MessageWithParams<P> {
    type Target = Message;

    fn deref(&self) -> &Message {
        &self.message
    }
}

impl<P: MessageParams> std::ops::DerefMut for MessageWithParams<P> {
    fn deref_mut(&mut self) -> &mut Message {
        &mut self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::{SerializeBuffers, SerializeOutcome, SerializedDimensions};
    use crate::core::ipcz_result::IpczResult;
    use crate::core::testing::mock_driver::MockDriver;
    use crate::core::testing::test_messages::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    const TRANSPORT_HANDLE: usize = 42;

    fn h(value: usize) -> DriverHandle {
        DriverHandle::new(value)
    }

    // A transmitted message captured by the fixture's transport, so that tests
    // can inspect and/or deserialize it later.
    struct ReceivedMessage {
        data: Vec<u8>,
        handles: Vec<DriverHandle>,
    }

    impl ReceivedMessage {
        fn as_raw(&self) -> RawMessage<'_> {
            RawMessage {
                data: &self.data,
                handles: &self.handles,
            }
        }
    }

    struct MessageTest {
        driver: Arc<MockDriver>,
        transport: Arc<DriverTransport>,
        received: Arc<Mutex<VecDeque<ReceivedMessage>>>,
        reject_driver_objects: Arc<AtomicBool>,
    }

    impl MessageTest {
        fn new() -> Self {
            let driver = MockDriver::new();
            let transport = DriverTransport::new(DriverObject::new(driver.as_driver(), h(TRANSPORT_HANDLE)));
            let received = Arc::new(Mutex::new(VecDeque::new()));
            let reject_driver_objects = Arc::new(AtomicBool::new(false));

            // All serialized messages transmitted through `transport` will be
            // captured directly in `received`.
            let sink = received.clone();
            driver.expect_transmit_repeatedly(h(TRANSPORT_HANDLE), move |_, data, handles| {
                sink.lock().unwrap().push_back(ReceivedMessage {
                    data: data.to_vec(),
                    handles: handles.to_vec(),
                });
                IpczResult::Ok
            });

            // For convenient automation when exercising DriverObject
            // transmission, all driver handles in these tests are treated as
            // 32-bit values. Their "serialized" form is the same value
            // decomposed: the high 16-bits are the serialized data bytes, and
            // the low 16-bits are treated as a new transmissible driver handle.
            driver.expect_serialize_any_repeatedly(move |handle, _, buffers| {
                match buffers {
                    Some(SerializeBuffers { data, handles }) if data.len() >= 2 && !handles.is_empty() => {
                        data[..2].copy_from_slice(&((handle.value() >> 16) as u16).to_ne_bytes());
                        handles[0] = h(handle.value() & 0xffff);
                        SerializeOutcome::Serialized
                    }
                    _ => SerializeOutcome::Dimensions(SerializedDimensions {
                        num_bytes: 2,
                        num_driver_handles: 1,
                    }),
                }
            });

            // "Deserialization" reverses the process above: 2 data bytes are
            // expected and 1 transmissible handle is expected, and these are
            // combined into a single new driver handle value to represent the
            // deserialized object.
            let reject = reject_driver_objects.clone();
            driver.expect_deserialize_repeatedly(h(TRANSPORT_HANDLE), move |data, handles, _| {
                if reject.load(Ordering::SeqCst) {
                    return Err(IpczResult::InvalidArgument);
                }

                assert_eq!(data.len(), 2);
                assert_eq!(handles.len(), 1);
                let data_value = u16::from_ne_bytes([data[0], data[1]]);
                Ok(h((usize::from(data_value) << 16) | handles[0].value()))
            });

            Self {
                driver,
                transport,
                received,
                reject_driver_objects,
            }
        }

        fn object(&self, handle: usize) -> DriverObject {
            DriverObject::new(self.driver.as_driver(), h(handle))
        }

        fn set_reject_driver_objects(&self, reject: bool) {
            self.reject_driver_objects.store(reject, Ordering::SeqCst);
        }

        fn received_message_count(&self) -> usize {
            self.received.lock().unwrap().len()
        }

        fn take_next_received_message(&self) -> ReceivedMessage {
            self.received.lock().unwrap().pop_front().expect("a received message")
        }
    }

    impl Drop for MessageTest {
        fn drop(&mut self) {
            self.driver.expect_close(h(TRANSPORT_HANDLE));
        }
    }

    fn deserialize_data<P: MessageParams>(
        m: &mut MessageWithParams<P>,
        data: &[u8],
        transport: &DriverTransport,
    ) -> bool {
        m.deserialize(&RawMessage { data, handles: &[] }, transport)
    }

    #[test]
    fn basic_message() {
        let t = MessageTest::new();
        let mut in_ = BasicTestMessage::new();
        assert!(size_of::<MessageHeaderV0>() >= in_.header().size as usize);
        assert_eq!(in_.header().version, 0);
        assert_eq!(in_.header().message_id, BasicTestMessage::ID);
        assert_eq!({ in_.header().reserved0 }, [0; 5]);
        assert_eq!({ in_.header().node_sequence_number }, SequenceNumber::new(0));
        assert_eq!(in_.header().size % 8, 0);
        assert_eq!({ in_.v0().unwrap().foo }, 0);
        assert_eq!({ in_.v0().unwrap().bar }, 0);
        assert_eq!({ in_.header().reserved1 }, 0);
        in_.v0_mut().unwrap().foo = 5;
        in_.v0_mut().unwrap().bar = 7;

        assert_eq!(t.received_message_count(), 0);
        t.transport.transmit(&mut in_);
        assert_eq!(t.received_message_count(), 1);

        let mut out = BasicTestMessage::new_incoming();
        let serialized = t.take_next_received_message();
        assert!(out.deserialize(&serialized.as_raw(), &t.transport));
        assert_eq!({ out.v0().unwrap().foo }, 5);
        assert_eq!({ out.v0().unwrap().bar }, 7);
    }

    #[test]
    fn enums() {
        let t = MessageTest::new();
        let mut in_ = MessageWithEnums::new();
        assert!(size_of::<MessageHeaderV0>() >= in_.header().size as usize);
        assert_eq!(in_.header().version, 0);
        in_.v0_mut().unwrap().foo = TestEnum8::B.to_raw();
        in_.v0_mut().unwrap().bar = TestEnum32::Two.to_raw();

        assert_eq!(t.received_message_count(), 0);
        t.transport.transmit(&mut in_);
        assert_eq!(t.received_message_count(), 1);

        let mut out = MessageWithEnums::new_incoming();
        let serialized = t.take_next_received_message();
        assert!(out.deserialize(&serialized.as_raw(), &t.transport));
        assert_eq!(TestEnum8::from_raw(out.v0().unwrap().foo), Some(TestEnum8::B));
        assert_eq!(TestEnum32::from_raw(out.v0().unwrap().bar), Some(TestEnum32::Two));
    }

    #[test]
    fn data_array() {
        let t = MessageTest::new();
        let mut in_ = MessageWithDataArray::new();
        let offset = in_.allocate_array::<u64>(3);
        in_.v0_mut().unwrap().values = offset;
        in_.get_array_view_mut::<u64>(offset as usize).copy_from_slice(&[11, 13, 17]);

        t.transport.transmit(&mut in_);

        let mut out = MessageWithDataArray::new_incoming();
        let serialized = t.take_next_received_message();
        assert!(out.deserialize(&serialized.as_raw(), &t.transport));

        let offset = out.v0().unwrap().values;
        assert_eq!(out.get_array_view::<u64>(offset as usize), [11, 13, 17]);
    }

    #[test]
    fn driver_object() {
        const OBJECT_HANDLE: usize = 0x12345678;
        let t = MessageTest::new();
        let mut in_ = MessageWithDriverObject::new();
        let index = in_.append_driver_object(t.object(OBJECT_HANDLE));
        in_.v0_mut().unwrap().object = index;

        t.transport.transmit(&mut in_);

        let mut out = MessageWithDriverObject::new_incoming();
        let serialized = t.take_next_received_message();
        assert!(out.deserialize(&serialized.as_raw(), &t.transport));

        let index = out.v0().unwrap().object;
        let mut object = out.take_driver_object(index);
        assert_eq!(object.release(), h(OBJECT_HANDLE));
    }

    #[test]
    fn driver_object_array() {
        const OBJECT_HANDLES: [usize; 3] = [0x12345678, 0x5a5aa5a5, 0x42425555];
        let t = MessageTest::new();
        let mut in_objects = OBJECT_HANDLES.map(|handle| t.object(handle));

        let mut in_ = MessageWithDriverObjectArray::new();
        let array_data = in_.append_driver_objects(&mut in_objects);
        in_.v0_mut().unwrap().objects = array_data;

        t.transport.transmit(&mut in_);

        let mut out = MessageWithDriverObjectArray::new_incoming();
        let serialized = t.take_next_received_message();
        assert!(out.deserialize(&serialized.as_raw(), &t.transport));

        let array_data = out.v0().unwrap().objects;
        let objects = out.get_driver_object_array_view(&array_data);
        assert_eq!(objects.len(), 3);
        for (object, expected) in objects.iter_mut().zip(OBJECT_HANDLES) {
            assert_eq!(object.release(), h(expected));
        }
    }

    #[test]
    fn short_message() {
        let t = MessageTest::new();
        let mut m = BasicTestMessage::new();
        let data = m.data_view()[..4].to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn short_header() {
        let t = MessageTest::new();
        let mut m = BasicTestMessage::new();
        m.header_mut().size = size_of::<MessageHeaderV0>() as u8 - 1;
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn header_overflow() {
        let t = MessageTest::new();
        let mut m = BasicTestMessage::new();
        m.header_mut().size = 255;
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));

        // An unaligned header size is also rejected.
        let mut m = BasicTestMessage::new();
        m.header_mut().size = 25;
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn short_params_header() {
        let t = MessageTest::new();
        let mut m = BasicTestMessage::new();
        let data = m.data_view()[..size_of::<MessageHeader>() + 1].to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn short_params() {
        let t = MessageTest::new();
        let mut m = BasicTestMessage::new();
        m.params_header_mut().size = 1;
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn params_overflow() {
        let t = MessageTest::new();
        let mut m = BasicTestMessage::new();
        m.params_header_mut().size = 100_000;
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn array_offset_overflow() {
        let t = MessageTest::new();
        let mut m = MessageWithDataArray::new();
        m.v0_mut().unwrap().values = 10_000_000;
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn array_offset_misaligned() {
        let t = MessageTest::new();
        let mut m = MessageWithDataArray::new();
        let offset = m.allocate_array::<u64>(2);
        m.v0_mut().unwrap().values = offset + 1;
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn array_size_overflow() {
        let t = MessageTest::new();
        let mut m = MessageWithDataArray::new();
        let offset = m.allocate_array::<u64>(10);
        m.v0_mut().unwrap().values = offset;

        let mut header: ArrayHeader = m.read_at(offset as usize);
        header.num_bytes = 1_000_000;
        m.write_at(offset as usize, header);
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn array_elements_overflow() {
        let t = MessageTest::new();
        let mut m = MessageWithDataArray::new();
        let offset = m.allocate_array::<u64>(10);
        m.v0_mut().unwrap().values = offset;

        let mut header: ArrayHeader = m.read_at(offset as usize);
        header.num_elements = 1_000_000;
        m.write_at(offset as usize, header);
        let data = m.data_view().to_vec();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    #[test]
    fn malformed_driver_object() {
        const OBJECT_HANDLE: usize = 0x12345678;
        let t = MessageTest::new();
        let mut in_ = MessageWithDriverObject::new();
        let index = in_.append_driver_object(t.object(OBJECT_HANDLE));
        in_.v0_mut().unwrap().object = index;

        // Force driver object deserialization to fail. This must result in
        // failure of overall message deserialization.
        t.set_reject_driver_objects(true);

        t.transport.transmit(&mut in_);

        // As upstream, handles are marked consumed before the driver is asked to
        // deserialize them, so a rejected object doesn't get its handle closed
        // by message cleanup.
        let message = t.take_next_received_message();
        let mut out = MessageWithDriverObject::new_incoming();
        assert!(!out.deserialize(&message.as_raw(), &t.transport));
    }

    #[test]
    fn driver_object_claimed_twice() {
        // Tests that if a single driver object is claimed more than once by a
        // message parameter, the message is rejected.
        const OBJECT_HANDLES: [usize; 3] = [0x12345678, 0x5a5aa5a5, 0x42425555];
        let t = MessageTest::new();
        let mut in_objects = OBJECT_HANDLES.map(|handle| t.object(handle));

        let mut in_ = MessageWithDriverArrayAndExtraObject::new();
        let array_data = in_.append_driver_objects(&mut in_objects);
        in_.v0_mut().unwrap().objects = array_data;

        // Assign the `extra_object` parameter a DriverObject which has already
        // been claimed by the second element of the `objects` parameter.
        in_.v0_mut().unwrap().extra_object = 1;

        t.transport.transmit(&mut in_);

        // Although deserialization will fail, it won't fail until parameter
        // validation, after all DriverObjects are deserialized. So we should
        // expect to see clean closure of the attached DriverObjects.
        OBJECT_HANDLES.iter().for_each(|&handle| t.driver.expect_close(h(handle)));

        let message = t.take_next_received_message();
        let mut out = MessageWithDriverArrayAndExtraObject::new_incoming();
        assert!(!out.deserialize(&message.as_raw(), &t.transport));
    }

    #[test]
    fn unclaimed_driver_objects() {
        // Smoke test to verify that a message with unclaimed DriverObject
        // attachments does not leak.
        let t = MessageTest::new();
        let mut in_ = MessageWithDriverObject::new();
        let index = in_.append_driver_object(t.object(0x12345678));
        in_.v0_mut().unwrap().object = index;

        // Append two more objects with no references to them in the message.
        in_.append_driver_object(t.object(0xabcdef90));
        in_.append_driver_object(t.object(0x5a5a5a5a));

        t.transport.transmit(&mut in_);

        let message = t.take_next_received_message();
        let mut out = MessageWithDriverObject::new_incoming();
        assert!(out.deserialize(&message.as_raw(), &t.transport));

        // Despite not being claimed by any parameters or otherwise referenced
        // within the message, the extra DriverObjects should be deserialized
        // and now owned by the message object.
        let objects = out.driver_objects();
        assert_eq!(objects.len(), 3);
        assert_eq!(objects[0].release(), h(0x12345678));
        assert_eq!(objects[1].release(), h(0xabcdef90));
        assert_eq!(objects[2].release(), h(0x5a5a5a5a));
    }

    #[test]
    fn overlapping_driver_handles() {
        let t = MessageTest::new();
        let mut in_ = Message::new(0, 0);

        // Driver objects are serialized as an array of raw bytes and an array of
        // driver handles. MockDriver uses 32-bit handle values and packs the
        // high 16 bits into the raw bytes and the low 16 bits into the handle
        // value; each DriverObject is expected to be represented with exactly 2
        // raw bytes and 1 handle value.
        let first_object_bytes = in_.allocate_array::<u8>(2);
        in_.get_array_view_mut::<u8>(first_object_bytes as usize)
            .copy_from_slice(&0x90abu16.to_ne_bytes());

        let second_object_bytes = in_.allocate_array::<u8>(2);
        in_.get_array_view_mut::<u8>(second_object_bytes as usize)
            .copy_from_slice(&0x90abu16.to_ne_bytes());

        let object_data_offset = in_.allocate_array::<DriverObjectData>(2);
        in_.header_mut().driver_object_data_array = object_data_offset;

        let handles = [h(0x12345678), h(0xcdef)];

        let object_data = in_.get_array_view_mut::<DriverObjectData>(object_data_offset as usize);
        // The first driver object data entry references `handles[1]`.
        object_data[0] = DriverObjectData {
            driver_data_array: first_object_bytes,
            first_driver_handle: 1,
            num_driver_handles: 1,
        };

        // The second driver object data entry references `handles[0]` and
        // `handles[1]`. This tests that:
        // - rejection of this entry still frees the resources associated with
        //   `handles[0]`.
        // - a second claim on `handles[1]` is rejected.
        //
        // This violates MockDriver's expectations of how serialized driver
        // objects are represented on the wire, but that should not be a problem
        // as the message should be rejected before trying to call the mock
        // driver's deserialization method.
        object_data[1] = DriverObjectData {
            driver_data_array: second_object_bytes,
            first_driver_handle: 0,
            num_driver_handles: 2,
        };

        // This is normally all handled by `DriverTransport::transmit()`, but this
        // test manually executes the steps normally taken by
        // `Message::serialize()` to build an invalid message.
        t.transport.transmit_raw(in_.data_view(), &handles);

        let serialized = t.take_next_received_message();

        let mut out = Message::new_incoming();
        t.driver.expect_close(h(0x12345678));
        assert!(!out.deserialize_unknown_type(&serialized.as_raw(), &t.transport));

        t.driver.expect_close(h(0x90abcdef));
    }

    #[test]
    fn bad_enums() {
        // Out of range enum values should be rejected.
        let t = MessageTest::new();
        let mut m1 = MessageWithEnums::new();
        m1.v0_mut().unwrap().foo = TestEnum8::B.to_raw();
        m1.v0_mut().unwrap().bar = 32;
        let data = m1.data_view().to_vec();
        assert!(!deserialize_data(&mut m1, &data, &t.transport));

        let mut m2 = MessageWithEnums::new();
        m2.v0_mut().unwrap().foo = 99;
        m2.v0_mut().unwrap().bar = TestEnum32::Four.to_raw();
        let data = m2.data_view().to_vec();
        assert!(!deserialize_data(&mut m2, &data, &t.transport));
    }

    #[test]
    fn accept_old_versions() {
        // A message whose parameters are shorter than the latest version is
        // accepted as long as it contains version 0; newer fields are simply
        // inaccessible.
        let t = MessageTest::new();
        let full = MessageWithMultipleVersions::new();
        let header_size = full.header().size as usize;
        let versions = <MessageWithMultipleVersionsParams as MessageParams>::VERSIONS;
        assert_eq!(versions.len(), 3);

        for (num_versions, expect_v1, expect_v2) in [(1, false, false), (2, true, false), (3, true, true)] {
            let params_size = size_of::<StructHeader>()
                + versions[..num_versions].iter().map(|v| v.size).sum::<usize>();
            let mut data = full.data_view()[..header_size + params_size].to_vec();
            // Patch the params header to claim only the versions present.
            data[header_size..header_size + 4].copy_from_slice(&(params_size as u32).to_ne_bytes());

            let mut m = MessageWithMultipleVersions::new_incoming();
            assert!(deserialize_data(&mut m, &data, &t.transport), "{num_versions} versions");
            assert!(m.v0().is_some());
            assert_eq!(m.v1().is_some(), expect_v1);
            assert_eq!(m.v2().is_some(), expect_v2);
        }

        // Anything shorter than version 0 is rejected.
        let mut data = full.data_view()[..header_size + size_of::<StructHeader>()].to_vec();
        data[header_size..header_size + 4].copy_from_slice(&8u32.to_ne_bytes());
        let mut m = MessageWithMultipleVersions::new_incoming();
        assert!(!deserialize_data(&mut m, &data, &t.transport));
    }

    // Golden messages to validate changes to wire protocol and layout.
    #[test]
    fn golden_basic_test_message() {
        let t = MessageTest::new();
        let mut m = BasicTestMessage::new_incoming();

        let empty_basic_message = [
            0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert!(deserialize_data(&mut m, &empty_basic_message, &t.transport));
        assert_eq!({ m.v0().unwrap().foo }, 0);
        assert_eq!({ m.v0().unwrap().bar }, 0);

        let filled_basic_message = [
            0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x66, 0x01, 0x10, 0x44, 0x04, 0x03, 0x02, 0x01,
        ];
        assert!(deserialize_data(&mut m, &filled_basic_message, &t.transport));
        assert_eq!({ m.v0().unwrap().foo }, 0x44100166);
        assert_eq!({ m.v0().unwrap().bar }, 0x01020304);

        // And a freshly built message encodes to the same bytes.
        let mut built = BasicTestMessage::new();
        built.v0_mut().unwrap().foo = 0x44100166;
        built.v0_mut().unwrap().bar = 0x01020304;
        assert_eq!(built.data_view(), filled_basic_message);
    }

    #[test]
    fn golden_message_with_data_array() {
        let t = MessageTest::new();
        let mut m = MessageWithDataArray::new_incoming();

        let empty_array_message = [
            0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert!(deserialize_data(&mut m, &empty_array_message, &t.transport));
        let offset = m.v0().unwrap().values;
        assert_eq!(m.get_array_view::<u64>(offset as usize).len(), 0);

        let array_with_values_message = [
            0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00,
            0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x11, 0x11, 0x00, 0x00, 0x11,
            0x47, 0x46, 0x45, 0x44, 0x14, 0x13, 0x12, 0x11, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee,
            0xee, 0xee,
        ];
        assert!(deserialize_data(&mut m, &array_with_values_message, &t.transport));
        let offset = m.v0().unwrap().values;
        assert_eq!(
            m.get_array_view::<u64>(offset as usize),
            [0x1100001111000011, 0x1112131444454647, 0xeeeeeeeeeeeeeeee]
        );

        // And a freshly built message encodes to the same bytes.
        let mut built = MessageWithDataArray::new();
        let offset = built.allocate_and_set_array(&[0x1100001111000011u64, 0x1112131444454647, 0xeeeeeeeeeeeeeeee]);
        built.v0_mut().unwrap().values = offset;
        assert_eq!(built.data_view(), array_with_values_message);
    }

    #[test]
    fn golden_message_with_multiple_versions() {
        let t = MessageTest::new();
        let mut m = MessageWithMultipleVersions::new_incoming();

        let empty_v1_message = [
            0x18, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x00,
            0x00, 0x11, 0x11, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert!(deserialize_data(&mut m, &empty_v1_message, &t.transport));
        assert_eq!({ m.v1().unwrap().c }, 0x1100001111000011);
        assert!(m.v2().is_none());
    }

    #[test]
    fn golden_message_with_driver_array_and_extra_object() {
        const OBJECT_HANDLES: [usize; 3] = [0x12345678, 0x5a5aa5a5, 0x42425555];
        let t = MessageTest::new();
        let mut m = MessageWithDriverArrayAndExtraObject::new_incoming();

        // Note: handle representation is provided by the fake transport above,
        // and is not related to the OS the test is running on.
        let as_received_data = ReceivedMessage {
            data: vec![
                0x18, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
                0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x60, 0x00, 0x00, 0x00, 0x01, 0x00,
                0x01, 0x00, 0x70, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
                0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x5a, 0x5a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x42, 0x42, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00,
            ],
            handles: vec![h(0x5678), h(0xa5a5), h(0x5555)],
        };

        assert!(m.deserialize(&as_received_data.as_raw(), &t.transport));

        // The objects are closed when the message is destroyed.
        OBJECT_HANDLES.iter().for_each(|&handle| t.driver.expect_close(h(handle)));
    }

    // A listener recording which message types it was handed.
    #[derive(Default)]
    struct TestListener {
        basic: Mutex<Vec<(u32, u32)>>,
        unknown_ids: Mutex<Vec<u8>>,
        envelopes: Mutex<Vec<DriverHandle>>,
        deferred: Mutex<Vec<TestMessage>>,
        defer: AtomicBool,
    }

    impl TestMessageListener for TestListener {
        fn on_message(&self, message: TestMessage) -> bool {
            if let TestMessage::Unknown(m) = &message {
                self.unknown_ids.lock().unwrap().push(m.header().message_id);
            }
            if self.defer.load(Ordering::SeqCst) {
                // Receipt and dispatch are separate, so dispatch can be deferred.
                self.deferred.lock().unwrap().push(message);
                return true;
            }
            self.dispatch_message(message)
        }

        fn on_basic_test_message(&self, mut message: BasicTestMessage) -> bool {
            let v0 = *message.v0().unwrap();
            self.basic.lock().unwrap().push((v0.foo, v0.bar));
            self.envelopes.lock().unwrap().push(message.take_envelope().release());
            true
        }
    }

    fn transmit_and_take<P: MessageParams>(t: &MessageTest, message: &mut MessageWithParams<P>) -> ReceivedMessage {
        assert_eq!(t.transport.transmit(message), IpczResult::Ok);
        t.take_next_received_message()
    }

    #[test]
    fn listener_dispatches_by_message_id() {
        let t = MessageTest::new();
        let listener = TestListener::default();

        let mut basic = BasicTestMessage::new();
        basic.v0_mut().unwrap().foo = 5;
        basic.v0_mut().unwrap().bar = 7;
        let serialized = transmit_and_take(&t, &mut basic);
        assert!(listener.handle_transport_message(&serialized.as_raw(), &t.transport, h(9)));
        assert_eq!(*listener.basic.lock().unwrap(), [(5, 7)]);
        // The driver's envelope handle travels with the message.
        assert_eq!(*listener.envelopes.lock().unwrap(), [h(9)]);
    }

    #[test]
    fn listener_default_handlers_reject_and_unknown_messages_are_ignored() {
        let t = MessageTest::new();
        let listener = TestListener::default();

        // A known message without an override is refused by the default handler.
        let mut array = MessageWithDataArray::new();
        let serialized = transmit_and_take(&t, &mut array);
        assert!(!listener.handle_transport_message(&serialized.as_raw(), &t.transport, h(0)));

        // A message id this interface doesn't define is quietly accepted.
        let mut unknown = Message::new(99, 8);
        unknown.write_at(24, StructHeader { size: 8, padding: 0 });
        assert!(t.transport.transmit(&mut unknown) == IpczResult::Ok);
        let serialized = t.take_next_received_message();
        assert!(listener.handle_transport_message(&serialized.as_raw(), &t.transport, h(0)));
        assert_eq!(*listener.unknown_ids.lock().unwrap(), [99]);

        // Garbage is rejected.
        let short = RawMessage { data: &[1, 2, 3], handles: &[] };
        assert!(!listener.handle_transport_message(&short, &t.transport, h(0)));
        assert_eq!(listener.unknown_ids.lock().unwrap().len(), 1);
    }

    #[test]
    fn listener_can_defer_dispatch() {
        let t = MessageTest::new();
        let listener = TestListener::default();
        listener.defer.store(true, Ordering::SeqCst);

        let mut basic = BasicTestMessage::new();
        basic.v0_mut().unwrap().foo = 1;
        let serialized = transmit_and_take(&t, &mut basic);
        assert!(listener.handle_transport_message(&serialized.as_raw(), &t.transport, h(0)));
        assert!(listener.basic.lock().unwrap().is_empty());

        let message = listener.deferred.lock().unwrap().pop().unwrap();
        assert!(listener.dispatch_message(message));
        assert_eq!(*listener.basic.lock().unwrap(), [(1, 0)]);
    }

    #[test]
    fn relayed_messages_are_validated_with_their_objects() {
        const OBJECT_HANDLE: usize = 0x12345678;
        let t = MessageTest::new();
        let mut in_ = MessageWithDriverObject::new();
        let index = in_.append_driver_object(t.object(OBJECT_HANDLE));
        in_.v0_mut().unwrap().object = index;
        let serialized = transmit_and_take(&t, &mut in_);

        // A broker deserializes the unknown message generically, then hands its
        // data and objects to the final recipient's message type.
        let mut generic = Message::new_incoming();
        assert!(generic.deserialize_unknown_type(&serialized.as_raw(), &t.transport));
        let data = generic.take_received_data();
        assert_eq!(data, serialized.data);

        let mut relayed = MessageWithDriverObject::new_incoming();
        assert!(relayed.deserialize_relayed(&data, generic.driver_objects()));
        let index = relayed.v0().unwrap().object;
        assert_eq!(relayed.take_driver_object(index).release(), h(OBJECT_HANDLE));

        // The objects were moved out of the source slice.
        assert!(generic.driver_objects().iter().all(|object| !object.is_valid()));

        // A relayed message claiming a nonexistent object is rejected.
        let mut bad = MessageWithDriverObject::new_incoming();
        assert!(!bad.deserialize_relayed(&data, &mut []));
    }

    #[test]
    fn serialization_failure() {
        // Verifies that if a serializable object fails to serialize, it's
        // properly destroyed by the transport and no transmission occurs.
        let driver = MockDriver::new();
        const TRANSPORT: usize = 5;
        const FAKE_OBJECT: usize = 12345678;
        let transport = DriverTransport::new(DriverObject::new(driver.as_driver(), h(TRANSPORT)));

        let mut message = MessageWithDriverObject::new();
        let index = message.append_driver_object(DriverObject::new(driver.as_driver(), h(FAKE_OBJECT)));
        message.v0_mut().unwrap().object = index;

        driver.expect_serialize_repeatedly(h(FAKE_OBJECT), |_, _, buffers| match buffers {
            // Return valid outputs when ipcz is sizing the object.
            None => SerializeOutcome::Dimensions(SerializedDimensions {
                num_bytes: 1,
                num_driver_handles: 1,
            }),
            // Return an error when serializing.
            Some(_) => SerializeOutcome::Error(IpczResult::FailedPrecondition),
        });

        // The object handle should be closed upon transmission failure, and
        // no transmission occurs (the mock would panic on one).
        driver.expect_close(h(FAKE_OBJECT));
        assert_eq!(transport.transmit(&mut message), IpczResult::FailedPrecondition);

        driver.expect_close(h(TRANSPORT));
    }
}
