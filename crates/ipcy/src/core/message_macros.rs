//! Message definition DSL, replacing upstream's `IPCZ_MSG_*` macros and the
//! checked-in generator output (`node_messages.{h,cc}`).
//!
//! A definition looks like:
//!
//! ```ignore
//! define_messages! {
//!     interface Node;
//!
//!     message AcceptParcel = 20 {
//!         version 0 {
//!             param sublink: SublinkId;
//!             param_enum link_side: LinkSide;
//!             param_array parcel_data: u8;
//!             driver_object transport: u32;
//!             driver_object_array driver_objects: DriverObjectArrayData;
//!         }
//!         version 1 { ... }
//!     }
//! }
//! ```
//!
//! Parameter kinds mirror `IPCZ_MSG_PARAM`, `_ENUM`, `_ARRAY`, `_DRIVER_OBJECT`
//! and `_DRIVER_OBJECT_ARRAY`. For the last two, the declared type is the wire
//! type (`u32` and `DriverObjectArrayData`).
//!
//! For each message `Foo` this generates:
//! - `FooParams`: the packed wire structure: a `StructHeader` followed by one
//!   `FooV<n>` struct per version, each padded with explicit zero bytes to a
//!   multiple of 8 (as upstream's `IPCZ_ALIGN(8)` does);
//! - `Foo`: a `MessageWithParams<FooParams>` with `v0()`, `v0_mut()`, ...
//!   accessors which return `None` for versions a received message is too short
//!   to contain;
//! - an `<Interface>Message` enum of all messages, and an
//!   `<Interface>MessageListener` trait which receives them.
//!
//! Enum-typed parameters are stored as their raw wire integer; convert with
//! [`WireEnum`](super::message::WireEnum).

macro_rules! wire_ty {
    (param, $t:ty) => { $t };
    (param_enum, $t:ty) => { <$t as $crate::core::message::WireEnum>::Raw };
    (param_array, $t:ty) => { u32 };
    (driver_object, $t:ty) => { u32 };
    (driver_object_array, $t:ty) => { $t };
}

macro_rules! param_meta {
    ($v:ty, param, $f:ident, $t:ty) => {
        $crate::core::message::internal::ParamMetadata {
            offset: ::std::mem::offset_of!($v, $f),
            size: ::std::mem::size_of::<$t>(),
            array_element_size: 0,
            enum_max_value: 0,
            param_type: $crate::core::message::internal::ParamType::Data,
        }
    };
    ($v:ty, param_enum, $f:ident, $t:ty) => {
        $crate::core::message::internal::ParamMetadata {
            offset: ::std::mem::offset_of!($v, $f),
            size: ::std::mem::size_of::<<$t as $crate::core::message::WireEnum>::Raw>(),
            array_element_size: 0,
            enum_max_value: <$t as $crate::core::message::WireEnum>::MAX_VALUE,
            param_type: $crate::core::message::internal::ParamType::Enum,
        }
    };
    ($v:ty, param_array, $f:ident, $t:ty) => {
        $crate::core::message::internal::ParamMetadata {
            offset: ::std::mem::offset_of!($v, $f),
            size: ::std::mem::size_of::<u32>(),
            array_element_size: ::std::mem::size_of::<$t>(),
            enum_max_value: 0,
            param_type: $crate::core::message::internal::ParamType::DataArray,
        }
    };
    ($v:ty, driver_object, $f:ident, $t:ty) => {
        $crate::core::message::internal::ParamMetadata {
            offset: ::std::mem::offset_of!($v, $f),
            size: ::std::mem::size_of::<$t>(),
            array_element_size: 0,
            enum_max_value: 0,
            param_type: $crate::core::message::internal::ParamType::DriverObject,
        }
    };
    ($v:ty, driver_object_array, $f:ident, $t:ty) => {
        $crate::core::message::internal::ParamMetadata {
            offset: ::std::mem::offset_of!($v, $f),
            size: ::std::mem::size_of::<$t>(),
            array_element_size: 0,
            enum_max_value: 0,
            param_type: $crate::core::message::internal::ParamType::DriverObjectArray,
        }
    };
}

macro_rules! define_messages {
    (
        interface $iface:ident;
        $(
            message $name:ident = $id:literal {
                $(
                    version $ver:literal {
                        $( $kind:ident $field:ident : $ty:ty ; )*
                    }
                )+
            }
        )*
    ) => {
        $(
            ::paste::paste! {
                $(
                    /// Fields added in version $ver of the message, padded to a
                    /// multiple of 8 bytes.
                    #[derive(Clone, Copy, Debug, Default)]
                    #[repr(C, packed)]
                    pub struct [<$name V $ver>] {
                        $( pub $field: $crate::core::message_macros::wire_ty!($kind, $ty), )*
                        pub _padding: [u8; (8 - (0 $( + ::std::mem::size_of::<$crate::core::message_macros::wire_ty!($kind, $ty)>() )*) % 8) % 8],
                    }

                    // SAFETY: packed, no implicit padding, only integer-like
                    // fields (see `Pod`'s contract for the wire types used).
                    unsafe impl $crate::core::message::Pod for [<$name V $ver>] {}
                )+

                /// The wire structure of the message parameters: a header
                /// followed by a block for each defined version.
                #[derive(Clone, Copy, Debug, Default)]
                #[repr(C, packed)]
                pub struct [<$name Params>] {
                    pub header: $crate::core::message::internal::StructHeader,
                    $( pub [<v $ver>]: [<$name V $ver>], )+
                }

                const _: () = assert!(::std::mem::size_of::<[<$name Params>]>() % 8 == 0);

                impl $crate::core::message::MessageParams for [<$name Params>] {
                    const ID: u8 = $id;
                    const VERSIONS: &'static [$crate::core::message::internal::VersionMetadata] = &[
                        $(
                            $crate::core::message::internal::VersionMetadata {
                                version_number: $ver,
                                offset: ::std::mem::offset_of!([<$name Params>], [<v $ver>]),
                                size: ::std::mem::size_of::<[<$name V $ver>]>(),
                                params: &[
                                    $( $crate::core::message_macros::param_meta!([<$name V $ver>], $kind, $field, $ty), )*
                                ],
                            },
                        )+
                    ];
                }

                // Message versions must be declared sequentially from 0.
                const _: () = {
                    let versions = <[<$name Params>] as $crate::core::message::MessageParams>::VERSIONS;
                    let mut i = 0;
                    while i < versions.len() {
                        assert!(versions[i].version_number == i as i32);
                        i += 1;
                    }
                };

                pub type $name = $crate::core::message::MessageWithParams<[<$name Params>]>;

                impl $crate::core::message::MessageWithParams<[<$name Params>]> {
                    pub const ID: u8 = $id;

                    $(
                        pub fn [<v $ver>](&self) -> Option<&[<$name V $ver>]> {
                            self.version_ref(::std::mem::offset_of!([<$name Params>], [<v $ver>]))
                        }

                        pub fn [<v $ver _mut>](&mut self) -> Option<&mut [<$name V $ver>]> {
                            self.version_mut(::std::mem::offset_of!([<$name Params>], [<v $ver>]))
                        }
                    )+
                }
            }
        )*

        ::paste::paste! {
            /// Static descriptions of every message of this interface.
            #[allow(dead_code)]
            pub const [<$iface:upper _MESSAGE_INFOS>]: &[$crate::core::message::MessageInfo] = &[
                $(
                    $crate::core::message::MessageInfo {
                        name: stringify!($name),
                        id: $id,
                        params_size: ::std::mem::size_of::<[<$name Params>]>(),
                        versions: <[<$name Params>] as $crate::core::message::MessageParams>::VERSIONS,
                        default_bytes: || $name::new().data_view().to_vec(),
                    },
                )*
            ];

            /// Any message of this interface, or one of an unrecognized type.
            #[derive(Debug)]
            pub enum [<$iface Message>] {
                $( $name($name), )*
                Unknown($crate::core::message::Message),
            }

            #[allow(dead_code)]
            impl [<$iface Message>] {
                pub fn message(&self) -> &$crate::core::message::Message {
                    match self {
                        $( Self::$name(m) => m, )*
                        Self::Unknown(m) => m,
                    }
                }

                pub fn message_mut(&mut self) -> &mut $crate::core::message::Message {
                    match self {
                        $( Self::$name(m) => m, )*
                        Self::Unknown(m) => m,
                    }
                }
            }

            /// Routes generic messages to methods named for the messages they
            /// carry, e.g. a DoStuff message is routed to `on_do_stuff()`.
            ///
            /// Note that listeners separate message receipt (`on_message`) from
            /// message dispatch (`dispatch_message`). By default, `on_message()`
            /// simply forwards to `dispatch_message()`, but the split allows
            /// implementations to defer dispatch.
            ///
            /// All raw transport messages are fully validated and deserialized
            /// before hitting `on_message()`.
            pub trait [<$iface MessageListener>] {
                fn on_message(&self, message: [<$iface Message>]) -> bool {
                    self.dispatch_message(message)
                }

                fn dispatch_message(&self, message: [<$iface Message>]) -> bool {
                    match message {
                        $( [<$iface Message>]::$name(m) => self.[<on_ $name:snake>](m), )*
                        // Message might be from a newer version of ipcz so
                        // quietly ignore it.
                        [<$iface Message>]::Unknown(_) => true,
                    }
                }

                $(
                    fn [<on_ $name:snake>](&self, _message: $name) -> bool {
                        false
                    }
                )*

                /// Deserializes and dispatches a raw message received on
                /// `transport`. Implementors of
                /// `DriverTransport`'s listener trait should call this from
                /// their `on_transport_message()`.
                fn handle_transport_message(
                    &self,
                    raw_message: &$crate::core::driver::RawMessage<'_>,
                    transport: &$crate::core::driver_transport::DriverTransport,
                    envelope: $crate::core::driver::DriverHandle,
                ) -> bool {
                    let make_envelope = || match transport.driver() {
                        Some(driver) => $crate::core::driver_object::DriverObject::new(driver, envelope),
                        None => $crate::core::driver_object::DriverObject::default(),
                    };
                    if raw_message.data.len() >= ::std::mem::size_of::<$crate::core::message::internal::MessageHeaderV0>() {
                        // The message id is the third byte of the header.
                        let message_id = raw_message.data[2];
                        $(
                            if message_id == $name::ID {
                                let mut message = $name::new_incoming();
                                message.set_envelope(make_envelope());
                                if !message.deserialize(raw_message, transport) {
                                    return false;
                                }
                                return self.on_message([<$iface Message>]::$name(message));
                            }
                        )*
                    }

                    let mut message = $crate::core::message::Message::new_incoming();
                    message.set_envelope(make_envelope());
                    message.deserialize_unknown_type(raw_message, transport)
                        && self.on_message([<$iface Message>]::Unknown(message))
                }
            }
        }
    };
}

pub(crate) use define_messages;
pub(crate) use param_meta;
pub(crate) use wire_ty;
