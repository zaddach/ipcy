//! Port of `test_messages_generator.h` and `message_test_types.h`: messages used
//! only by tests.

use crate::core::message::internal::DriverObjectArrayData;
use crate::core::message::WireEnum;
use crate::core::message_macros::define_messages;

/// Like `LinkSide`: a one-byte enum for generated message validation code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TestEnum8 {
    A = 0,
    B = 1,
    C = 2,
}

impl WireEnum for TestEnum8 {
    type Raw = u8;
    const MAX_VALUE: u32 = TestEnum8::C as u32;

    fn from_raw(raw: u8) -> Option<Self> {
        [Self::A, Self::B, Self::C].into_iter().find(|v| *v as u8 == raw)
    }

    fn to_raw(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum TestEnum32 {
    Zero = 0,
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
}

impl WireEnum for TestEnum32 {
    type Raw = u32;
    const MAX_VALUE: u32 = TestEnum32::Four as u32;

    fn from_raw(raw: u32) -> Option<Self> {
        [Self::Zero, Self::One, Self::Two, Self::Three, Self::Four]
            .into_iter()
            .find(|v| *v as u32 == raw)
    }

    fn to_raw(self) -> u32 {
        self as u32
    }
}

define_messages! {
    interface Test;

    message BasicTestMessage = 0 {
        version 0 {
            param foo: u32;
            param bar: u32;
        }
    }

    message MessageWithDataArray = 1 {
        version 0 {
            param_array values: u64;
        }
    }

    message MessageWithDriverObject = 2 {
        version 0 {
            driver_object object: u32;
        }
    }

    message MessageWithDriverObjectArray = 3 {
        version 0 {
            driver_object_array objects: DriverObjectArrayData;
        }
    }

    message MessageWithDriverArrayAndExtraObject = 4 {
        version 0 {
            driver_object_array objects: DriverObjectArrayData;
            driver_object extra_object: u32;
        }
    }

    message MessageWithMultipleVersions = 5 {
        version 0 {
            param a: u32;
            param b: u32;
        }
        version 1 {
            param c: u64;
            param d: u64;
        }
        version 2 {
            param_array e: u32;
        }
    }

    message MessageWithEnums = 6 {
        version 0 {
            param padding1: u16;
            param_enum foo: TestEnum8;
            param padding2: u8;
            param_enum bar: TestEnum32;
        }
    }
}
