//! Port of `util/safe_math.h`.

/// Casts `value` to `Dst`, asserting that it fits.
pub fn checked_cast<Dst, Src>(value: Src) -> Dst
where
    Dst: TryFrom<Src>,
{
    Dst::try_from(value)
        .unwrap_or_else(|_| panic!("checked_cast: value out of range of destination type"))
}

/// Casts `value` to `Dst`, clamping to `Dst::MAX` if it does not fit. Like the
/// C++ original this is only meaningful for unsigned types.
pub fn saturated_cast<Dst, Src>(value: Src) -> Dst
where
    Dst: TryFrom<Src> + SaturatedMax,
{
    Dst::try_from(value).unwrap_or(Dst::MAX)
}

/// Helper trait exposing `MAX` for the unsigned integer types.
pub trait SaturatedMax {
    const MAX: Self;
}

macro_rules! impl_saturated_max {
    ($($t:ty),*) => { $(impl SaturatedMax for $t { const MAX: Self = <$t>::MAX; })* };
}
impl_saturated_max!(u8, u16, u32, u64, usize);

/// Adds `a` and `b`, asserting that the result does not overflow.
pub fn check_add<T: CheckedOps>(a: T, b: T) -> T {
    a.checked_add_(b).expect("check_add: overflow")
}

/// Multiplies `a` and `b`, asserting that the result does not overflow.
pub fn check_mul<T: CheckedOps>(a: T, b: T) -> T {
    a.checked_mul_(b).expect("check_mul: overflow")
}

/// Adds `a` and `b`, clamping to the maximum value on overflow.
pub fn saturated_add<T: CheckedOps>(a: T, b: T) -> T {
    a.checked_add_(b).unwrap_or(T::MAX_VALUE)
}

/// Uniform access to the `checked_*` operations of the integer types.
pub trait CheckedOps: Sized {
    const MAX_VALUE: Self;
    fn checked_add_(self, rhs: Self) -> Option<Self>;
    fn checked_mul_(self, rhs: Self) -> Option<Self>;
}

macro_rules! impl_checked_ops {
    ($($t:ty),*) => { $(
        impl CheckedOps for $t {
            const MAX_VALUE: Self = <$t>::MAX;
            fn checked_add_(self, rhs: Self) -> Option<Self> { self.checked_add(rhs) }
            fn checked_mul_(self, rhs: Self) -> Option<Self> { self.checked_mul(rhs) }
        }
    )* };
}
impl_checked_ops!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturated_cast_test() {
        const MAX_U32: u32 = 0xffff_ffff;
        const SMALL_U64: u64 = 0x1234_5678;
        const LARGE_U64: u64 = 0x1234_5678_9abc;

        // Casting to a smaller type within its range yields the same value.
        assert_eq!(SMALL_U64, saturated_cast::<u32, _>(SMALL_U64) as u64);

        // Casting to a smaller type outside of its range yields the max value
        // for the destination type.
        assert_eq!(MAX_U32, saturated_cast::<u32, _>(LARGE_U64));

        // Casting to a larger type always yields the same value.
        assert_eq!(MAX_U32 as u64, saturated_cast::<u64, _>(MAX_U32));
    }

    #[test]
    fn checked_ops() {
        assert_eq!(checked_cast::<u8, _>(255u32), 255);
        assert_eq!(check_add(1u32, 2), 3);
        assert_eq!(check_mul(3u32, 4), 12);
        assert_eq!(saturated_add(u32::MAX, 1), u32::MAX);
        assert_eq!(saturated_add(1u32, 1), 2);
    }

    #[test]
    #[should_panic]
    fn check_add_overflow_panics() {
        check_add(u8::MAX, 1);
    }

    #[test]
    #[should_panic]
    fn checked_cast_overflow_panics() {
        checked_cast::<u8, _>(256u32);
    }
}
