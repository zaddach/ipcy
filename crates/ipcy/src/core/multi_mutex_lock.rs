//! Port of `util/multi_mutex_lock.h`.
//!
//! DEVIATION: instead of a scoped lock object holding raw mutex pointers, these
//! helpers return the guards directly (Rust's RAII guards already unlock on
//! drop). Locks are always acquired in a globally consistent order based on the
//! address of each mutex, regardless of argument order. The mutexes must be
//! distinct.

use std::sync::{Mutex, MutexGuard};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Poisoning is not part of the C++ model; keep going with the inner state.
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn addr<T>(mutex: &Mutex<T>) -> usize {
    mutex as *const Mutex<T> as usize
}

/// Locks two mutexes in address order, returning guards in argument order.
pub fn lock2<'a, A, B>(a: &'a Mutex<A>, b: &'a Mutex<B>) -> (MutexGuard<'a, A>, MutexGuard<'a, B>) {
    if addr(a) < addr(b) {
        let ga = lock(a);
        (ga, lock(b))
    } else {
        let gb = lock(b);
        (lock(a), gb)
    }
}

/// Locks three mutexes in address order, returning guards in argument order.
pub fn lock3<'a, A, B, C>(
    a: &'a Mutex<A>,
    b: &'a Mutex<B>,
    c: &'a Mutex<C>,
) -> (MutexGuard<'a, A>, MutexGuard<'a, B>, MutexGuard<'a, C>) {
    let mut order = [(addr(a), 0), (addr(b), 1), (addr(c), 2)];
    order.sort_unstable();
    let (mut ga, mut gb, mut gc) = (None, None, None);
    order.iter().for_each(|&(_, i)| match i {
        0 => ga = Some(lock(a)),
        1 => gb = Some(lock(b)),
        _ => gc = Some(lock(c)),
    });
    (ga.unwrap(), gb.unwrap(), gc.unwrap())
}

/// Locks four mutexes in address order, returning guards in argument order.
#[allow(clippy::type_complexity)]
pub fn lock4<'a, A, B, C, D>(
    a: &'a Mutex<A>,
    b: &'a Mutex<B>,
    c: &'a Mutex<C>,
    d: &'a Mutex<D>,
) -> (
    MutexGuard<'a, A>,
    MutexGuard<'a, B>,
    MutexGuard<'a, C>,
    MutexGuard<'a, D>,
) {
    let mut order = [(addr(a), 0), (addr(b), 1), (addr(c), 2), (addr(d), 3)];
    order.sort_unstable();
    let (mut ga, mut gb, mut gc, mut gd) = (None, None, None, None);
    order.iter().for_each(|&(_, i)| match i {
        0 => ga = Some(lock(a)),
        1 => gb = Some(lock(b)),
        2 => gc = Some(lock(c)),
        _ => gd = Some(lock(d)),
    });
    (ga.unwrap(), gb.unwrap(), gc.unwrap(), gd.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn guards_in_argument_order() {
        let (a, b, c, d) = (Mutex::new(1), Mutex::new("b"), Mutex::new(3u8), Mutex::new(4.0));
        {
            let (ga, gb) = lock2(&a, &b);
            assert_eq!((*ga, *gb), (1, "b"));
        }
        {
            let (gc, ga, gb) = lock3(&c, &a, &b);
            assert_eq!((*gc, *ga, *gb), (3, 1, "b"));
        }
        let (gd, gc, gb, ga) = lock4(&d, &c, &b, &a);
        assert_eq!((*gd, *gc, *gb, *ga), (4.0, 3, "b", 1));
    }

    #[test]
    fn opposite_orders_do_not_deadlock() {
        let a = Arc::new(Mutex::new(0u32));
        let b = Arc::new(Mutex::new(0u32));
        let handles = (0..4)
            .map(|i| {
                let (a, b) = (a.clone(), b.clone());
                std::thread::spawn(move || {
                    (0..2000).for_each(|_| {
                        if i % 2 == 0 {
                            let (mut x, mut y) = lock2(&a, &b);
                            *x += 1;
                            *y += 1;
                        } else {
                            let (mut y, mut x) = lock2(&b, &a);
                            *x += 1;
                            *y += 1;
                        }
                    })
                })
            })
            .collect::<Vec<_>>();
        handles.into_iter().for_each(|h| h.join().unwrap());
        assert_eq!((*a.lock().unwrap(), *b.lock().unwrap()), (8000, 8000));
    }
}
