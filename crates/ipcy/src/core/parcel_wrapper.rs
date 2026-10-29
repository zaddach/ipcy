//! Port of `ipcz/parcel_wrapper.{h,cc}`.

use std::sync::{Arc, Mutex, MutexGuard};

use super::api_object::{self, ApiObject, ApiObjectType};
use super::ipcz_result::IpczResult;
use super::ipcz_types::{
    BeginGetFlags, BeginGetOutput, EndGetFlags, GetFlags, GetOutput, IpczHandle, IpczTransaction,
};
use super::parcel::Parcel;

#[derive(Default)]
struct State {
    parcel: Option<Box<Parcel>>,
    in_two_phase_get: bool,
}

/// A parcel wrapper owns a Parcel received from a portal, after the Parcel has
/// been retrieved from the portal by the application.
///
/// Applications can use these objects to perform two-phase reads of parcels
/// without blocking the receiving portal, and to report their own
/// application-level validation failures to ipcz via the Reject() API.
pub struct ParcelWrapper {
    state: Mutex<State>,
}

impl ParcelWrapper {
    pub fn new(parcel: Option<Box<Parcel>>) -> Arc<ParcelWrapper> {
        Arc::new(ParcelWrapper {
            state: Mutex::new(State {
                parcel,
                in_two_phase_get: false,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Runs `f` with the wrapped parcel. Panics if there is none.
    pub fn with_parcel<R>(&self, f: impl FnOnce(&mut Parcel) -> R) -> R {
        f(self.lock().parcel.as_mut().expect("wrapper has a parcel"))
    }

    pub fn set_parcel(&self, parcel: Option<Box<Parcel>>) {
        self.lock().parcel = parcel;
    }

    pub fn take_parcel(&self) -> Option<Box<Parcel>> {
        self.lock().parcel.take()
    }

    /// Signals application-level rejection of this parcel. `context` is an
    /// opaque value passed by the application and propagated to the driver when
    /// appropriate. See the Reject() API.
    pub fn reject(&self, context: usize) -> IpczResult {
        let remote_source = self.with_parcel(|parcel| parcel.remote_source().cloned());
        let Some(remote_source) = remote_source else {
            return IpczResult::FailedPrecondition;
        };

        let transport = remote_source.transport();
        if let Some(driver) = transport.driver() {
            driver.report_bad_transport_activity(transport.driver_handle(), context);
        }
        IpczResult::Ok
    }

    /// Consumes the parcel's data and handles into `data` and `handles`.
    pub fn get(
        self: &Arc<Self>,
        flags: GetFlags,
        data: &mut [u8],
        handles: &mut [IpczHandle],
        want_parcel: bool,
    ) -> GetOutput {
        let mut state = self.lock();
        if state.in_two_phase_get {
            return GetOutput::failure(IpczResult::AlreadyExists);
        }
        let parcel = state.parcel.as_mut().expect("wrapper has a parcel");

        let allow_partial = flags.contains(GetFlags::PARTIAL);
        let (data_capacity, handles_capacity) = (data.len(), handles.len());
        let data_size = if allow_partial {
            parcel.data_size().min(data_capacity)
        } else {
            parcel.data_size()
        };
        let handles_size = if allow_partial {
            parcel.num_objects().min(handles_capacity)
        } else {
            parcel.num_objects()
        };

        let mut output = GetOutput {
            result: IpczResult::Ok,
            num_bytes: data_size,
            num_handles: handles_size,
            parcel: None,
        };

        let consuming_whole_parcel = data_capacity >= data_size && handles_capacity >= handles_size;
        if !consuming_whole_parcel && !allow_partial {
            output.result = IpczResult::ResourceExhausted;
            return output;
        }

        data[..data_size].copy_from_slice(&parcel.data_view()[..data_size]);
        parcel.consume_handles(&mut handles[..handles_size]);
        drop(state);

        if want_parcel {
            // Allow the caller to acquire another handle to this wrapper. Not
            // particularly useful, but consistent with Router::get().
            output.parcel = Some(api_object::release_as_handle(self.clone()));
        }
        output
    }

    pub fn begin_get(&self, flags: BeginGetFlags, handles: &mut [IpczHandle]) -> BeginGetOutput {
        if flags.contains(BeginGetFlags::OVERLAPPED) {
            return BeginGetOutput::failure(IpczResult::InvalidArgument);
        }

        let mut state = self.lock();
        if state.in_two_phase_get {
            return BeginGetOutput::failure(IpczResult::AlreadyExists);
        }
        let parcel = state.parcel.as_mut().expect("wrapper has a parcel");

        let allow_partial = flags.contains(BeginGetFlags::PARTIAL);
        let num_objects = parcel.num_objects();
        let num_handles_to_consume = num_objects.min(handles.len());
        let mut output = BeginGetOutput {
            result: IpczResult::Ok,
            data: parcel.data_ptr(),
            num_bytes: parcel.data_size(),
            num_handles: num_handles_to_consume,
            transaction: 0,
        };

        if num_handles_to_consume < num_objects && !allow_partial {
            output.result = IpczResult::ResourceExhausted;
            output.num_handles = num_objects;
            return output;
        }

        parcel.consume_handles(&mut handles[..num_handles_to_consume]);
        output.transaction = &**parcel as *const Parcel as IpczTransaction;
        state.in_two_phase_get = true;
        output
    }

    pub fn end_get(
        self: &Arc<Self>,
        transaction: IpczTransaction,
        _flags: EndGetFlags,
        want_parcel: bool,
    ) -> (IpczResult, Option<IpczHandle>) {
        {
            let mut state = self.lock();
            let current = state.parcel.as_ref().map(|p| &**p as *const Parcel as IpczTransaction);
            if current != Some(transaction) || !state.in_two_phase_get {
                return (IpczResult::InvalidArgument, None);
            }
            state.in_two_phase_get = false;
        }

        let handle = want_parcel.then(|| api_object::release_as_handle(self.clone()));
        (IpczResult::Ok, handle)
    }
}

impl ApiObject for ParcelWrapper {
    fn object_type(&self) -> ApiObjectType {
        ApiObjectType::Parcel
    }

    fn close(self: Arc<Self>) -> IpczResult {
        IpczResult::Ok
    }
}
