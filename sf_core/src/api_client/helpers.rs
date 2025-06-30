use arrow::ffi_stream::FFI_ArrowArrayStream;

use crate::thrift_gen::database_driver_v1::ArrowArrayStreamPtr;

impl From<Box<ArrowArrayStreamPtr>> for *mut FFI_ArrowArrayStream {
    fn from(ptr: Box<ArrowArrayStreamPtr>) -> Self {
        unsafe { std::ptr::read(ptr.value.as_ptr() as *const *mut FFI_ArrowArrayStream) }
    }
}
