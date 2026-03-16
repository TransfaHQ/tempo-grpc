pub mod codec;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub mod proto {
    tonic::include_proto!("exex");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("exex_descriptor");
}
