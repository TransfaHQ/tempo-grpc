pub mod codec;

pub mod proto {
    tonic::include_proto!("exex");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("exex_descriptor");
}
