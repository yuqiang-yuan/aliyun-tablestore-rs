//! Protobuf and plainbuf
pub mod table_store {
    include!(concat!(env!("OUT_DIR"), "/table_store.rs"));
}

pub mod table_store_filter {
    include!(concat!(env!("OUT_DIR"), "/table_store_filter.rs"));
}

pub mod plain_buffer;
