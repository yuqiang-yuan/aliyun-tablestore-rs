//! Protobuf and plainbuf and types generated from `.proto` file using `prost_build`.

include!(concat!(env!("OUT_DIR"), "/table_store.rs"));

pub mod filter {
    // Mapping to Java SDK: ots_filter.proto
    include!(concat!(env!("OUT_DIR"), "/table_store_filter.rs"));
}

// pub mod table_store_search {
//     // Mapping to Java SDK: search.proto
//     include!(concat!(env!("OUT_DIR"), "/table_store_search.rs"));
// }

pub mod plain_buffer;
pub mod simple_row_matrix;
