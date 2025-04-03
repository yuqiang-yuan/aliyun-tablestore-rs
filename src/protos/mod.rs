//! Protobuf and plainbuf and types generated from `.proto` file using `prost_build`.

// Mapping to Java SDK: ots_internal_api.proto
include!("./table_store.rs");

include!("./timeseries_fbs.rs");

pub mod filter {
    // Mapping to Java SDK: ots_filter.proto
    include!("./table_store_filter.rs");
}

pub mod search {
    // Mapping to Java SDK: search.proto
    // Line 999 and Line 1000 are updated to add `table_store.` prefix to `ConsumedCapacity`
    include!("./table_store_search.rs");
}

pub mod timeseries {
    include!("./timeseries.rs");
}

pub mod plain_buffer;
pub mod simple_row_matrix;
