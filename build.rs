// Use this in build.rs
fn main() -> std::io::Result<()> {
    // plain buffer
    let mut config = prost_build::Config::new();
    config.out_dir("src/protos").extern_path(".table_store", "crate::protos");
    config.compile_protos(
        &[
            "src/protos/table_store.proto",
            "src/protos/table_store_filter.proto",
            "src/protos/table_store_search.proto",
            "src/protos/timeseries.proto",
        ],
        &["src/protos/"],
    )?;

    Ok(())
}
