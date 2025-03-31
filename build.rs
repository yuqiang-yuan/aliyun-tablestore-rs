// Use this in build.rs
fn main() -> std::io::Result<()> {
    let mut config = prost_build::Config::new();
    config.extern_path(".table_store", "crate::protos");
    config.compile_protos(
        &[
            "src/protos/table_store.proto",
            "src/protos/table_store_filter.proto",
            "src/protos/table_store_search.proto",
        ],
        &["src/protos/"],
    )?;

    Ok(())
}
