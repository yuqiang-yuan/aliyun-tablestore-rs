// Use this in build.rs
fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["src/protos/table_store.proto"], &["src/protos"])?;

    Ok(())
}
