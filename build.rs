fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    println!("cargo:rerun-if-changed=proto/plane.proto");
    tonic_prost_build::configure().compile_protos(&["proto/plane.proto"], &["proto"])?;
    Ok(())
}
