fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/hsmq.v1.proto")?;
    Ok(())
}
