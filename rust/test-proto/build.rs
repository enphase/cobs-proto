fn main() {
    let mut generator = micropb_gen::Generator::new();
    generator.use_container_heapless()
        .configure(
            ".test_packet.DevicePacket.error",
            micropb_gen::Config::new().max_bytes(32),
        )
        .add_protoc_arg("-I../../proto");

    generator.compile_protos(
        &["test_packet.proto"],
        std::env::var("OUT_DIR").unwrap() + "/test_packet.rs",
    )
    .unwrap();
}
