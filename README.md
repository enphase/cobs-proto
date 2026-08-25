# cobs-proto

Firmware-side (Rust, with micropb) and host-side (Python) library for COBS + CRC framed protobuf transport.
Request-response format on host side with optional streaming readings.
Type-parameterized for your particular proto message type.

This is a pre-v1 release and API stability is not guaranteed.


## Rust notes

- Async read using `embedded-io-async` and sync encode interface

Encoding usage:
```rust
type MyEncoder = PacketEncoder<
    MyEncoderProto,
    { micropb::size::max_encoded_size::<MyEncoderProto>() },
    { cobs::max_encoding_length(micropb::size::max_encoded_size::<MyEncoderProto>()) },
>;

let packet = MyEncoderProto::default();  // your packet here

let mut frame_buf = [0u8; MyEncoder::MAX_FRAME_SIZE];
match MyEncoder::encode(&packet, &mut frame_buf) {
    Ok(len) => {
        // send frame_buf[..len] over USB
    }
    Err(e) => error!("proto encode error: {}", e),
}
```

Decoding usage:
```rust
type MyDecoder = PacketDecoder<
    MyDecoderProto,
    { cobs::max_encoding_length(micropb::size::max_encoded_size::<MyDecoderProto>()) },
>;

let mut decoder = MyDecoder::new();
let mut reader = some_async_reader(); // implement embedded_io_async::Read
match decoder.read_packet(&mut reader).await {
    Ok(packet) => {
        info!("decoded packet: {}", packet);
    }
    Err(e) => {
        error!("decoder error: {}", e);
    }
}
```


## Python notes

- Async asyncio.Protocol implementation
- Provides test helpers for unit testing

Example usage:
```python

```
