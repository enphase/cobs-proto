# cobs-proto

Device-side COBS + CRC framed protobuf transport for serial links, built on
[micropb](https://crates.io/crates/micropb) and
[embedded-io-async](https://crates.io/crates/embedded-io-async).
Async decode (accumulating partial frames), sync encode.
Buffer sizes are derived at compile time using const generics.

`no_std`, no allocator.  

See the [Python README](https://github.com/enphase/cobs-proto/blob/main/python/README.md) for details on the matching host-side implementation.
See the [top-level README](https://github.com/enphase/cobs-proto) for details on the wire format.

This is a pre-v1 release and API stability is not guaranteed.


## Example

```toml
[dependencies]
cobs-proto = "0.1"
```

### Encoding

```rust
use cobs_proto::{PacketEncoder, max_wire_size};
use test_proto::proto::test_packet_::*;

type DeviceEncoder = PacketEncoder<DevicePacket, { max_wire_size::<DevicePacket>() }>;

let packet = DevicePacket {
    error: Default::default(),
    payload: Some(DevicePacket_::Payload::ValueStatus(
        ValueStatus { current_value: 42 },
    )),
};

let mut buf = [0u8; max_wire_size::<DevicePacket>()];
let len = DeviceEncoder::encode(&packet, &mut buf).unwrap();
assert_eq!(&buf[..len], &[0x00, 0x07, 0x1a, 0x02, 0x08, 0x2a, 0x11, 0xed, 0x00]);
```

### Decoding

```rust
use cobs_proto::{PacketDecoder, max_wire_size};
use test_proto::proto::test_packet_::*;

type HostDecoder = PacketDecoder<HostPacket, { max_wire_size::<HostPacket>() }>;

let mut decoder = HostDecoder::new();
// In real code, pass an `embedded_io_async::Read` impl (e.g. USB/UART):
// let packet = decoder.read_packet(&mut reader).await.unwrap();
```
