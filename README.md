# cobs-proto

Firmware-side (Rust, with micropb) and host-side (Python) library for COBS + CRC framed protobuf transport.
Request-response format on host side with optional streaming readings.
Type-parameterized for your particular proto message type.

This is a pre-v1 release and API stability is not guaranteed.


## Wire format

Each frame on the wire is structured as:

```text
0x00 | COBS( proto_bytes | CRC16-BE ) | 0x00
```

- `proto_bytes`: protobuf-serialized message
- `CRC16-BE`: CRC-16/XMODEM checksum of `proto_bytes`, big-endian (matches Python's `binascii.crc_hqx`)
- `COBS(...)`: Consistent Overhead Byte Stuffing, eliminating all `0x00` bytes
- Sentinel `0x00` bytes delimit frames


## Example proto

See [`proto/test_packet.proto`](proto/test_packet.proto) for a minimal example schema used by both the Rust and Python test suites.


## Rust

`no_std` compatible. Async decode via `embedded-io-async`, sync encode.

### Encoding

```rust
use cobs_proto::{PacketEncoder, micropb, cobs};
use test_proto::proto::test_packet_::*;

type DeviceEncoder = PacketEncoder<
    DevicePacket,
    { micropb::size::max_encoded_size::<DevicePacket>() },
    { cobs::max_encoding_length(micropb::size::max_encoded_size::<DevicePacket>()) },
>;

let packet = DevicePacket {
    error: Default::default(),
    payload: Some(DevicePacket_::Payload::ValueStatus(
        ValueStatus { current_value: 42 },
    )),
};

let mut buf = [0u8; DeviceEncoder::MAX_FRAME_SIZE];
let len = DeviceEncoder::encode(&packet, &mut buf).unwrap();
assert_eq!(&buf[..len], &[0x00, 0x07, 0x1a, 0x02, 0x08, 0x2a, 0x11, 0xed, 0x00]);
```

### Decoding

```rust
use cobs_proto::{PacketDecoder, micropb, cobs};
use test_proto::proto::test_packet_::*;

type HostDecoder = PacketDecoder<
    HostPacket,
    { cobs::max_encoding_length(micropb::size::max_encoded_size::<HostPacket>()) },
>;

let mut decoder = HostDecoder::new();
// In real code, pass an `embedded_io_async::Read` impl (e.g. USB/UART):
// let packet = decoder.read_packet(&mut reader).await.unwrap();
```


## Python

Async `asyncio.Protocol` implementation with test helpers for unit testing.

### Usage

```python
import asyncio
from cobs_proto import CobsProtoProtocol
from your_proto import packet_pb2

# Subclass to specify the decoded packet type
# Request-response only, see CobsProtoStreamingProtocol which adds device streaming data
class MyProtocol(CobsProtoProtocol[packet_pb2.HostPacket, packet_pb2.DevicePacket]):
    @staticmethod
    def _device_packet_type():
        return packet_pb2.DevicePacket

# Connect (via pyserial-asyncio or similar)
transport, protocol = await serial_asyncio.create_serial_connection(
    asyncio.get_running_loop(), lambda: MyProtocol(), port, baudrate=115200,
)

# Send a request and await the response
response = await protocol.send_request(packet_pb2.HostPacket(ping=packet_pb2.Ping()))
```
