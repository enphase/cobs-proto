# cobs-proto

Firmware-side (Rust, with micropb) and host-side (Python) library for COBS + CRC framed protobuf transport.
Request-response format on host side with optional streaming readings.
Type-parameterized for your particular proto message type.

See the [rust](rust/README.md) and [python](python/README.md) READMEs for examples for each package.

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
