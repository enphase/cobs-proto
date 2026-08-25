# cobs-proto

Firmware-side (Rust, with micropb) and host-side (Python) library for COBS + CRC framed protobuf transport.
Type-parameterized for your particular proto message type.

This is a pre-v1 release and API stability is not guaranteed.


## Rust notes

- Async read using `embedded-io-async` and sync encode interface

## Python notes

- Async asyncio.Protocol implementation
- Provides test helpers for unit testing
