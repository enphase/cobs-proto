// Copyright 2026 Enphase Energy, Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

use cobs_proto::{DecodeError, PacketDecoder, PacketEncoder, max_wire_size};
use embassy_futures::block_on;
use test_proto::proto::test_packet_::*;

// --- Shared conformance vectors ---
// The binary frames are shared with Python to check wire compatibility.

const DEVICE_PACKET: DevicePacket = DevicePacket {
    error: heapless::String::new(),
    payload: Some(DevicePacket_::Payload::ValueStatus(ValueStatus {
        current_value: 42,
    })),
};
const DEVICE_FRAME: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../test_proto/device_value_status_42.bin"
));

const HOST_PACKET: HostPacket = HostPacket {
    packet: Some(HostPacket_::Packet::Ping(Ping {})),
};
const HOST_FRAME: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../test_proto/host_ping.bin"
));

// --- Type aliases ---

type DeviceEncoder = PacketEncoder<DevicePacket, { max_wire_size::<DevicePacket>() }>;
type DeviceDecoder = PacketDecoder<DevicePacket, { max_wire_size::<DevicePacket>() }>;
type HostEncoder = PacketEncoder<HostPacket, { max_wire_size::<HostPacket>() }>;
type HostDecoder = PacketDecoder<HostPacket, { max_wire_size::<HostPacket>() }>;

// --- Mock reader ---

/// A mock `embedded_io_async::Read` that returns pre-configured responses.
///
/// Each call to `read()` copies the next entry from `responses` into the caller's buffer.
/// Panics if `read()` is called after all responses have been consumed.
struct MockReader<'a> {
    responses: Vec<&'a [u8]>,
    index: usize,
}

impl<'a> MockReader<'a> {
    fn new(responses: Vec<&'a [u8]>) -> Self {
        Self {
            responses,
            index: 0,
        }
    }
}

impl embedded_io_async::ErrorType for MockReader<'_> {
    type Error = embedded_io_async::ErrorKind;
}

impl embedded_io_async::Read for MockReader<'_> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        assert!(
            self.index < self.responses.len(),
            "MockReader exhausted: no more responses"
        );
        let data = self.responses[self.index];
        self.index += 1;
        buf[..data.len()].copy_from_slice(data);
        Ok(data.len())
    }
}

// --- Encoder tests ---

#[test]
fn encode_produces_golden_frame() {
    let mut buf = [0u8; max_wire_size::<DevicePacket>()];
    let len = DeviceEncoder::encode(&DEVICE_PACKET, &mut buf).unwrap();
    assert_eq!(&buf[..len], DEVICE_FRAME);
}

#[test]
fn encode_host_produces_golden_frame() {
    let mut buf = [0u8; max_wire_size::<HostPacket>()];
    let len = HostEncoder::encode(&HOST_PACKET, &mut buf).unwrap();
    assert_eq!(&buf[..len], HOST_FRAME);
}

// --- Decoder tests ---

#[test]
fn decode_golden_frame() {
    let mut decoder = DeviceDecoder::new();
    let mut reader = MockReader::new(vec![DEVICE_FRAME]);
    let packet = block_on(decoder.read_packet(&mut reader)).unwrap();
    assert_eq!(packet, DEVICE_PACKET);
}

#[test]
fn decode_host_golden_frame() {
    let mut decoder = HostDecoder::new();
    let mut reader = MockReader::new(vec![HOST_FRAME]);
    let packet = block_on(decoder.read_packet(&mut reader)).unwrap();
    assert_eq!(packet, HOST_PACKET);
}

// --- Error tests ---

#[test]
fn crc_mismatch() {
    let mut corrupted = DEVICE_FRAME.to_vec();
    corrupted[3] ^= 0xFF;
    let mut decoder = DeviceDecoder::new();
    let mut reader = MockReader::new(vec![&corrupted]);
    let result = block_on(decoder.read_packet(&mut reader));
    assert!(matches!(result, Err(DecodeError::CrcMismatch)));
}

// --- Fragmented delivery ---

#[test]
fn decode_with_single_byte_reads() {
    // Feed the frame one byte at a time
    let mut decoder = DeviceDecoder::new();
    let mut reader = MockReader::new(DEVICE_FRAME.chunks(1).collect());
    let packet = block_on(decoder.read_packet(&mut reader)).unwrap();
    assert_eq!(packet, DEVICE_PACKET);
}

// --- Sequential packets ---

#[test]
fn decode_two_packets_sequentially() {
    let mut decoder = DeviceDecoder::new();
    let mut reader = MockReader::new(vec![DEVICE_FRAME, DEVICE_FRAME]);

    let pkt1 = block_on(decoder.read_packet(&mut reader)).unwrap();
    let pkt2 = block_on(decoder.read_packet(&mut reader)).unwrap();
    assert_eq!(pkt1, DEVICE_PACKET);
    assert_eq!(pkt2, DEVICE_PACKET);
}
