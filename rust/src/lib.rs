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

#![no_std]
#![doc = include_str!("../../README.md")]

use embedded_io_async::Read;
use micropb::{MessageDecode, MessageEncode, PbEncoder};

/// Maximum wire frame size (bytes) for message type `T`, including sentinels, COBS overhead, CRC16,
/// and proto payload. This is the single const generic needed for both `PacketEncoder` and `PacketDecoder`.
pub const fn max_wire_size<T: MessageEncode>() -> usize {
    // proto + 2-byte CRC, then COBS-encoded, plus 2 sentinel bytes
    let proto_and_crc = micropb::size::max_encoded_size::<T>() + 2;
    cobs::max_encoding_length(proto_and_crc) + 2
}

// CRC-16/XMODEM for frame integrity (poly 0x1021, init 0x0000, no reflection)
// Matches Python's binascii.crc_hqx
pub const CRC_XMODEM: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

const RX_BUF_SIZE: usize = 16;

#[derive(Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum EncodeError<E> {
    ProtoEncode(E),
}

#[derive(Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum DecodeError<E> {
    Io,
    CobsDecoding,
    CrcMismatch,
    ProtoDecoding(E),
}

pub struct PacketEncoder<T: MessageEncode, const MAX_WIRE_SIZE: usize> {
    _phantom: core::marker::PhantomData<T>,
}

impl<T: MessageEncode, const MAX_WIRE_SIZE: usize> PacketEncoder<T, MAX_WIRE_SIZE> {
    /// Encodes a packet in-place into the provided buffer.
    /// Returns the number of bytes written (includes leading and trailing sentinels).
    ///
    /// # Panics
    /// Panics if buffer is smaller than `MAX_WIRE_SIZE`.
    ///
    /// # Errors
    /// - `EncodeError::ProtoEncode` if proto encoding fails
    pub fn encode(packet: &T, buffer: &mut [u8]) -> Result<usize, EncodeError<()>> {
        assert!(buffer.len() >= MAX_WIRE_SIZE, "buffer too small");

        // Encode proto into temporary stack buffer.
        let mut proto_buf = [0u8; MAX_WIRE_SIZE];
        let mut encoder = PbEncoder::new(&mut proto_buf[..]);
        packet
            .encode(&mut encoder)
            .map_err(|_| EncodeError::ProtoEncode(()))?;
        let proto_size = packet.compute_size();

        // Compute CRC16 of proto bytes
        let crc = CRC_XMODEM.checksum(&proto_buf[..proto_size]);

        // Append CRC16 in big-endian format after proto bytes (in-place)
        proto_buf[proto_size] = (crc >> 8) as u8;
        proto_buf[proto_size + 1] = (crc & 0xFF) as u8;

        // COBS encode proto+CRC into output buffer (includes leading + trailing sentinels)
        let cobs_size = cobs::encode_including_sentinels(&proto_buf[..proto_size + 2], buffer);

        Ok(cobs_size)
    }
}

pub struct PacketDecoder<T: MessageDecode + MessageEncode + Default, const MAX_WIRE_SIZE: usize> {
    cobs_decoder: cobs::CobsDecoderHeapless<MAX_WIRE_SIZE>,
    rx_buf: [u8; RX_BUF_SIZE],
    rx_start: usize,
    rx_end: usize,
    _phantom: core::marker::PhantomData<T>,
}

impl<T: MessageDecode + MessageEncode + Default, const MAX_WIRE_SIZE: usize> Default
    for PacketDecoder<T, MAX_WIRE_SIZE>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T: MessageDecode + MessageEncode + Default, const MAX_WIRE_SIZE: usize>
    PacketDecoder<T, MAX_WIRE_SIZE>
{
    pub fn new() -> Self {
        Self {
            cobs_decoder: cobs::CobsDecoderHeapless::new(),
            // store the buffer to allow the decoder to return packets mid-read-buffer
            rx_buf: [0u8; RX_BUF_SIZE],
            rx_start: 0,
            rx_end: 0,
            _phantom: core::marker::PhantomData,
        }
    }

    /// Reads from reader until a complete packet is decoded.
    ///
    /// Awaits until either:
    /// - A valid packet is decoded (returns Ok(packet))
    /// - A decode error occurs (returns Err, auto-resets to frame sync)
    ///
    /// Drop-safe: Can be cancelled without losing data. Next call resumes correctly.
    ///
    /// # Errors
    /// - `DecodeError::Io` if read fails
    /// - `DecodeError::CobsDecoding` if COBS frame is invalid
    /// - `DecodeError::CrcMismatch` if CRC16 validation fails
    /// - `DecodeError::ProtoDecoding` if proto is invalid
    pub async fn read_packet<R: Read>(&mut self, reader: &mut R) -> Result<T, DecodeError<()>> {
        loop {
            // Read new data if buffer is empty
            if self.rx_start >= self.rx_end {
                let n = reader
                    .read(&mut self.rx_buf)
                    .await
                    .map_err(|_| DecodeError::Io)?;
                self.rx_start = 0;
                self.rx_end = n;
            }

            // Process bytes in buffer
            let decode_result = self
                .cobs_decoder
                .push(&self.rx_buf[self.rx_start..self.rx_end]);

            match decode_result {
                Ok(None) => {
                    // Consumed all bytes, need more data
                    self.rx_start = self.rx_end;
                }
                Ok(Some(decoded)) => {
                    self.rx_start += decoded.parsed_size();

                    let decoded_data = &self.cobs_decoder.dest()[..decoded.frame_size()];

                    // Frame must have at least 2 bytes for CRC16
                    if decoded_data.len() < 2 {
                        self.cobs_decoder.reset();
                        return Err(DecodeError::CrcMismatch);
                    }

                    // Extract CRC16 (last 2 bytes, big-endian)
                    let proto_len = decoded_data.len() - 2;
                    let received_crc =
                        u16::from_be_bytes([decoded_data[proto_len], decoded_data[proto_len + 1]]);

                    // Compute CRC16 of proto bytes
                    let computed_crc = CRC_XMODEM.checksum(&decoded_data[..proto_len]);

                    // Validate CRC
                    if received_crc != computed_crc {
                        self.cobs_decoder.reset();
                        return Err(DecodeError::CrcMismatch);
                    }

                    // Decode proto
                    let mut packet = T::default();
                    packet
                        .decode_from_bytes(&decoded_data[..proto_len])
                        .map_err(|_| DecodeError::ProtoDecoding(()))?;
                    return Ok(packet);
                }
                Err(_) => {
                    // COBS decoding error - reset and try to recover
                    self.rx_start = self.rx_end;
                    self.cobs_decoder.reset();
                    return Err(DecodeError::CobsDecoding);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use embassy_futures::block_on;
    use test_proto::proto::test_packet_::*;

    // --- Type aliases ---

    type TestEncoder = PacketEncoder<DevicePacket, { max_wire_size::<DevicePacket>() }>;

    type TestDecoder = PacketDecoder<DevicePacket, { max_wire_size::<DevicePacket>() }>;

    type HostEncoder = PacketEncoder<HostPacket, { max_wire_size::<HostPacket>() }>;

    type HostDecoder = PacketDecoder<HostPacket, { max_wire_size::<HostPacket>() }>;

    // --- Mock reader ---

    /// A mock `embedded_io_async::Read` that returns pre-configured responses.
    ///
    /// Each call to `read()` copies the next entry from `responses` into the
    /// caller's buffer. Panics if `read()` is called after all responses have
    /// been consumed.
    struct MockReader<'a, const N: usize> {
        responses: [&'a [u8]; N],
        index: usize,
    }

    impl<'a, const N: usize> MockReader<'a, N> {
        fn new(responses: [&'a [u8]; N]) -> Self {
            Self {
                responses,
                index: 0,
            }
        }
    }

    impl<const N: usize> embedded_io_async::ErrorType for MockReader<'_, N> {
        type Error = embedded_io_async::ErrorKind;
    }

    impl<const N: usize> embedded_io_async::Read for MockReader<'_, N> {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            assert!(self.index < N, "MockReader exhausted: no more responses");
            let data = self.responses[self.index];
            self.index += 1;
            buf[..data.len()].copy_from_slice(data);
            Ok(data.len())
        }
    }

    // --- Cross-language golden vectors (generated by Python) ---

    // DevicePacket { value_status: ValueStatus { current_value: 42 } }
    const GOLDEN_DEVICE_PACKET: DevicePacket = DevicePacket {
        error: heapless::String::new(),
        payload: Some(DevicePacket_::Payload::ValueStatus(ValueStatus {
            current_value: 42,
        })),
    };
    const GOLDEN_DEVICE_FRAME: &[u8] = &[0x00, 0x07, 0x1a, 0x02, 0x08, 0x2a, 0x11, 0xed, 0x00];

    // HostPacket { ping: Ping {} }
    const GOLDEN_HOST_PACKET: HostPacket = HostPacket {
        packet: Some(HostPacket_::Packet::Ping(Ping {})),
    };
    const GOLDEN_HOST_FRAME: &[u8] = &[0x00, 0x02, 0x0a, 0x03, 0xef, 0xcb, 0x00];

    // --- Encoder tests ---

    #[test]
    fn encode_produces_golden_frame() {
        let mut buf = [0u8; max_wire_size::<DevicePacket>()];
        let len = TestEncoder::encode(&GOLDEN_DEVICE_PACKET, &mut buf).unwrap();
        assert_eq!(&buf[..len], GOLDEN_DEVICE_FRAME);
    }

    #[test]
    fn encode_host_produces_golden_frame() {
        let mut buf = [0u8; max_wire_size::<HostPacket>()];
        let len = HostEncoder::encode(&GOLDEN_HOST_PACKET, &mut buf).unwrap();
        assert_eq!(&buf[..len], GOLDEN_HOST_FRAME);
    }

    // --- Decoder tests ---

    #[test]
    fn decode_golden_frame() {
        let mut decoder = TestDecoder::new();
        let mut reader = MockReader::new([GOLDEN_DEVICE_FRAME]);
        let packet = block_on(decoder.read_packet(&mut reader)).unwrap();
        assert_eq!(packet, GOLDEN_DEVICE_PACKET);
    }

    #[test]
    fn decode_host_golden_frame() {
        let mut decoder = HostDecoder::new();
        let mut reader = MockReader::new([GOLDEN_HOST_FRAME]);
        let packet = block_on(decoder.read_packet(&mut reader)).unwrap();
        assert_eq!(packet, GOLDEN_HOST_PACKET);
    }

    // --- Error tests ---

    #[test]
    fn crc_mismatch() {
        let mut corrupted = [0u8; 9];
        corrupted.copy_from_slice(GOLDEN_DEVICE_FRAME);
        corrupted[3] ^= 0xFF;
        let mut decoder = TestDecoder::new();
        let mut reader = MockReader::new([&corrupted[..]]);
        let result = block_on(decoder.read_packet(&mut reader));
        assert!(matches!(result, Err(DecodeError::CrcMismatch)));
    }

    // --- Fragmented delivery ---

    #[test]
    fn decode_with_single_byte_reads() {
        // Feed the frame one byte at a time
        let frame = GOLDEN_DEVICE_FRAME;
        assert!(frame.len() == 9);
        let mut decoder = TestDecoder::new();
        let mut reader = MockReader::new([
            &frame[0..1],
            &frame[1..2],
            &frame[2..3],
            &frame[3..4],
            &frame[4..5],
            &frame[5..6],
            &frame[6..7],
            &frame[7..8],
            &frame[8..9],
        ]);
        let packet = block_on(decoder.read_packet(&mut reader)).unwrap();
        assert_eq!(packet, GOLDEN_DEVICE_PACKET);
    }

    // --- Sequential packets ---

    #[test]
    fn decode_two_packets_sequentially() {
        let mut decoder = TestDecoder::new();
        let mut reader = MockReader::new([GOLDEN_DEVICE_FRAME, GOLDEN_DEVICE_FRAME]);

        let pkt1 = block_on(decoder.read_packet(&mut reader)).unwrap();
        let pkt2 = block_on(decoder.read_packet(&mut reader)).unwrap();
        assert_eq!(pkt1, GOLDEN_DEVICE_PACKET);
        assert_eq!(pkt2, GOLDEN_DEVICE_PACKET);
    }
}
