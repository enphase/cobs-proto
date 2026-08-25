#![no_std]

use embedded_io_async::Read;
use micropb::{MessageDecode, MessageEncode, PbEncoder};

// Re-export for use in const generic expressions
pub use cobs;
pub use micropb;

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

// Note: PROTO_SIZE and COBS_SIZE must be provided separately because Rust's const generics don't yet support
// using const parameters in const expressions like array sizes. https://github.com/rust-lang/rust/issues/76560
pub struct PacketEncoder<T: MessageEncode, const PROTO_SIZE: usize, const COBS_SIZE: usize> {
    _phantom: core::marker::PhantomData<T>,  // suppress compiler warning since T doesn't appear in struct fields
}

impl<T: MessageEncode, const PROTO_SIZE: usize, const COBS_SIZE: usize> PacketEncoder<T, PROTO_SIZE, COBS_SIZE> {
    /// Maximum frame size for this packet type (includes COBS encoding + CRC16 + sentinel)
    pub const MAX_FRAME_SIZE: usize = COBS_SIZE + 2 + 1;

    /// Encodes a packet in-place into the provided buffer.
    /// Returns the number of bytes written (includes trailing sentinel).
    ///
    /// # Panics
    /// Panics if buffer is smaller than the maximum frame size for the packet type.
    /// This is a programming error that should be caught at compile time by using
    /// appropriately sized buffers.
    ///
    /// # Errors
    /// - `EncodeError::ProtoEncode` if proto encoding fails
    pub fn encode(
        packet: &T,
        buffer: &mut [u8],
    ) -> Result<usize, EncodeError<()>> {
        assert!(
            buffer.len() >= Self::MAX_FRAME_SIZE,
            "buffer too small"
        );

        // Encode proto into temporary stack buffer sized for this packet type
        let mut proto_buf = [0u8; PROTO_SIZE];
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

        // COBS encode proto+CRC directly into output buffer (leaving room for sentinel)
        let buffer_len = buffer.len();
        let cobs_size = cobs::encode_including_sentinels(&proto_buf[..proto_size + 2], &mut buffer[..buffer_len - 1]);

        Ok(cobs_size)
    }
}

pub struct PacketDecoder<T: MessageDecode + MessageEncode + Default, const COBS_SIZE: usize> {
    cobs_decoder: cobs::CobsDecoderHeapless<COBS_SIZE>,
    rx_buf: [u8; RX_BUF_SIZE],
    rx_start: usize,
    rx_end: usize,
    _phantom: core::marker::PhantomData<T>,
}

impl<T: MessageDecode + MessageEncode + Default, const COBS_SIZE: usize> PacketDecoder<T, COBS_SIZE> {
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
    pub async fn read_packet<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<T, DecodeError<()>> {
        loop {
            // Read new data if buffer is empty
            if self.rx_start >= self.rx_end {
                let n = reader.read(&mut self.rx_buf).await.map_err(|_| DecodeError::Io)?;
                self.rx_start = 0;
                self.rx_end = n;
            }
            
            // Process bytes in buffer
            let decode_result = self.cobs_decoder.push(&self.rx_buf[self.rx_start..self.rx_end]);
            
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
                    let received_crc = u16::from_be_bytes([
                        decoded_data[proto_len],
                        decoded_data[proto_len + 1],
                    ]);
                    
                    // Compute CRC16 of proto bytes
                    let computed_crc = CRC_XMODEM.checksum(&decoded_data[..proto_len]);
                    
                    // Validate CRC
                    if received_crc != computed_crc {
                        self.cobs_decoder.reset();
                        return Err(DecodeError::CrcMismatch);
                    }
                    
                    // Decode proto
                    let mut packet = T::default();
                    packet.decode_from_bytes(&decoded_data[..proto_len])
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
