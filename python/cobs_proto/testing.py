"""Test helpers for COBS+CRC framed asyncio protocols."""

import asyncio
import binascii
import struct
from typing import TypeVar
from unittest.mock import MagicMock

import cobs.cobs
from google.protobuf import message as pb_message

TProtocol = TypeVar("TProtocol", bound=asyncio.Protocol)


def make_connected_protocol(protocol: TProtocol) -> tuple[TProtocol, MagicMock]:
    """Wire a mock serial transport to an asyncio Protocol and call connection_made.

    Returns ``(protocol, transport)`` so the caller can inspect writes if needed.
    """
    transport = MagicMock(spec=asyncio.Transport)
    transport.serial = MagicMock()
    transport.serial.reset_input_buffer = MagicMock()
    protocol.connection_made(transport)
    return protocol, transport


def encode_device_frame(packet: pb_message.Message) -> bytes:
    """Encode a protobuf message into a COBS+CRC framed wire packet."""
    message_bytes = packet.SerializeToString()
    crc = binascii.crc_hqx(message_bytes, 0)
    message_with_crc = message_bytes + struct.pack(">H", crc)
    return b"\x00" + cobs.cobs.encode(message_with_crc) + b"\x00"


async def respond(
    protocol: asyncio.Protocol,
    wire_packet: pb_message.Message,
) -> None:
    """Wait one event loop turn then feed wire_packet into the protocol."""
    await asyncio.sleep(0)
    protocol.data_received(encode_device_frame(wire_packet))
