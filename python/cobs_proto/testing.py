# Copyright 2026 Enphase Energy, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Test helpers for COBS+CRC framed asyncio protocols."""

import asyncio
import binascii
import struct
from typing import cast, TypeVar
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
    return cast(bytes, b"\x00" + cobs.cobs.encode(message_with_crc) + b"\x00")


async def respond(
    protocol: asyncio.Protocol,
    wire_packet: pb_message.Message,
) -> None:
    """Wait one event loop turn then feed wire_packet into the protocol."""
    await asyncio.sleep(0)
    protocol.data_received(encode_device_frame(wire_packet))
