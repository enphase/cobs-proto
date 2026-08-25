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

"""COBS+CRC framed protobuf transport over asyncio serial.

Provides a base class for request/response protocols that use:
- Protobuf serialization
- CRC-16/XMODEM integrity
- COBS byte stuffing with 0x00 frame delimiter

Subclasses specify the concrete protobuf message types and (for streaming
devices) implement packet classification to route between the response
future and the streaming queue.
"""

import abc
import asyncio
import binascii
import logging
import struct
from typing import (
    AsyncIterator,
    Generic,
    Optional,
    Type,
    TypeVar,
)

import cobs.cobs
from google.protobuf import message as pb_message

TWireHostPacket = TypeVar("TWireHostPacket", bound=pb_message.Message)
TWireDevicePacket = TypeVar("TWireDevicePacket", bound=pb_message.Message)


class CobsProtoProtocol(
    asyncio.Protocol,
    abc.ABC,
    Generic[TWireHostPacket, TWireDevicePacket],
):
    """Base asyncio.Protocol for COBS+CRC framed protobuf serial communication.

    Only one request may be in flight at a time.

    Type parameters:
        TWireHostPacket: The protobuf message serialized on the wire to the device.
        TWireDevicePacket: The protobuf message deserialized from the wire.
    """

    REQUEST_TIMEOUT = 1.0  # seconds before a pending request is discarded

    # --- Subclass must set these ---

    @staticmethod
    @abc.abstractmethod
    def _device_packet_type() -> Type[TWireDevicePacket]:
        """Return the protobuf class used to deserialize incoming device packets."""
        ...

    def __init__(self) -> None:
        super().__init__()
        self._buffer = bytearray()
        self._transport: Optional[asyncio.Transport] = None
        self._pending_response: Optional[asyncio.Future[TWireDevicePacket]] = None
        self._connected = asyncio.Event()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        serial = transport.serial  # type: ignore[attr-defined]
        serial.reset_input_buffer()  # clear any stale pending data in the buffer
        self._transport = transport
        self._connected.set()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logging.error(f"connection lost: {exc}")
        self._transport = None

    async def wait_connected(self) -> None:
        """Block until the serial connection is established."""
        await self._connected.wait()

    # --- Send side ---

    def _write_message(self, message: pb_message.Message) -> None:
        if self._transport is None:
            raise ConnectionError("no connection")
        message_bytes = message.SerializeToString()
        crc = binascii.crc_hqx(message_bytes, 0)
        message_with_crc = message_bytes + struct.pack(">H", crc)
        cobs_bytes = cobs.cobs.encode(message_with_crc)
        self._transport.write(b"\x00" + cobs_bytes + b"\x00")

    async def send_request(self, packet: TWireHostPacket) -> TWireDevicePacket:
        """Send a command and await the response.

        Raises ``asyncio.TimeoutError`` if no response arrives in time.
        """
        assert self._pending_response is None, "only one request may be in flight"

        fut: asyncio.Future[TWireDevicePacket] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending_response = fut
        self._write_message(packet)
        try:
            return await asyncio.wait_for(asyncio.shield(fut), self.REQUEST_TIMEOUT)
        except asyncio.TimeoutError:
            self._pending_response = None
            raise

    # --- Receive side ---

    def _dispatch_device_packet(self, packet: TWireDevicePacket) -> None:
        """Route a decoded device packet to the pending response future.

        Subclasses (e.g. streaming) may override to intercept other
        messages before delegating to super.
        """
        if self._pending_response is not None:
            fut = self._pending_response
            self._pending_response = None
            fut.set_result(packet)
        else:
            logging.warning(
                f"discarding received packet with no pending request: {packet}"
            )

    def data_received(self, data: bytes) -> None:
        self._buffer += data
        split = self._buffer.split(b"\x00")
        self._buffer = bytearray(split[-1])
        for serial_bytes in split[:-1]:
            if not serial_bytes:
                continue  # skip empty frames
            try:
                decoded_bytes = cobs.cobs.decode(serial_bytes)
                if len(decoded_bytes) < 2:
                    raise ValueError("Frame too short for CRC")
                message_bytes = decoded_bytes[:-2]
                received_crc = struct.unpack(">H", decoded_bytes[-2:])[0]
                computed_crc = binascii.crc_hqx(message_bytes, 0)
                if received_crc != computed_crc:
                    raise ValueError(
                        f"CRC mismatch: received {received_crc:04x}, computed {computed_crc:04x}"
                    )
                packet = self._device_packet_type().FromString(message_bytes)
            except Exception as e:
                logging.warning(
                    f"discarding bad packet: {len(serial_bytes)} bytes, {serial_bytes!r} - {e}"
                )
                continue
            logging.debug(f"received message {packet}")

            self._dispatch_device_packet(packet)


class CobsProtoStreamingProtocol(
    CobsProtoProtocol[TWireHostPacket, TWireDevicePacket],
    Generic[TWireHostPacket, TWireDevicePacket],
):
    """Extension of ``CobsProtoProtocol`` for devices that also send
    unsolicited streaming packets alongside request/response traffic.

    Subclasses implement ``_is_streaming_packet`` to determine routing:
    return ``True`` for streaming packets (enqueued for ``iter_readings``),
    ``False`` for request-response packets (resolved against the pending future).

    The streaming queue holds the full ``TWireDevicePacket``; unpacking
    is the caller's responsibility.
    """

    @abc.abstractmethod
    def _is_streaming_packet(self, packet: TWireDevicePacket) -> bool:
        """Return ``True`` if the packet is an unsolicited streaming packet,
        ``False`` if it is a response to a request."""
        ...

    def __init__(self) -> None:
        super().__init__()
        self._reading_queue: asyncio.Queue[TWireDevicePacket] = asyncio.Queue()

    def _dispatch_device_packet(self, packet: TWireDevicePacket) -> None:
        if self._is_streaming_packet(packet):
            self._reading_queue.put_nowait(packet)
        else:
            super()._dispatch_device_packet(packet)

    async def next_reading(self) -> TWireDevicePacket:
        """Return the next streaming packet, blocking if none is available."""
        return await self._reading_queue.get()

    async def iter_readings(self) -> AsyncIterator[TWireDevicePacket]:
        """Yield streaming packets as they arrive."""
        while True:
            yield await self._reading_queue.get()
