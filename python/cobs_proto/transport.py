"""COBS+CRC framed protobuf transport over asyncio serial.

Provides a base class for request/response protocols that use:
- Protobuf serialization
- CRC-16/XMODEM integrity
- COBS byte stuffing with 0x00 frame delimiter

Subclasses specify the concrete protobuf message types and implement
protocol-specific request construction and response dispatching.
"""

import abc
import asyncio
import binascii
import functools
import logging
import struct
from typing import (
    AsyncIterator,
    Callable,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import cobs.cobs
from google.protobuf import message as pb_message
from google.protobuf import symbol_database as pb_symbol_database

TWireHostPacket = TypeVar("TWireHostPacket", bound=pb_message.Message)
THostPayload = TypeVar("THostPayload", bound=pb_message.Message)
TWireDevicePacket = TypeVar("TWireDevicePacket", bound=pb_message.Message)
TDeviceResponse = TypeVar("TDeviceResponse", bound=pb_message.Message)
TDeviceReading = TypeVar("TDeviceReading", bound=pb_message.Message)


class ResponseError(Exception):
    """Raised when the device returns a non-empty error string."""

    def __init__(self, message: str, cmd_seq: int) -> None:
        super().__init__(message)
        self.cmd_seq = cmd_seq


class CobsProtoProtocol(
    asyncio.Protocol,
    abc.ABC,
    Generic[TWireHostPacket, THostPayload, TWireDevicePacket, TDeviceResponse],
):
    """Base asyncio.Protocol for COBS+CRC framed protobuf serial communication.

    Type parameters:
        TWireHostPacket: The full protobuf message serialized on the wire to the device.
        THostPayload: The inner command payload the caller constructs.
        TWireDevicePacket: The full protobuf message deserialized from the wire.
        TDeviceResponse: The unwrapped response type returned to the caller.
    """

    MAX_CMD_SEQ = 255  # inclusive
    REQUEST_TIMEOUT = 1.0  # seconds before a pending request is discarded

    # --- Subclass must set these ---

    @staticmethod
    @abc.abstractmethod
    def _device_packet_type() -> Type[TWireDevicePacket]:  # type: ignore[misc]
        """Return the protobuf class used to deserialize incoming device packets."""
        ...

    @abc.abstractmethod
    def _wrap_host_payload(
        self, cmd_seq: int, payload: THostPayload
    ) -> TWireHostPacket:
        """Wrap a command payload and cmd_seq into the wire host packet."""
        ...

    @abc.abstractmethod
    def _unwrap_response(
        self, packet: TWireDevicePacket
    ) -> Tuple[int, Optional[TDeviceResponse]]:
        """Extract (cmd_seq, response) from a device packet that is a command response.

        Returns (cmd_seq, payload) where payload is None for ack-only responses.
        Must raise ``ResponseError`` if the device reports an error.
        """
        ...

    def __init__(self) -> None:
        super().__init__()
        self._buffer = bytearray()
        self._transport: Optional[asyncio.Transport] = None
        self._next_cmd_seq = 1
        self._resp_waiting: Dict[int, asyncio.Future[Optional[TDeviceResponse]]] = {}
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

    def _allocate_cmd_seq(self) -> int:
        seq = self._next_cmd_seq
        self._next_cmd_seq += 1
        if self._next_cmd_seq > self.MAX_CMD_SEQ:
            self._next_cmd_seq = 1
        return seq

    def _write_message(self, message: pb_message.Message) -> None:
        if self._transport is None:
            raise ConnectionError("no connection")
        message_bytes = message.SerializeToString()
        crc = binascii.crc_hqx(message_bytes, 0)
        message_with_crc = message_bytes + struct.pack(">H", crc)
        cobs_bytes = cobs.cobs.encode(message_with_crc)
        self._transport.write(b"\x00" + cobs_bytes + b"\x00")

    async def send_request(self, payload: THostPayload) -> Optional[TDeviceResponse]:
        """Send a command and await the correlated response.

        Raises ``ResponseError`` if the device returns an error.
        Raises ``asyncio.TimeoutError`` if no response arrives in time.
        """
        cmd_seq = self._allocate_cmd_seq()
        assert cmd_seq not in self._resp_waiting, "duplicate cmd_seq"

        packet = self._wrap_host_payload(cmd_seq, payload)

        fut: asyncio.Future[Optional[TDeviceResponse]] = (
            asyncio.get_running_loop().create_future()
        )
        self._resp_waiting[cmd_seq] = fut
        self._write_message(packet)
        try:
            return await asyncio.wait_for(asyncio.shield(fut), self.REQUEST_TIMEOUT)
        except asyncio.TimeoutError:
            self._resp_waiting.pop(cmd_seq, None)
            raise

    # --- Receive side ---

    def _dispatch_device_packet(self, packet: TWireDevicePacket) -> None:
        """Route a decoded device packet.  Base implementation treats it as a
        response via ``_unwrap_response`` and resolves the matching future.

        Subclasses (e.g. streaming) may override to intercept other
        messages before delegating to super.
        """
        try:
            cmd_seq, response = self._unwrap_response(packet)
        except ResponseError as e:
            cmd_seq, response = e.cmd_seq, e

        fut = self._resp_waiting.pop(cmd_seq, None)
        if fut is not None:
            if isinstance(response, Exception):
                fut.set_exception(response)
            else:
                fut.set_result(response)
        else:
            logging.warning(
                f"discarding received packet with no waiting cmd_seq={cmd_seq}: {response}"
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
                packet = self._device_packet_type().FromString(message_bytes)  # type: ignore[misc]
            except Exception as e:
                logging.warning(
                    f"discarding bad packet: {len(serial_bytes)} bytes, {serial_bytes!r} - {e}"
                )
                continue
            logging.debug(f"received message {packet}")

            self._dispatch_device_packet(packet)


class OneofPayloadMixin:
    """Mixin providing descriptor-driven ``_wrap_host_payload`` and
    ``_unwrap_response`` for protocols whose host and device packets use
    protobuf oneofs for payload dispatch.

    Subclasses define the host wire packet type, oneof names, and a
    response-container accessor.  The mixin builds accessor-based payload
    maps and provides default wrap/unwrap implementations.
    """

    @staticmethod
    @abc.abstractmethod
    def _host_packet_type() -> Type[pb_message.Message]: ...

    @staticmethod
    @abc.abstractmethod
    def _host_oneof_name() -> str: ...

    @staticmethod
    @abc.abstractmethod
    def _response_oneof_name() -> str: ...

    @staticmethod
    def _response_error_field() -> str:
        return "error"

    @staticmethod
    def _response_container(
        packet: pb_message.Message,
    ) -> Tuple[int, pb_message.Message]:
        """Extract (cmd_seq, container) from a device wire packet.

        The container is the message whose oneof (named by
        ``_response_oneof_name``) holds the payload or error.
        Defaults to ``(packet.cmd_seq, packet)`` for flat layouts.
        Override for nested structures (e.g. PLC DevicePacket.result).
        """
        return packet.cmd_seq, packet

    @staticmethod
    @functools.cache
    def _build_oneof_payload_map(
        packet_class: Type[pb_message.Message],
        oneof_name: str,
    ) -> Dict[
        Type[pb_message.Message], Callable[[pb_message.Message], pb_message.Message]
    ]:
        """Build a {payload_type -> accessor} map from a protobuf oneof.

        Each accessor is a function that, given a wire packet, returns the
        mutable sub-message field for that payload type.
        Skips scalar fields (e.g. string error). Asserts on duplicate types.
        Result is cached per (packet_class, oneof_name).
        """
        sym_db = pb_symbol_database.Default()
        oneof = packet_class.DESCRIPTOR.oneofs_by_name[oneof_name]
        result: Dict[
            Type[pb_message.Message], Callable[[pb_message.Message], pb_message.Message]
        ] = {}
        for field in oneof.fields:
            if field.message_type is None:
                continue
            py_class = sym_db.GetSymbol(field.message_type.full_name)
            assert py_class not in result, f"ambiguous oneof field for {py_class}"
            name = field.name
            result[py_class] = lambda pkt, n=name: getattr(pkt, n)
        return result

    def _wrap_host_payload(
        self, cmd_seq: int, payload: pb_message.Message
    ) -> pb_message.Message:
        host_class = self._host_packet_type()
        payload_map = self._build_oneof_payload_map(host_class, self._host_oneof_name())
        accessor = payload_map.get(type(payload))
        assert accessor is not None, f"unknown payload type {type(payload)}"
        packet = host_class(cmd_seq=cmd_seq)
        accessor(packet).CopyFrom(payload)
        return packet

    def _unwrap_response(
        self, packet: pb_message.Message
    ) -> Tuple[int, Optional[pb_message.Message]]:
        cmd_seq, container = self._response_container(packet)
        error_field = self._response_error_field()
        field = container.WhichOneof(self._response_oneof_name())
        if field == error_field:
            raise ResponseError(getattr(container, error_field), cmd_seq)
        return cmd_seq, getattr(container, field) if field else None


class CobsProtoStreamingProtocol(
    CobsProtoProtocol[
        TWireHostPacket, THostPayload, TWireDevicePacket, TDeviceResponse
    ],
    Generic[
        TWireHostPacket,
        THostPayload,
        TWireDevicePacket,
        TDeviceResponse,
        TDeviceReading,
    ],
):
    """Extension of ``CobsProtoProtocol`` for devices that also send streaming
    (unrequested, asynchronous) readings alongside request/response traffic.

    Provides a reading queue with ``next_reading`` and ``iter_readings``.
    Subclasses override ``_dispatch_device_packet`` to route incoming packets,
    calling ``super()._dispatch_device_packet(packet)`` for responses and
    ``_enqueue_reading(reading)`` for streaming readings.

    Additional type parameter:
        TDeviceReading: The protobuf message type for streaming readings.
    """

    def __init__(self) -> None:
        super().__init__()
        self._reading_queue: asyncio.Queue[TDeviceReading] = asyncio.Queue()

    def _enqueue_reading(self, reading: TDeviceReading) -> None:
        """Add an streaming reading to the queue."""
        self._reading_queue.put_nowait(reading)

    async def next_reading(self) -> TDeviceReading:
        """Return the next streaming reading, blocking if none is available."""
        return await self._reading_queue.get()

    async def iter_readings(self) -> AsyncIterator[TDeviceReading]:
        """Yield streaming readings as they arrive."""
        while True:
            yield await self._reading_queue.get()
