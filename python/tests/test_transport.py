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

"""Tests for cobs_proto transport using the shared test_packet proto."""

import asyncio
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from cobs_proto import CobsProtoProtocol, CobsProtoStreamingProtocol
from cobs_proto import encode_device_frame, make_connected_protocol, respond
from .proto import test_packet_pb2 as pb

# --- Concrete test protocol subclasses ---


class _ExampleProtocol(
    CobsProtoProtocol[pb.HostPacket, pb.DevicePacket],
):
    @staticmethod
    def _device_packet_type() -> type[pb.DevicePacket]:
        return pb.DevicePacket


class _ExampleStreamingProtocol(
    CobsProtoStreamingProtocol[pb.HostPacket, pb.DevicePacket],
):
    @staticmethod
    def _device_packet_type() -> type[pb.DevicePacket]:
        return pb.DevicePacket

    def _is_streaming_packet(self, packet: pb.DevicePacket) -> bool:
        return packet.HasField("reading")


# --- Cross-language golden vectors ---
# The .bin files are raw golden wire frames shared with the Rust tests.

_VECTORS_DIR = Path(__file__).resolve().parents[2] / "test_proto"

GOLDEN_DEVICE_PACKET = pb.DevicePacket(value_status=pb.ValueStatus(current_value=42))
GOLDEN_DEVICE_FRAME = (_VECTORS_DIR / "device_value_status_42.bin").read_bytes()

# HostPacket { ping: Ping {} }
GOLDEN_HOST_FRAME = (_VECTORS_DIR / "host_ping.bin").read_bytes()


# --- Wire encoding tests ---


class TestWireEncoding:
    def test_encode_device_frame_matches_golden(self) -> None:
        """encode_device_frame produces bytes matching the Rust encoder."""
        assert encode_device_frame(GOLDEN_DEVICE_PACKET) == GOLDEN_DEVICE_FRAME

    def test_encode_host_frame_matches_golden(self) -> None:
        """_write_message produces bytes matching the Rust encoder."""
        protocol, transport = make_connected_protocol(_ExampleProtocol())
        protocol._write_message(pb.HostPacket(ping=pb.Ping()))
        written = transport.write.call_args[0][0]
        assert written == GOLDEN_HOST_FRAME


# --- Wire decoding tests ---


class TestWireDecoding:
    def test_decode_golden_frame(self) -> None:
        """data_received correctly decodes the golden device frame."""
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        fut = asyncio.get_event_loop().create_future()
        protocol._pending_response = fut
        protocol.data_received(GOLDEN_DEVICE_FRAME)
        assert fut.done()
        assert fut.result() == GOLDEN_DEVICE_PACKET

    def test_crc_mismatch_discards_packet(self) -> None:
        """A corrupted frame is silently discarded."""
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        fut = asyncio.get_event_loop().create_future()
        protocol._pending_response = fut
        corrupted = bytearray(GOLDEN_DEVICE_FRAME)
        corrupted[3] ^= 0xFF
        protocol.data_received(bytes(corrupted))
        assert not fut.done()

    def test_partial_frame_delivery(self) -> None:
        """A frame split across two data_received calls still decodes."""
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        fut = asyncio.get_event_loop().create_future()
        protocol._pending_response = fut
        mid = len(GOLDEN_DEVICE_FRAME) // 2
        protocol.data_received(GOLDEN_DEVICE_FRAME[:mid])
        assert not fut.done()
        protocol.data_received(GOLDEN_DEVICE_FRAME[mid:])
        assert fut.done()
        assert fut.result() == GOLDEN_DEVICE_PACKET


# --- Request-response lifecycle ---


class TestRequestResponse:
    @pytest.mark.asyncio
    async def test_send_request_and_respond(self) -> None:
        protocol, _ = make_connected_protocol(_ExampleProtocol())

        result, _ = await asyncio.gather(
            protocol.send_request(pb.HostPacket(ping=pb.Ping())),
            respond(protocol, GOLDEN_DEVICE_PACKET),
        )
        assert result == GOLDEN_DEVICE_PACKET

    @pytest.mark.asyncio
    async def test_timeout_on_no_response(self) -> None:
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        protocol.REQUEST_TIMEOUT = 0.01
        with pytest.raises(asyncio.TimeoutError):
            await protocol.send_request(pb.HostPacket(ping=pb.Ping()))
        assert protocol._pending_response is None

    @pytest.mark.asyncio
    async def test_one_in_flight_enforcement(self) -> None:
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        asyncio.ensure_future(protocol.send_request(pb.HostPacket(ping=pb.Ping())))
        await asyncio.sleep(0)
        with pytest.raises(AssertionError, match="only one request"):
            await protocol.send_request(pb.HostPacket(ping=pb.Ping()))

    @pytest.mark.asyncio
    async def test_unrequested_packet_discarded(self) -> None:
        """A packet with no pending request is discarded (not raised)."""
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        protocol.data_received(encode_device_frame(GOLDEN_DEVICE_PACKET))


# --- Streaming protocol tests ---


class TestStreaming:
    @pytest.mark.asyncio
    async def test_streaming_packet_goes_to_queue(self) -> None:
        protocol, _ = make_connected_protocol(_ExampleStreamingProtocol())
        reading = pb.DeviceReading(value=42)
        protocol.data_received(encode_device_frame(pb.DevicePacket(reading=reading)))

        pkt = await asyncio.wait_for(protocol.next_reading(), timeout=0.1)
        assert pkt.reading == reading

    @pytest.mark.asyncio
    async def test_response_packet_resolves_future(self) -> None:
        protocol, _ = make_connected_protocol(_ExampleStreamingProtocol())

        result, _ = await asyncio.gather(
            protocol.send_request(pb.HostPacket(ping=pb.Ping())),
            respond(protocol, GOLDEN_DEVICE_PACKET),
        )
        assert result == GOLDEN_DEVICE_PACKET

    @pytest.mark.asyncio
    async def test_interleaved_streaming_and_response(self) -> None:
        protocol, _ = make_connected_protocol(_ExampleStreamingProtocol())
        reading = pb.DeviceReading(value=7)

        async def simulate() -> None:
            await respond(protocol, pb.DevicePacket(reading=reading))
            await respond(protocol, GOLDEN_DEVICE_PACKET)

        result, _ = await asyncio.gather(
            protocol.send_request(pb.HostPacket(ping=pb.Ping())),
            simulate(),
        )
        assert result == GOLDEN_DEVICE_PACKET

        pkt = await asyncio.wait_for(protocol.next_reading(), timeout=0.1)
        assert pkt.reading == reading

    @pytest.mark.asyncio
    async def test_iter_readings(self) -> None:
        protocol, _ = make_connected_protocol(_ExampleStreamingProtocol())
        readings = [pb.DeviceReading(value=i) for i in range(3)]
        for r in readings:
            protocol.data_received(encode_device_frame(pb.DevicePacket(reading=r)))

        collected = []
        async for pkt in protocol.iter_readings():
            collected.append(pkt.reading)
            if len(collected) == 3:
                break
        assert collected == readings
