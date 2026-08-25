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
    def _device_packet_type():
        return pb.DevicePacket


class _ExampleStreamingProtocol(
    CobsProtoStreamingProtocol[pb.HostPacket, pb.DevicePacket],
):
    @staticmethod
    def _device_packet_type():
        return pb.DevicePacket

    def _is_streaming_packet(self, packet):
        return packet.HasField("reading")


# --- Cross-language golden vectors (must match Rust tests) ---

# DevicePacket { value_status: ValueStatus { current_value: 42 } }
GOLDEN_DEVICE_FRAME = bytes([0x00, 0x07, 0x1a, 0x02, 0x08, 0x2a, 0x11, 0xed, 0x00])
GOLDEN_DEVICE_PACKET = pb.DevicePacket(value_status=pb.ValueStatus(current_value=42))

# HostPacket { ping: Ping {} }
GOLDEN_HOST_FRAME = bytes([0x00, 0x02, 0x0a, 0x03, 0xef, 0xcb, 0x00])


# --- Wire encoding tests ---


class TestWireEncoding:
    def test_encode_device_frame_matches_golden(self):
        """encode_device_frame produces bytes matching the Rust encoder."""
        assert encode_device_frame(GOLDEN_DEVICE_PACKET) == GOLDEN_DEVICE_FRAME

    def test_encode_host_frame_matches_golden(self):
        """_write_message produces bytes matching the Rust encoder."""
        protocol, transport = make_connected_protocol(_ExampleProtocol())
        protocol._write_message(pb.HostPacket(ping=pb.Ping()))
        written = transport.write.call_args[0][0]
        assert written == GOLDEN_HOST_FRAME


# --- Wire decoding tests ---


class TestWireDecoding:
    def test_decode_golden_frame(self):
        """data_received correctly decodes the golden device frame."""
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        fut = asyncio.get_event_loop().create_future()
        protocol._pending_response = fut
        protocol.data_received(GOLDEN_DEVICE_FRAME)
        assert fut.done()
        assert fut.result() == GOLDEN_DEVICE_PACKET

    def test_crc_mismatch_discards_packet(self):
        """A corrupted frame is silently discarded."""
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        fut = asyncio.get_event_loop().create_future()
        protocol._pending_response = fut
        corrupted = bytearray(GOLDEN_DEVICE_FRAME)
        corrupted[3] ^= 0xFF
        protocol.data_received(bytes(corrupted))
        assert not fut.done()

    def test_partial_frame_delivery(self):
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
    async def test_send_request_and_respond(self):
        protocol, _ = make_connected_protocol(_ExampleProtocol())

        result, _ = await asyncio.gather(
            protocol.send_request(pb.HostPacket(ping=pb.Ping())),
            respond(protocol, GOLDEN_DEVICE_PACKET),
        )
        assert result == GOLDEN_DEVICE_PACKET

    @pytest.mark.asyncio
    async def test_timeout_on_no_response(self):
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        protocol.REQUEST_TIMEOUT = 0.01
        with pytest.raises(asyncio.TimeoutError):
            await protocol.send_request(pb.HostPacket(ping=pb.Ping()))
        assert protocol._pending_response is None

    @pytest.mark.asyncio
    async def test_one_in_flight_enforcement(self):
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        asyncio.ensure_future(protocol.send_request(pb.HostPacket(ping=pb.Ping())))
        await asyncio.sleep(0)
        with pytest.raises(AssertionError, match="only one request"):
            await protocol.send_request(pb.HostPacket(ping=pb.Ping()))

    @pytest.mark.asyncio
    async def test_unrequested_packet_discarded(self):
        """A packet with no pending request is discarded (not raised)."""
        protocol, _ = make_connected_protocol(_ExampleProtocol())
        protocol.data_received(encode_device_frame(GOLDEN_DEVICE_PACKET))


# --- Streaming protocol tests ---


class TestStreaming:
    @pytest.mark.asyncio
    async def test_streaming_packet_goes_to_queue(self):
        protocol, _ = make_connected_protocol(_ExampleStreamingProtocol())
        reading = pb.DeviceReading(value=42)
        protocol.data_received(encode_device_frame(pb.DevicePacket(reading=reading)))

        pkt = await asyncio.wait_for(protocol.next_reading(), timeout=0.1)
        assert pkt.reading == reading

    @pytest.mark.asyncio
    async def test_response_packet_resolves_future(self):
        protocol, _ = make_connected_protocol(_ExampleStreamingProtocol())

        result, _ = await asyncio.gather(
            protocol.send_request(pb.HostPacket(ping=pb.Ping())),
            respond(protocol, GOLDEN_DEVICE_PACKET),
        )
        assert result == GOLDEN_DEVICE_PACKET

    @pytest.mark.asyncio
    async def test_interleaved_streaming_and_response(self):
        protocol, _ = make_connected_protocol(_ExampleStreamingProtocol())
        reading = pb.DeviceReading(value=7)

        async def simulate():
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
    async def test_iter_readings(self):
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
