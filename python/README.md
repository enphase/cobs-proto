# cobs-proto

Host-side COBS + CRC framed protobuf transport for asyncio serial protocols.

Provides an `asyncio.Protocol` implementation with request-response structure and optional
(unrequested) streaming readings.

See the [Rust README](https://github.com/enphase/cobs-proto/blob/main/rust/README.md) for details on the matching device-side implementation.
See the [top-level README](https://github.com/enphase/cobs-proto) for details on the wire format.

This is a pre-v1 release and API stability is not guaranteed.


## Usage

```console
pip install cobs-proto
```

```python
import asyncio
from cobs_proto import CobsProtoProtocol
from your_proto import packet_pb2

# Subclass to specify the decoded packet type
# Request-response only, see CobsProtoStreamingProtocol which adds device streaming data
class MyProtocol(CobsProtoProtocol[packet_pb2.HostPacket, packet_pb2.DevicePacket]):
    @staticmethod
    def _device_packet_type():
        return packet_pb2.DevicePacket

# Connect (via pyserial-asyncio or similar)
transport, protocol = await serial_asyncio.create_serial_connection(
    asyncio.get_running_loop(), lambda: MyProtocol(), port, baudrate=115200,
)

# Send a request and await the response
response = await protocol.send_request(packet_pb2.HostPacket(ping=packet_pb2.Ping()))
```
