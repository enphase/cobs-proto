from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HostPacket(_message.Message):
    __slots__ = ("ping", "set_value")
    PING_FIELD_NUMBER: _ClassVar[int]
    SET_VALUE_FIELD_NUMBER: _ClassVar[int]
    ping: Ping
    set_value: SetValue
    def __init__(
        self,
        ping: _Optional[_Union[Ping, _Mapping]] = ...,
        set_value: _Optional[_Union[SetValue, _Mapping]] = ...,
    ) -> None: ...

class Ping(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SetValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class DevicePacket(_message.Message):
    __slots__ = ("error", "pong", "value_status", "reading")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    PONG_FIELD_NUMBER: _ClassVar[int]
    VALUE_STATUS_FIELD_NUMBER: _ClassVar[int]
    READING_FIELD_NUMBER: _ClassVar[int]
    error: str
    pong: Pong
    value_status: ValueStatus
    reading: DeviceReading
    def __init__(
        self,
        error: _Optional[str] = ...,
        pong: _Optional[_Union[Pong, _Mapping]] = ...,
        value_status: _Optional[_Union[ValueStatus, _Mapping]] = ...,
        reading: _Optional[_Union[DeviceReading, _Mapping]] = ...,
    ) -> None: ...

class Pong(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ValueStatus(_message.Message):
    __slots__ = ("current_value",)
    CURRENT_VALUE_FIELD_NUMBER: _ClassVar[int]
    current_value: int
    def __init__(self, current_value: _Optional[int] = ...) -> None: ...

class DeviceReading(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...
