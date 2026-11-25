from typing import Protocol

from recorder.ack import AckStatus
from recorder.packet import Packet, decode_packet, validate_packet_id


class RecorderPort(Protocol):
    def read_packet(self, packet_id: str) -> bytes: ...

    def acknowledge(self, packet_id: str) -> AckStatus: ...

    def confirmation_status(self, packet_id: str) -> AckStatus: ...


def fetch_packet(port: RecorderPort, packet_id: str) -> Packet:
    validate_packet_id(packet_id)
    payload = port.read_packet(packet_id)
    if not isinstance(payload, bytes):
        raise ValueError('recorder payload must be bytes')
    packet = decode_packet(payload)
    if packet.packet_id != packet_id:
        raise ValueError('recorder returned a different original packet id')
    return packet
