from typing import Protocol

from recorder.ack import AckStatus
from recorder.packet import Packet, decode_delivered_packet, validate_packet_id


class RecorderPort(Protocol):
    def read_packet(self, packet_id: str) -> bytes: ...

    def acknowledge(self, packet_id: str) -> AckStatus: ...

    def confirmation_status(self, packet_id: str) -> AckStatus: ...


def fetch_packet(port: RecorderPort, packet_id: str) -> tuple[Packet, bool]:
    """Return the delivered packet and whether the reply carried every member.

    A complete reply uses the original v1 encoding; after a device compaction
    the reply may be a v2 subset containing only the members still retained.
    """
    validate_packet_id(packet_id)
    payload = port.read_packet(packet_id)
    if not isinstance(payload, bytes):
        raise ValueError('recorder payload must be bytes')
    packet, complete = decode_delivered_packet(payload)
    if packet.packet_id != packet_id:
        raise ValueError('recorder returned a different original packet id')
    return packet, complete
