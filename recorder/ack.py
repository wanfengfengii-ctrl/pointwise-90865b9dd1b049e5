from enum import Enum

from recorder.device_store import DeviceStore
from recorder.packet import validate_packet_id


class AckStatus(str, Enum):
    RETAINED = 'retained'
    CONFIRMED = 'confirmed'
    UNKNOWN = 'unknown'


class AckReplyLost(ConnectionError):
    pass


def acknowledge_packet(store: DeviceStore, packet_id: str, *, lose_reply: bool = False) -> AckStatus:
    validate_packet_id(packet_id)
    newly_reclaimed, confirmation = store.reclaim(packet_id)
    if newly_reclaimed and lose_reply:
        raise AckReplyLost(packet_id)
    return AckStatus.CONFIRMED if confirmation is not None else AckStatus.UNKNOWN


def query_ack_status(store: DeviceStore, packet_id: str) -> AckStatus:
    validate_packet_id(packet_id)
    known, confirmation = store.packet_confirmation(packet_id)
    if not known:
        return AckStatus.UNKNOWN
    return AckStatus.RETAINED if confirmation is None else AckStatus.CONFIRMED
