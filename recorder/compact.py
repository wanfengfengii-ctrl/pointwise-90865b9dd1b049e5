from collections.abc import Sequence

from recorder.device_store import DeviceStore
from recorder.packet import validate_packet_id


def compact_packets(store: DeviceStore, operation_id: str, left_id: str, right_id: str,
                    evidence: Sequence[tuple[str, int, str]]) -> str:
    validate_packet_id(operation_id)
    return store.compact(operation_id, left_id, right_id, evidence)
