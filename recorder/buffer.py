from recorder.device_store import BufferFull as BufferFull
from recorder.device_store import DeviceStore, PacketUnavailable
from recorder.packet import Packet, PacketRow, decode_packet, encode_packet, validate_packet_id


def retain_packet(store: DeviceStore, packet_id: str, rows: tuple[PacketRow, ...]) -> Packet:
    validate_packet_id(packet_id)

    def payload_at(sequence: int) -> bytes:
        return encode_packet(Packet(packet_id, sequence, rows))

    return decode_packet(store.append(packet_id, payload_at))


def read_packet(store: DeviceStore, packet_id: str) -> bytes:
    validate_packet_id(packet_id)
    payload = store.load_payload(packet_id)
    if payload is None:
        raise PacketUnavailable(packet_id)
    return payload
