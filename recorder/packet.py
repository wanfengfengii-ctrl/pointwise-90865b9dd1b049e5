import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Origin:
    packet_id: str
    position: int


@dataclass(frozen=True)
class PacketRow:
    batch: str
    position: int
    channel: str
    value: int
    origin: Origin


@dataclass(frozen=True)
class Packet:
    packet_id: str
    sequence: int
    rows: tuple[PacketRow, ...]


def validate_packet_id(packet_id: str) -> None:
    if (not isinstance(packet_id, str) or not packet_id or packet_id != packet_id.strip()
            or not packet_id.isprintable()):
        raise ValueError('invalid packet id')


def decode_integer(encoded: str) -> int:
    if type(encoded) is not str:
        raise ValueError('encoded integer must be hexadecimal text')
    value = int(encoded, 16)
    if hex(value) != encoded:
        raise ValueError('encoded integer is not canonical')
    return value


def validate_row(row: PacketRow) -> None:
    if not isinstance(row, PacketRow) or not isinstance(row.origin, Origin):
        raise ValueError('invalid packet row or origin')
    if not isinstance(row.batch, str) or not row.batch or not isinstance(row.channel, str) or not row.channel:
        raise ValueError('missing batch or channel')
    if type(row.position) is not int or row.position < 1 or type(row.value) is not int:
        raise ValueError('invalid row position or integer reading')
    validate_packet_id(row.origin.packet_id)
    if type(row.origin.position) is not int or row.origin.position < 1:
        raise ValueError('invalid physical position')


def _member_fields(row: PacketRow) -> list:
    return [row.batch, hex(row.position), row.channel, hex(row.value),
            row.origin.packet_id, row.origin.position]


def member_digest(row: PacketRow) -> str:
    return hashlib.sha256(_serialize(_member_fields(row))).hexdigest()


def _serialize(value) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(',', ':')).encode('ascii')


def encode_packet(packet: Packet) -> bytes:
    validate_packet_id(packet.packet_id)
    if type(packet.sequence) is not int or packet.sequence < 1 or not packet.rows:
        raise ValueError('packet requires a positive sequence and at least one row')
    for index, row in enumerate(packet.rows, 1):
        validate_row(row)
        if row.origin != Origin(packet.packet_id, index):
            raise ValueError('physical origin does not match packet position')
    return _serialize({'version': 1, 'packet_id': packet.packet_id, 'sequence': packet.sequence,
                       'rows': [_member_fields(row) for row in packet.rows]})


def _decode_member(fields: list) -> PacketRow:
    if not isinstance(fields, list) or len(fields) != 6:
        raise ValueError('invalid packet member fields')
    batch, position, channel, value, packet_id, physical_position = fields
    return PacketRow(batch, decode_integer(position), channel, decode_integer(value),
                     Origin(packet_id, physical_position))


def decode_packet(payload: bytes) -> Packet:
    document = json.loads(payload)
    if not isinstance(document, dict) or set(document) != {'version', 'packet_id', 'sequence', 'rows'}:
        raise ValueError('invalid packet document')
    if type(document['version']) is not int or document['version'] != 1:
        raise ValueError('unsupported packet version')
    if not isinstance(document['rows'], list):
        raise ValueError('packet rows must be a list')
    packet = Packet(document['packet_id'], document['sequence'],
                    tuple(_decode_member(fields) for fields in document['rows']))
    encode_packet(packet)
    return packet
