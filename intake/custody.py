import hashlib
import sqlite3
from dataclasses import dataclass

from recorder.device_store import PacketIdentityConflict
from recorder.packet import Packet, encode_packet, member_digest


class CustodyError(ValueError):
    pass


@dataclass(frozen=True)
class MemberSlot:
    position: int
    batch: str
    line: int
    digest: str
    secured: bool


@dataclass(frozen=True)
class PacketProgress:
    packet_id: str
    sequence: int
    state: str
    members: tuple[MemberSlot, ...]


class CustodyStore:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def initialize(self) -> None:
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS custody_packets (
                packet_id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL UNIQUE,
                digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('collecting', 'pending', 'confirmed'))
            )
        ''')
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS custody_members (
                packet_id TEXT NOT NULL REFERENCES custody_packets(packet_id),
                position INTEGER NOT NULL CHECK (position > 0),
                batch TEXT NOT NULL REFERENCES runs(batch),
                line TEXT NOT NULL,
                digest TEXT NOT NULL,
                secured INTEGER NOT NULL DEFAULT 0 CHECK (secured IN (0, 1)),
                PRIMARY KEY (packet_id, position)
            )
        ''')

    def open_packet(self, packet: Packet, *, complete: bool = True) -> PacketProgress:
        if complete:
            digest = hashlib.sha256(encode_packet(packet)).hexdigest()
        stored = self.connection.execute('''
            SELECT packet_id, digest FROM custody_packets WHERE packet_id = ? OR sequence = ?
        ''', (packet.packet_id, packet.sequence)).fetchall()
        if stored:
            if complete:
                if stored != [(packet.packet_id, digest)]:
                    raise PacketIdentityConflict(packet.packet_id)
            else:
                # A post-compaction reply only carries the members the device
                # still retains; every delivered member must match the index.
                indexed = {row[0]: row[1:] for row in self.connection.execute('''
                    SELECT position, batch, line, digest FROM custody_members
                    WHERE packet_id = ?
                ''', (packet.packet_id,)).fetchall()}
                if not indexed or not stored or stored[0][0] != packet.packet_id:
                    raise PacketIdentityConflict(packet.packet_id)
                for row in packet.rows:
                    slot = indexed.get(row.origin.position)
                    if slot is None or slot != (row.batch, hex(row.position), member_digest(row)):
                        raise PacketIdentityConflict(packet.packet_id)
        elif not complete:
            raise PacketIdentityConflict(packet.packet_id)
        else:
            self.connection.execute('''
                INSERT INTO custody_packets (packet_id, sequence, digest, state)
                VALUES (?, ?, ?, 'collecting')
            ''', (packet.packet_id, packet.sequence, digest))
            self.connection.executemany('''
                INSERT INTO custody_members (packet_id, position, batch, line, digest)
                VALUES (?, ?, ?, ?, ?)
            ''', ((packet.packet_id, row.origin.position, row.batch, hex(row.position), member_digest(row))
                  for row in packet.rows))
        return self.require_progress(packet.packet_id)

    def get_progress(self, packet_id: str) -> PacketProgress | None:
        records = self.connection.execute('''
            SELECT p.sequence, p.state, m.position, m.batch, m.line, m.digest, m.secured
            FROM custody_packets AS p
            LEFT JOIN custody_members AS m ON m.packet_id = p.packet_id
            WHERE p.packet_id = ? ORDER BY m.position
        ''', (packet_id,)).fetchall()
        if not records:
            return None
        sequence, state, *_ = records[0]
        if records[0][2] is None:
            raise CustodyError(f'missing member index for {packet_id}')
        return PacketProgress(packet_id, sequence, state, tuple(
            MemberSlot(position, batch, int(line, 16), digest, bool(secured))
            for _, _, position, batch, line, digest, secured in records
        ))

    def require_progress(self, packet_id: str) -> PacketProgress:
        progress = self.get_progress(packet_id)
        if progress is None:
            raise CustodyError(f'no custody index for {packet_id}')
        return progress

    def record_member_evidence(self, packet_id: str, position: int, batch: str, line: int, digest: str) -> None:
        updated = self.connection.execute('''
            UPDATE custody_members SET secured = 1
            WHERE packet_id = ? AND position = ? AND batch = ? AND line = ? AND digest = ?
        ''', (packet_id, position, batch, hex(line), digest))
        if updated.rowcount != 1:
            raise CustodyError(f'original member does not match custody index for {packet_id}')

    def set_ack_state(self, packet_id: str, state: str) -> str:
        current = self.require_progress(packet_id).state
        if current == state or (current == 'confirmed' and state == 'pending'):
            return current
        if (current, state) not in {('collecting', 'pending'), ('pending', 'confirmed')}:
            raise CustodyError(f'invalid acknowledgement transition for {packet_id}')
        updated = self.connection.execute('''
            UPDATE custody_packets SET state = ? WHERE packet_id = ? AND state = ?
        ''', (state, packet_id, current))
        if updated.rowcount != 1:
            raise CustodyError(f'acknowledgement state changed for {packet_id}')
        return state
