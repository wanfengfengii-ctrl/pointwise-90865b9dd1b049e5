import hashlib
import sqlite3
from collections.abc import Callable, Sequence

from recorder.packet import (PacketRow, decode_delivered_packet, encode_compacted_packet,
                             member_digest, validate_packet_id)


class BufferFull(RuntimeError):
    pass


class PacketUnavailable(LookupError):
    pass


class PacketIdentityConflict(ValueError):
    pass


class DeviceStore:
    def __init__(self, connection: sqlite3.Connection, capacity: int | None = None,
                 compaction: bool | None = None):
        if capacity is not None and (type(capacity) is not int or not 1 <= capacity <= 2**63 - 1):
            raise ValueError('capacity must be a positive SQLite integer')
        if compaction is not None and type(compaction) is not bool:
            raise ValueError('compaction must be a boolean flag')
        self.connection = connection
        self.requested_capacity = capacity
        self.requested_compaction = compaction

    def initialize(self) -> None:
        with self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            self.connection.execute('''
                CREATE TABLE IF NOT EXISTS device_settings (
                    slot INTEGER PRIMARY KEY CHECK (slot = 1),
                    capacity INTEGER NOT NULL CHECK (capacity > 0),
                    compaction INTEGER NOT NULL DEFAULT 0 CHECK (compaction IN (0, 1))
                )
            ''')
            columns = {
                row[1] for row in self.connection.execute('PRAGMA table_info(device_settings)').fetchall()
            }
            if 'compaction' not in columns:
                self.connection.execute(
                    'ALTER TABLE device_settings ADD COLUMN compaction INTEGER NOT NULL DEFAULT 0 '
                    'CHECK (compaction IN (0, 1))')
            self.connection.execute('''
                INSERT INTO device_settings (slot, capacity, compaction) VALUES (1, ?, ?)
                ON CONFLICT(slot) DO NOTHING
            ''', (self.requested_capacity if self.requested_capacity is not None else 4,
                  1 if self.requested_compaction else 0))
            if self.requested_capacity is not None and self.requested_capacity != self._capacity():
                raise ValueError('configured capacity differs from the existing recorder')
            if self.requested_compaction is not None and self.requested_compaction != self.compaction_enabled():
                raise ValueError('configured compaction mode differs from the existing recorder')
            self.connection.execute('''
                CREATE TABLE IF NOT EXISTS device_packets (
                    sequence INTEGER PRIMARY KEY,
                    packet_id TEXT NOT NULL UNIQUE,
                    digest TEXT NOT NULL,
                    payload BLOB,
                    confirmation INTEGER UNIQUE,
                    slot INTEGER,
                    compact_op TEXT,
                    CHECK ((payload IS NULL) = (confirmation IS NOT NULL)),
                    CHECK (slot IS NULL OR slot > 0)
                )
            ''')
            packet_columns = {
                row[1] for row in self.connection.execute('PRAGMA table_info(device_packets)').fetchall()
            }
            if 'slot' not in packet_columns:
                self.connection.execute('ALTER TABLE device_packets ADD COLUMN slot INTEGER')
            if 'compact_op' not in packet_columns:
                self.connection.execute('ALTER TABLE device_packets ADD COLUMN compact_op TEXT')
            self.connection.execute('''
                UPDATE device_packets SET slot = sequence WHERE slot IS NULL AND payload IS NOT NULL
            ''')
            self.connection.execute('''
                CREATE TABLE IF NOT EXISTS device_compactions (
                    operation_id TEXT PRIMARY KEY,
                    left_id TEXT NOT NULL,
                    right_id TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (outcome IN ('compacted', 'ineligible')),
                    shared_slot INTEGER,
                    CHECK ((outcome = 'compacted') = (shared_slot IS NOT NULL))
                )
            ''')

    def _settings(self) -> tuple[int, int]:
        return self.connection.execute(
            'SELECT capacity, compaction FROM device_settings WHERE slot = 1').fetchone()

    def _capacity(self) -> int:
        return self._settings()[0]

    def compaction_enabled(self) -> bool:
        return bool(self._settings()[1])

    def _occupied_slots(self) -> set[int]:
        return {row[0] for row in self.connection.execute(
            'SELECT DISTINCT slot FROM device_packets WHERE payload IS NOT NULL AND slot IS NOT NULL')}

    def append(self, packet_id: str, encode_at_sequence: Callable[[int], bytes]) -> bytes:
        with self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            existing = self.connection.execute('''
                SELECT sequence, digest, payload FROM device_packets WHERE packet_id = ?
            ''', (packet_id,)).fetchone()
            if existing is not None:
                sequence, known_digest, stored_payload = existing
                if hashlib.sha256(encode_at_sequence(sequence)).hexdigest() != known_digest:
                    raise PacketIdentityConflict(packet_id)
                if stored_payload is None:
                    raise PacketUnavailable(packet_id)
                return stored_payload
            used, last_sequence = self.connection.execute('''
                SELECT COUNT(payload), COALESCE(MAX(sequence), 0) FROM device_packets
            ''').fetchone()
            if self.compaction_enabled():
                occupied = self._occupied_slots()
                if len(occupied) >= self._capacity():
                    raise BufferFull(packet_id)
                slot = next(candidate for candidate in range(1, self._capacity() + 1)
                            if candidate not in occupied)
            else:
                if used >= self._capacity():
                    raise BufferFull(packet_id)
                slot = None
            sequence = last_sequence + 1
            payload = encode_at_sequence(sequence)
            self.connection.execute('''
                INSERT INTO device_packets (sequence, packet_id, digest, payload, slot)
                VALUES (?, ?, ?, ?, ?)
            ''', (sequence, packet_id, hashlib.sha256(payload).hexdigest(), payload, slot))
            return payload

    def load_payload(self, packet_id: str) -> bytes | None:
        row = self.connection.execute(
            'SELECT payload FROM device_packets WHERE packet_id = ?', (packet_id,)
        ).fetchone()
        return row[0] if row is not None else None

    def reclaim(self, packet_id: str) -> tuple[bool, int | None]:
        with self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            row = self.connection.execute(
                'SELECT confirmation, slot FROM device_packets WHERE packet_id = ?', (packet_id,)
            ).fetchone()
            if row is None:
                return False, None
            if row[0] is not None:
                return False, row[0]
            confirmation = self.read_position() + 1
            shared_slot = row[1]
            if shared_slot is not None:
                still_shared = self.connection.execute('''
                    SELECT 1 FROM device_packets
                    WHERE packet_id != ? AND slot = ? AND payload IS NOT NULL LIMIT 1
                ''', (packet_id, shared_slot)).fetchone()
                if still_shared:
                    # The compacted partner still occupies the shared slot.
                    self.connection.execute('''
                        UPDATE device_packets SET payload = NULL, confirmation = ?
                        WHERE packet_id = ?
                    ''', (confirmation, packet_id))
                    return True, confirmation
            self.connection.execute('''
                UPDATE device_packets SET payload = NULL, confirmation = ?, slot = NULL
                WHERE packet_id = ?
            ''', (confirmation, packet_id))
            return True, confirmation

    def packet_confirmation(self, packet_id: str) -> tuple[bool, int | None]:
        row = self.connection.execute(
            'SELECT confirmation FROM device_packets WHERE packet_id = ?', (packet_id,)
        ).fetchone()
        return (True, row[0]) if row is not None else (False, None)

    def lookup_confirmation(self, packet_id: str) -> int | None:
        return self.packet_confirmation(packet_id)[1]

    def read_position(self) -> int:
        return self.connection.execute('SELECT COALESCE(MAX(confirmation), 0) FROM device_packets').fetchone()[0]

    def compact(self, operation_id: str, left_id: str, right_id: str,
                evidence: Sequence[tuple[str, int, str]]) -> str:
        validate_packet_id(operation_id)
        validate_packet_id(left_id)
        validate_packet_id(right_id)
        if left_id == right_id:
            raise ValueError('compaction requires two distinct packets')
        if not isinstance(evidence, Sequence) or isinstance(evidence, (str, bytes)):
            raise ValueError('compaction evidence must be a sequence of tuples')
        normalized = []
        for item in evidence:
            if not isinstance(item, tuple) or len(item) != 3:
                raise ValueError('each evidence item must be (packet_id, physical_position, digest)')
            packet_id, physical_position, digest = item
            validate_packet_id(packet_id)
            if type(physical_position) is not int or physical_position < 1:
                raise ValueError('evidence physical position must be a positive integer')
            if type(digest) is not str or not digest:
                raise ValueError('evidence digest must be non-empty text')
            normalized.append((packet_id, physical_position, digest))
        with self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            prior = self.connection.execute('''
                SELECT outcome, left_id, right_id, shared_slot FROM device_compactions
                WHERE operation_id = ?
            ''', (operation_id,)).fetchone()
            if prior is not None:
                outcome, stored_left, stored_right, shared_slot = prior
                if (stored_left, stored_right) != (left_id, right_id):
                    raise ValueError('operation id is already bound to a different ordered packet pair')
                return outcome

            def finish_ineligible() -> str:
                self.connection.execute('''
                    INSERT INTO device_compactions (operation_id, left_id, right_id, outcome, shared_slot)
                    VALUES (?, ?, ?, 'ineligible', NULL)
                ''', (operation_id, left_id, right_id))
                return 'ineligible'

            if not self.compaction_enabled():
                return finish_ineligible()
            sides = self._load_compaction_sides(left_id, right_id)
            if sides is None:
                return finish_ineligible()
            left_rows, right_rows, left_slot, right_slot = sides
            removed, trimmed = self._match_evidence(normalized, left_id, left_rows, right_id, right_rows)
            if removed is None:
                return finish_ineligible()
            shared_slot = min(left_slot, right_slot)
            self.connection.execute('''
                INSERT INTO device_compactions (operation_id, left_id, right_id, outcome, shared_slot)
                VALUES (?, ?, ?, 'compacted', ?)
            ''', (operation_id, left_id, right_id, shared_slot))
            for packet_id, rows, slot in ((left_id, trimmed[0], shared_slot),
                                          (right_id, trimmed[1], shared_slot)):
                payload = encode_compacted_packet(packet_id, self._sequence_of(packet_id), rows)
                self.connection.execute('''
                    UPDATE device_packets SET payload = ?, slot = ?, compact_op = ?
                    WHERE packet_id = ?
                ''', (payload, slot, operation_id, packet_id))
            return 'compacted'

    def _sequence_of(self, packet_id: str) -> int:
        return self.connection.execute(
            'SELECT sequence FROM device_packets WHERE packet_id = ?', (packet_id,)).fetchone()[0]

    def _load_compaction_sides(self, left_id: str, right_id: str
                               ) -> tuple[tuple[PacketRow, ...], tuple[PacketRow, ...], int, int] | None:
        rows_by_packet = {}
        slots = {}
        for packet_id in (left_id, right_id):
            record = self.connection.execute('''
                SELECT payload, slot, compact_op FROM device_packets WHERE packet_id = ?
            ''', (packet_id,)).fetchone()
            if record is None or record[0] is None or record[1] is None or record[2] is not None:
                return None
            payload, slot, _ = record
            decoded, complete = decode_delivered_packet(payload)
            if not complete:
                return None
            if len(decoded.rows) != 2:
                return None
            rows_by_packet[packet_id] = decoded.rows
            slots[packet_id] = slot
        if slots[left_id] == slots[right_id]:
            return None
        return rows_by_packet[left_id], rows_by_packet[right_id], slots[left_id], slots[right_id]

    @staticmethod
    def _match_evidence(evidence: list[tuple[str, int, str]], left_id: str,
                        left_rows: tuple[PacketRow, ...], right_id: str,
                        right_rows: tuple[PacketRow, ...]
                        ) -> tuple[set[tuple[str, int]] | None,
                                   tuple[tuple[PacketRow, ...], tuple[PacketRow, ...]]]:
        if len(evidence) != 2:
            return None, ((), ())
        members = {(left_id, row.origin.position, member_digest(row)): row for row in left_rows}
        members.update({(right_id, row.origin.position, member_digest(row)): row
                        for row in right_rows})
        # Each persistent receipt must match a retained member: same original
        # packet, physical position and member digest. One member per packet is
        # removed, leaving exactly one waiting reading in each side.
        if any(item not in members for item in evidence):
            return None, ((), ())
        removed_packets = {packet_id for packet_id, _, _ in evidence}
        if removed_packets != {left_id, right_id}:
            return None, ((), ())
        removed = {(packet_id, position) for packet_id, position, _ in evidence}
        trimmed_sides = []
        for packet_id, rows in ((left_id, left_rows), (right_id, right_rows)):
            kept = tuple(row for row in rows if (packet_id, row.origin.position) not in removed)
            if not kept or len(kept) != len(rows) - 1:
                return None, ((), ())
            trimmed_sides.append(kept)
        return removed, (trimmed_sides[0], trimmed_sides[1])
