import hashlib
import sqlite3
from collections.abc import Callable


class BufferFull(RuntimeError):
    pass


class PacketUnavailable(LookupError):
    pass


class PacketIdentityConflict(ValueError):
    pass


class DeviceStore:
    def __init__(self, connection: sqlite3.Connection, capacity: int | None = None):
        if capacity is not None and (type(capacity) is not int or not 1 <= capacity <= 2**63 - 1):
            raise ValueError('capacity must be a positive SQLite integer')
        self.connection = connection
        self.requested_capacity = capacity

    def initialize(self) -> None:
        with self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            self.connection.execute('''
                CREATE TABLE IF NOT EXISTS device_settings (
                    slot INTEGER PRIMARY KEY CHECK (slot = 1),
                    capacity INTEGER NOT NULL CHECK (capacity > 0)
                )
            ''')
            self.connection.execute('''
                CREATE TABLE IF NOT EXISTS device_packets (
                    sequence INTEGER PRIMARY KEY,
                    packet_id TEXT NOT NULL UNIQUE,
                    digest TEXT NOT NULL,
                    payload BLOB,
                    confirmation INTEGER UNIQUE,
                    CHECK ((payload IS NULL) = (confirmation IS NOT NULL))
                )
            ''')
            self.connection.execute('''
                INSERT INTO device_settings (slot, capacity) VALUES (1, ?)
                ON CONFLICT(slot) DO NOTHING
            ''', (self.requested_capacity if self.requested_capacity is not None else 4,))
            if self.requested_capacity is not None and self.requested_capacity != self._capacity():
                raise ValueError('configured capacity differs from the existing recorder')

    def _capacity(self) -> int:
        return self.connection.execute('SELECT capacity FROM device_settings WHERE slot = 1').fetchone()[0]

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
            if used >= self._capacity():
                raise BufferFull(packet_id)
            sequence = last_sequence + 1
            payload = encode_at_sequence(sequence)
            self.connection.execute('''
                INSERT INTO device_packets (sequence, packet_id, digest, payload) VALUES (?, ?, ?, ?)
            ''', (sequence, packet_id, hashlib.sha256(payload).hexdigest(), payload))
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
                'SELECT confirmation FROM device_packets WHERE packet_id = ?', (packet_id,)
            ).fetchone()
            if row is None:
                return False, None
            if row[0] is not None:
                return False, row[0]
            confirmation = self.read_position() + 1
            self.connection.execute('''
                UPDATE device_packets SET payload = NULL, confirmation = ? WHERE packet_id = ?
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
