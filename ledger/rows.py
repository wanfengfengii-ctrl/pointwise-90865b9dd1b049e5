import sqlite3
from dataclasses import dataclass

from recorder.packet import Origin, PacketRow, decode_integer


@dataclass(frozen=True)
class ReceivedRow:
    batch: str
    position: int
    channel: str
    value: int
    origin: Origin

    def as_packet_row(self) -> PacketRow:
        return PacketRow(self.batch, self.position, self.channel, self.value, self.origin)


class RowStore:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def initialize(self) -> None:
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS received_rows (
                batch TEXT NOT NULL REFERENCES runs(batch),
                line TEXT NOT NULL,
                channel TEXT NOT NULL,
                value TEXT NOT NULL,
                packet_id TEXT NOT NULL,
                physical_position INTEGER NOT NULL CHECK (physical_position > 0),
                PRIMARY KEY (batch, line),
                UNIQUE (packet_id, physical_position)
            )
        ''')
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS row_conflicts (
                batch TEXT NOT NULL,
                line TEXT NOT NULL,
                proposed_value TEXT NOT NULL,
                proposed_packet TEXT NOT NULL,
                proposed_position INTEGER NOT NULL,
                PRIMARY KEY (batch, line, proposed_value, proposed_packet, proposed_position),
                FOREIGN KEY (batch, line) REFERENCES received_rows(batch, line)
            )
        ''')

    def get(self, batch: str, position: int) -> ReceivedRow | None:
        row = self.connection.execute('''
            SELECT channel, value, packet_id, physical_position FROM received_rows
            WHERE batch = ? AND line = ?
        ''', (batch, hex(position))).fetchone()
        if row is None:
            return None
        channel, value, packet_id, physical_position = row
        return ReceivedRow(batch, position, channel, decode_integer(value), Origin(packet_id, physical_position))

    def insert(self, row: PacketRow) -> ReceivedRow:
        self.connection.execute('''
            INSERT INTO received_rows (batch, line, channel, value, packet_id, physical_position)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (row.batch, hex(row.position), row.channel, hex(row.value), row.origin.packet_id,
              row.origin.position))
        return ReceivedRow(row.batch, row.position, row.channel, row.value, row.origin)

    def record_conflict(self, row: PacketRow) -> None:
        self.connection.execute('''
            INSERT INTO row_conflicts (batch, line, proposed_value, proposed_packet, proposed_position)
            VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
        ''', (row.batch, hex(row.position), hex(row.value), row.origin.packet_id, row.origin.position))
