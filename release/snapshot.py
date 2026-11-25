import sqlite3
from dataclasses import dataclass

from ledger.rows import ReceivedRow
from ledger.tally import BatchTally
from recorder.packet import Origin, decode_integer
from release.receipt import Receipt, initialize_receipt_store


@dataclass(frozen=True)
class ReportSnapshot:
    report_id: str
    batch: str
    manifest_id: str
    unit: str
    rows: tuple[ReceivedRow, ...]
    total: int
    receipt: Receipt


class ReportStore:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def initialize(self) -> None:
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                batch TEXT NOT NULL UNIQUE REFERENCES runs(batch),
                manifest_id TEXT NOT NULL UNIQUE REFERENCES manifests(manifest_id),
                unit TEXT NOT NULL,
                total TEXT NOT NULL
            )
        ''')
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS report_lines (
                report_id TEXT NOT NULL REFERENCES reports(report_id),
                ordinal INTEGER NOT NULL CHECK (ordinal > 0),
                line TEXT NOT NULL,
                channel TEXT NOT NULL,
                value TEXT NOT NULL,
                packet_id TEXT NOT NULL,
                physical_position INTEGER NOT NULL CHECK (physical_position > 0),
                PRIMARY KEY (report_id, ordinal)
            )
        ''')
        initialize_receipt_store(self.connection)

    def get(self, report_id: str) -> ReportSnapshot | None:
        head = self.connection.execute('''
            SELECT r.batch, r.manifest_id, r.unit, r.total, a.receipt_id
            FROM reports AS r JOIN receipts AS a ON a.report_id = r.report_id
            WHERE r.report_id = ?
        ''', (report_id,)).fetchone()
        if head is None:
            return None
        batch, manifest_id, unit, total, receipt_id = head
        details = self.connection.execute('''
            SELECT line, channel, value, packet_id, physical_position FROM report_lines
            WHERE report_id = ? ORDER BY ordinal
        ''', (report_id,))
        rows = tuple(ReceivedRow(batch, decode_integer(line), channel, decode_integer(value),
                                 Origin(packet_id, position))
                     for line, channel, value, packet_id, position in details)
        return ReportSnapshot(report_id, batch, manifest_id, unit, rows, decode_integer(total),
                              Receipt(receipt_id, report_id, batch, manifest_id))

    def get_for_manifest(self, manifest_id: str) -> ReportSnapshot | None:
        row = self.connection.execute('SELECT report_id FROM reports WHERE manifest_id = ?',
                                      (manifest_id,)).fetchone()
        return self.get(row[0]) if row is not None else None

    def report_id_for_batch(self, batch: str) -> str | None:
        row = self.connection.execute('''
            SELECT r.report_id FROM reports AS r JOIN receipts AS a ON a.report_id = r.report_id
            WHERE r.batch = ?
        ''', (batch,)).fetchone()
        return row[0] if row is not None else None

    def insert(self, report_id: str, tally: BatchTally) -> None:
        self.connection.execute('''
            INSERT INTO reports (report_id, batch, manifest_id, unit, total) VALUES (?, ?, ?, ?, ?)
        ''', (report_id, tally.batch, tally.manifest_id, tally.unit, hex(tally.total)))
        self.connection.executemany('''
            INSERT INTO report_lines (report_id, ordinal, line, channel, value, packet_id, physical_position)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', ((report_id, ordinal, hex(row.position), row.channel, hex(row.value),
               row.origin.packet_id, row.origin.position) for ordinal, row in enumerate(tally.rows, 1)))
