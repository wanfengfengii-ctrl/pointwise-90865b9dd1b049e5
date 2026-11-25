import csv
import io
import sqlite3
from decimal import Decimal

from release.lookup import claim


def initialize_export_store(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS export_events (
            export_id INTEGER PRIMARY KEY,
            report_id TEXT NOT NULL REFERENCES reports(report_id)
        )
    ''')


def export_report(connection: sqlite3.Connection, report_id: str) -> str:
    with connection:
        connection.execute('BEGIN IMMEDIATE')
        report = claim(connection, report_id)
        output = io.StringIO(newline='')
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(('kind', 'report_id', 'batch', 'manifest_id', 'unit', 'total', 'receipt_id',
                         'ordinal', 'position', 'channel', 'value', 'packet_id', 'physical_position'))
        identity = (report.report_id, report.batch, report.manifest_id, report.unit,
                    str(Decimal(report.total)), report.receipt.receipt_id)
        writer.writerow(('summary', *identity, '', '', '', '', '', ''))
        for ordinal, row in enumerate(report.rows, 1):
            writer.writerow(('reading', *identity, ordinal, str(Decimal(row.position)), row.channel,
                             str(Decimal(row.value)), row.origin.packet_id, row.origin.position))
        connection.execute('INSERT INTO export_events (report_id) VALUES (?)', (report.report_id,))
        return output.getvalue()
