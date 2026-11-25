import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Receipt:
    receipt_id: str
    report_id: str
    batch: str
    manifest_id: str


def initialize_receipt_store(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS receipts (
            receipt_id TEXT PRIMARY KEY,
            report_id TEXT NOT NULL UNIQUE REFERENCES reports(report_id)
        )
    ''')


def issue_first_receipt(connection: sqlite3.Connection, report_id: str, batch: str, manifest_id: str) -> Receipt:
    receipt = Receipt(f'receipt:{report_id}', report_id, batch, manifest_id)
    inserted = connection.execute('''
        INSERT INTO receipts (receipt_id, report_id)
        SELECT ?, report_id FROM reports WHERE report_id = ? AND batch = ? AND manifest_id = ?
    ''', (receipt.receipt_id, report_id, batch, manifest_id))
    if inserted.rowcount != 1:
        raise ValueError('receipt could not be bound to its report and manifest')
    return receipt
