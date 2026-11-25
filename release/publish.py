import sqlite3

from ledger.completeness import evaluate_completeness
from ledger.rows import RowStore
from ledger.tally import prepare_tally
from release.receipt import issue_first_receipt
from release.snapshot import ReportSnapshot, ReportStore
from runs.closure import get_manifest
from runs.rules import validate_identifier


class ManifestMismatch(ValueError):
    pass


def seal(connection: sqlite3.Connection, batch: str, manifest_id: str) -> ReportSnapshot:
    validate_identifier(batch, 'batch')
    reports = ReportStore(connection)
    with connection:
        connection.execute('BEGIN IMMEDIATE')
        manifest = get_manifest(connection, batch)
        if manifest is None or manifest.manifest_id != manifest_id:
            raise ManifestMismatch(batch)
        existing = reports.get_for_manifest(manifest_id)
        if existing is not None:
            return existing
        tally = prepare_tally(evaluate_completeness(manifest, RowStore(connection)))
        report_id = f'report:{batch}'
        reports.insert(report_id, tally)
        receipt = issue_first_receipt(connection, report_id, batch, manifest_id)
        return ReportSnapshot(report_id, tally.batch, tally.manifest_id, tally.unit, tally.rows,
                              tally.total, receipt)
