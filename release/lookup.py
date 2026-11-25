import sqlite3

from release.snapshot import ReportSnapshot, ReportStore


class UnknownReport(LookupError):
    pass


def claim(connection: sqlite3.Connection, report_id: str) -> ReportSnapshot:
    if not isinstance(report_id, str) or not report_id:
        raise UnknownReport(report_id)
    report = ReportStore(connection).get(report_id)
    if report is None:
        raise UnknownReport(report_id)
    return report


def find_report_id(connection: sqlite3.Connection, batch: str) -> str | None:
    return ReportStore(connection).report_id_for_batch(batch)
