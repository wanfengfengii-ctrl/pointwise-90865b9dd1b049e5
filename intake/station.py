import argparse
import csv
import io
import json
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from intake.compaction import request_compaction
from intake.custody import CustodyStore
from intake.dispatch import IngestResult, IntakeInterrupted, ingest
from intake.read import RecorderPort
from ledger.rows import RowStore
from ledger.tally import BatchIncomplete
from recorder.ack import AckStatus
from recorder.capture import open_recorder
from recorder.device_store import PacketUnavailable
from recorder.packet import PacketRow
from release.export import export_report, initialize_export_store
from release.lookup import claim, find_report_id
from release.publish import seal
from release.snapshot import ReportSnapshot, ReportStore
from runs.admission import register
from runs.catalog import Run, RunCatalog
from runs.closure import Manifest, close_batch, initialize_manifest_store


class Station:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.catalog = RunCatalog(connection)
        self.rows = RowStore(connection)
        self.custody = CustodyStore(connection)

    def register(self, batch: str, channel: str, unit: str) -> Run:
        return register(self.catalog, batch, channel, unit)

    def ingest(self, port: RecorderPort | None, packet_id: str, *,
               before_member: Callable[[PacketRow], None] | None = None) -> IngestResult:
        return ingest(self.connection, port, packet_id, before_member=before_member)

    def compact(self, port: RecorderPort, operation_id: str, left_id: str, right_id: str) -> str:
        return request_compaction(self.connection, port, operation_id, left_id, right_id)

    def close_batch(self, batch: str, positions: Sequence[int]) -> Manifest:
        return close_batch(self.catalog, batch, positions)

    def seal(self, batch: str, manifest_id: str) -> ReportSnapshot:
        return seal(self.connection, batch, manifest_id)

    def claim(self, report_id: str) -> ReportSnapshot:
        return claim(self.connection, report_id)

    def find_report_id(self, batch: str) -> str | None:
        return find_report_id(self.connection, batch)

    def export_report(self, report_id: str) -> str:
        return export_report(self.connection, report_id)


def initialize_station(connection: sqlite3.Connection) -> None:
    connection.execute('PRAGMA foreign_keys = ON')
    with connection:
        connection.execute('BEGIN IMMEDIATE')
        RunCatalog(connection).initialize()
        initialize_manifest_store(connection)
        RowStore(connection).initialize()
        CustodyStore(connection).initialize()
        ReportStore(connection).initialize()
        initialize_export_store(connection)


@contextmanager
def open_station(path: str | Path) -> Iterator[Station]:
    with closing(sqlite3.connect(path, isolation_level=None)) as connection:
        initialize_station(connection)
        yield Station(connection)


@dataclass(frozen=True)
class ScenarioInput:
    runs: list
    packets: dict
    manifests: dict


def load_scenario(sample_dir: Path) -> ScenarioInput:
    def read(name: str):
        return json.loads((sample_dir / f'{name}.json').read_text(encoding='utf-8'))

    return ScenarioInput(read('runs'), read('packets'), read('manifests'))


def _report_values(report: ReportSnapshot) -> dict:
    return {'values': [row.value for row in report.rows],
            'origins': [[row.origin.packet_id, row.origin.position] for row in report.rows],
            'total': report.total}


def _export_values(text: str, report: ReportSnapshot) -> dict:
    summary, *rows = csv.DictReader(io.StringIO(text))
    if summary['kind'] != 'summary' or summary['receipt_id'] != report.receipt.receipt_id:
        raise AssertionError('export does not identify the original receipt')
    if any(row['kind'] != 'reading' or row['receipt_id'] != summary['receipt_id'] for row in rows):
        raise AssertionError('export detail is not bound to the original receipt')
    return {'values': [int(row['value']) for row in rows],
            'origins': [[row['packet_id'], int(row['physical_position'])] for row in rows],
            'total': int(summary['total'])}


def run_scenario(sample_dir: Path, directory: Path) -> dict:
    sample = load_scenario(sample_dir)
    station_path, device_path = directory / 'station.sqlite3', directory / 'device.sqlite3'

    def pause_before_b(member: PacketRow) -> None:
        if member.batch == 'B':
            raise IntakeInterrupted('pause before B receives its first reading')

    with open_station(station_path) as station, open_recorder(device_path, station.catalog) as device:
        for batch, channel, unit in sample.runs:
            station.register(batch, channel, unit)
        device.record('A0', sample.packets['A0'])
        if station.ingest(device, 'A0').state != 'confirmed':
            raise AssertionError('A0 did not finish intake')
        device.record('P', sample.packets['P'])
        try:
            station.ingest(device, 'P', before_member=pause_before_b)
        except IntakeInterrupted:
            pass
        else:
            raise AssertionError('the partial handoff did not reach the pause')
        before_ack = device.confirmation_status('P').value
        manifest_a = station.close_batch('A', sample.manifests['A'])
        a = station.seal('A', manifest_a.manifest_id)
        if station.find_report_id('B') is not None:
            raise AssertionError('B was released during the pause')

    with open_station(station_path) as station, open_recorder(
            device_path, station.catalog, loss_after_reclaim=('P',)) as device:
        device.record('Q', sample.packets['Q'])
        if station.ingest(device, 'Q').state != 'confirmed':
            raise AssertionError('Q did not finish intake')
        manifest_b = station.close_batch('B', sample.manifests['B'])
        try:
            station.seal('B', manifest_b.manifest_id)
        except BatchIncomplete as exc:
            missing = list(exc.missing)
        else:
            raise AssertionError('B was released with a missing reading')
        if station.find_report_id('B') is not None:
            raise AssertionError('B has a report before all its readings are received')
        pending = station.ingest(device, 'P').state
        after_loss = device.confirmation_status('P').value
        try:
            device.read_packet('P')
        except PacketUnavailable:
            pass
        else:
            raise AssertionError('P is still fetchable after device confirmation')

    with open_station(station_path) as station:
        offline = station.ingest(None, 'P').state
        b = station.seal('B', manifest_b.manifest_id)
        claimed_a = station.claim(a.report_id)
        claimed_b = station.claim(b.report_id)
        if (claimed_a, claimed_b) != (a, b):
            raise AssertionError('offline claims differ from signed reports')
        exported = _export_values(station.export_report(b.report_id), claimed_b)
        with open_recorder(device_path, station.catalog) as device:
            settled = station.ingest(device, 'P').state
            if device.confirmation_status('Q') is not AckStatus.CONFIRMED:
                raise AssertionError('reconciling P changed Q status')

    expected_a = {'values': [7, 11], 'origins': [['A0', 1], ['P', 1]], 'total': 18}
    expected_b = {'values': [29, 31], 'origins': [['P', 2], ['Q', 1]], 'total': 60}
    expected = {'A': expected_a, 'B': expected_b, 'before_ack': 'retained', 'missing': [1],
                'after_loss': 'confirmed', 'pending': 'pending', 'offline': 'pending', 'settled': 'confirmed'}
    actual = {'A': _report_values(a), 'B': _report_values(b), 'before_ack': before_ack,
              'missing': missing, 'after_loss': after_loss, 'pending': pending,
              'offline': offline, 'settled': settled}
    downstream_expected = {'A': expected_a, 'B': expected_b, 'B_export': expected_b}
    downstream_actual = {'A': _report_values(claimed_a), 'B': _report_values(claimed_b), 'B_export': exported}
    if actual != expected or downstream_actual != downstream_expected:
        raise AssertionError(f'mixed handover differs from its independent reference: {actual!r}, {downstream_actual!r}')
    return {'case_id': 'mixed',
            'input': {'runs': sample.runs, 'packets': {name: sample.packets[name] for name in ('A0', 'P', 'Q')},
                      'manifests': sample.manifests, 'pause_before': ['P', 2], 'lost_reply': 'P'},
            'expected': expected, 'actual': actual, 'downstream_expected': downstream_expected,
            'downstream_actual': downstream_actual,
            'code_paths': ['recorder/device_store.py', 'intake/dispatch.py', 'intake/evidence.py',
                           'intake/resume.py', 'ledger/completeness.py', 'ledger/tally.py',
                           'release/publish.py', 'release/lookup.py', 'release/export.py'],
            'oracle': 'Hand sum 7 + 11 and 29 + 31, and read physical positions from the source packet order.'}


def main() -> None:
    parser = argparse.ArgumentParser(description='Run the vibration lab shift handover sample')
    parser.add_argument('command', choices=['demo'])
    parser.add_argument('sample_dir', type=Path)
    arguments = parser.parse_args()
    with TemporaryDirectory(prefix='vibration-lab-') as directory:
        evidence = run_scenario(arguments.sample_dir, Path(directory))
    print('POINTWISE_SCENARIO ' + json.dumps(evidence, ensure_ascii=False, separators=(',', ':')))


if __name__ == '__main__':
    main()
