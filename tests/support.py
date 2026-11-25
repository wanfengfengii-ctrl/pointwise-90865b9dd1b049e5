import csv
import io
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from intake.dispatch import IntakeInterrupted
from intake.station import load_scenario, open_station
from recorder.capture import open_recorder


class StationCase(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='vibration-test-')
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        self.station_path = directory / 'station.sqlite3'
        self.device_path = directory / 'device.sqlite3'
        self.sample = load_scenario(Path(__file__).resolve().parents[1] / 'samples')

    @contextmanager
    def session(self, *, capacity=None, lost=()):
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog, capacity=capacity, loss_after_reclaim=lost) as device:
            yield station, device

    def register_samples(self, station):
        for batch, channel, unit in self.sample.runs:
            station.register(batch, channel, unit)

    def record_sample(self, device, packet_id):
        return device.record(packet_id, self.sample.packets[packet_id])

    def intake_samples(self, station, device, *packet_ids):
        for packet_id in packet_ids:
            self.record_sample(device, packet_id)
            self.assertEqual(station.ingest(device, packet_id).state, 'confirmed')

    def seal_sample(self, station, batch):
        manifest = station.close_batch(batch, self.sample.manifests[batch])
        return station.seal(batch, manifest.manifest_id)

    def assert_report(self, report, values, origins, total):
        self.assertEqual([row.value for row in report.rows], values)
        self.assertEqual([(row.origin.packet_id, row.origin.position) for row in report.rows], origins)
        self.assertEqual(report.total, total)
        self.assertEqual(report.receipt.report_id, report.report_id)
        self.assertEqual(report.receipt.manifest_id, report.manifest_id)
        self.assertEqual(report.receipt.batch, report.batch)


def interrupt_before_b(member):
    if member.batch == 'B':
        raise IntakeInterrupted('B member was not delivered yet')


def export_records(text):
    return list(csv.DictReader(io.StringIO(text)))
