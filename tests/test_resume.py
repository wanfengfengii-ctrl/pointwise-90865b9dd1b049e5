from intake.station import open_station
from recorder.ack import AckStatus
from recorder.device_store import PacketUnavailable
from recorder.packet import Origin, decode_packet
from release.lookup import UnknownReport
from tests.support import StationCase, export_records


class ResumeTests(StationCase):
    def test_ack_loss(self):
        with self.session(lost=('P',)) as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0')
            self.record_sample(device, 'P')
            self.assertEqual(station.ingest(device, 'P').state, 'pending')
            self.assertEqual(device.confirmation_status('P'), AckStatus.CONFIRMED)
            with self.assertRaises(PacketUnavailable):
                device.read_packet('P')
            a = self.seal_sample(station, 'A')
        with open_station(self.station_path) as station:
            self.assertEqual(station.ingest(None, 'P').state, 'pending')
            row = station.rows.get('B', 1)
            self.assertEqual((row.value, row.origin), (29, Origin('P', 2)))
            self.assertEqual(station.claim(a.report_id), a)
            self.assertEqual(station.connection.execute('SELECT count(*) FROM received_rows').fetchone(), (3,))
        with self.session() as (station, device):
            self.intake_samples(station, device, 'Q')
            b = self.seal_sample(station, 'B')
            self.assert_report(b, [29, 31], [('P', 2), ('Q', 1)], 60)
            position = device.store.read_position()
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            self.assertEqual(device.store.read_position(), position)
            self.assertEqual(station.connection.execute('SELECT count(*) FROM received_rows').fetchone(), (4,))
            self.assert_report(station.claim(a.report_id), [7, 11], [('A0', 1), ('P', 1)], 18)

    def test_exact_status(self):
        with self.session(lost=('P',)) as (station, device):
            self.register_samples(station)
            q = self.record_sample(device, 'Q')
            p = self.record_sample(device, 'P')
            self.assertGreater(p.sequence, q.sequence)
            self.assertEqual(station.ingest(device, 'P').state, 'pending')
            self.assertEqual(device.store.lookup_confirmation('P'), 1)
        with self.session() as (station, device):
            self.assertEqual(device.confirmation_status('Q'), AckStatus.RETAINED)
            self.assertEqual(device.confirmation_status('unknown'), AckStatus.UNKNOWN)
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            with self.assertRaises(PacketUnavailable):
                station.ingest(device, 'unknown')
            self.assertEqual(station.custody.get_progress('P').state, 'confirmed')
            self.assertEqual(decode_packet(device.read_packet('Q')), q)
            self.assertIsNone(device.store.lookup_confirmation('Q'))
            self.assertEqual(device.store.lookup_confirmation('P'), 1)
            self.assertEqual(device.store.read_position(), 1)
            self.assertEqual(station.connection.execute('SELECT count(*) FROM received_rows').fetchone(), (2,))
            self.assertEqual(station.rows.get('B', 1).origin, Origin('P', 2))

    def test_offline(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0', 'P', 'Q')
            reports = [self.seal_sample(station, batch) for batch in ('A', 'B')]
            originals = [station.export_report(report.report_id) for report in reports]
        with open_station(self.station_path) as station:
            for report, original in zip(reports, originals):
                self.assertEqual(station.find_report_id(report.batch), report.report_id)
                self.assertEqual(station.claim(report.report_id), report)
                self.assertEqual(station.export_report(report.report_id), original)
            self.assert_report(reports[0], [7, 11], [('A0', 1), ('P', 1)], 18)
            self.assert_report(reports[1], [29, 31], [('P', 2), ('Q', 1)], 60)
            self.assertEqual(station.connection.execute('SELECT count(*) FROM export_events').fetchone(), (4,))
            with self.assertRaises(UnknownReport):
                station.export_report('report:missing')
            self.assertEqual(station.connection.execute('SELECT count(*) FROM export_events').fetchone(), (4,))

    def test_offline_late_packet(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0', 'P', 'Q')
            original = self.seal_sample(station, 'B')
            exported = station.export_report(original.report_id)
            self.intake_samples(station, device, 'L')
            self.assertEqual(station.rows.get('B', 3).value, 5)
        with open_station(self.station_path) as station:
            same = station.claim(original.report_id)
            self.assertEqual(same, original)
            self.assertEqual(station.seal('B', original.manifest_id), original)
            self.assert_report(same, [29, 31], [('P', 2), ('Q', 1)], 60)
            late_row = station.rows.get('B', 3)
            self.assertEqual((late_row.value, late_row.origin), (5, Origin('L', 1)))
            offline_export = station.export_report(original.report_id)
            self.assertEqual(offline_export, exported)
            records = export_records(offline_export)
            self.assertEqual(records[0]['receipt_id'], original.receipt.receipt_id)
            self.assertEqual([record['value'] for record in records[1:]], ['29', '31'])
