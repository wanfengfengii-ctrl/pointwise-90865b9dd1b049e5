import sqlite3

from intake.dispatch import IntakeInterrupted
from ledger.tally import BatchIncomplete
from recorder.ack import AckStatus
from release.lookup import UnknownReport
from release.publish import ManifestMismatch
from runs.catalog import UnknownRun
from runs.closure import ManifestConflict
from tests.support import StationCase, export_records, interrupt_before_b


class BatchCloseTests(StationCase):
    def test_mixed(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0', 'P', 'Q')
            a = self.seal_sample(station, 'A')
            b = self.seal_sample(station, 'B')
            self.assert_report(a, [7, 11], [('A0', 1), ('P', 1)], 18)
            self.assert_report(b, [29, 31], [('P', 2), ('Q', 1)], 60)
            self.assertEqual([a.unit, b.unit], ['m/s²', 'm/s²'])
            self.assertEqual(station.find_report_id('A'), a.report_id)
            self.assertEqual(station.claim(b.report_id), b)

    def test_a_before_ack(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0')
            self.record_sample(device, 'P')
            with self.assertRaises(IntakeInterrupted):
                station.ingest(device, 'P', before_member=interrupt_before_b)
            self.assertEqual(device.confirmation_status('P'), AckStatus.RETAINED)
            a = self.seal_sample(station, 'A')
            self.assert_report(station.claim(a.report_id), [7, 11], [('A0', 1), ('P', 1)], 18)
            self.assertIsNone(station.find_report_id('B'))
            with self.assertRaises(UnknownReport):
                station.claim('report:B')
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            self.intake_samples(station, device, 'Q')
            b = self.seal_sample(station, 'B')
            self.assert_report(station.claim(b.report_id), [29, 31], [('P', 2), ('Q', 1)], 60)
            self.assertEqual(station.claim(a.report_id), a)

    def test_manifest_conflict(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'P', 'Q')
            manifest = station.close_batch('B', [2, 1])
            self.assertEqual(station.close_batch('B', [2, 1]), manifest)
            with self.assertRaises(ManifestConflict):
                station.close_batch('B', [1, 2])
            with self.assertRaises(ValueError):
                station.close_batch('A', [1, 1])
            with self.assertRaises(UnknownRun):
                station.close_batch('C', [1])
            with self.assertRaises(ManifestMismatch):
                station.seal('A', manifest.manifest_id)
            report = station.seal('B', manifest.manifest_id)
            self.assertEqual([row.position for row in report.rows], [2, 1])
            self.assert_report(report, [31, 29], [('Q', 1), ('P', 2)], 60)

    def test_physical_origins(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0', 'P', 'Q')
            a = station.claim(self.seal_sample(station, 'A').report_id)
            b = station.claim(self.seal_sample(station, 'B').report_id)
            self.assertEqual((a.rows[1].position, a.rows[1].origin.position), (2, 1))
            self.assertEqual((b.rows[0].position, b.rows[0].origin.position), (1, 2))
            exported = export_records(station.export_report(b.report_id))
            self.assertEqual([(r['ordinal'], r['position'], r['packet_id'], r['physical_position']) for r in exported[1:]],
                             [('1', '1', 'P', '2'), ('2', '2', 'Q', '1')])

    def test_exact_sum(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0', 'P', 'Q')
            self.assertEqual(self.seal_sample(station, 'A').total, 18)
            self.assertEqual(self.seal_sample(station, 'B').total, 60)
            station.register('C', 'ax', 'm/s²')
            station.register('D', 'ay', 'm/s²')
            station.register('E', 'ay', 'm/s²')
            device.record('EXACT', [('C', 1, 'ax', 9007199254740993), ('D', 1, 'ay', 9223372036854775807),
                                    ('C', 2, 'ax', -9007199254740992), ('D', 2, 'ay', 9223372036854775807),
                                    ('E', 1, 'ay', -1180591620717411303424)])
            self.assertEqual(station.ingest(device, 'EXACT').state, 'confirmed')
            reports = {}
            for batch, positions, total in [('C', [1, 2], 1), ('D', [1, 2], 18446744073709551614),
                                            ('E', [1], -1180591620717411303424)]:
                manifest = station.close_batch(batch, positions)
                reports[batch] = station.seal(batch, manifest.manifest_id)
                self.assertEqual(reports[batch].total, total)
        with self.session() as (station, _):
            for batch, values, origins, total in [
                ('C', [9007199254740993, -9007199254740992], [('EXACT', 1), ('EXACT', 3)], 1),
                ('D', [9223372036854775807, 9223372036854775807], [('EXACT', 2), ('EXACT', 4)], 18446744073709551614),
                ('E', [-1180591620717411303424], [('EXACT', 5)], -1180591620717411303424),
            ]:
                with self.subTest(batch=batch):
                    report = station.claim(reports[batch].report_id)
                    self.assertEqual(report, reports[batch])
                    self.assert_report(report, values, origins, total)
                    exported = export_records(station.export_report(report.report_id))
                    self.assertEqual(exported[0]['total'], str(total))
                    self.assertEqual([int(row['value']) for row in exported[1:]], values)

    def test_missing_and_zero_received(self):
        with self.session() as (station, device):
            self.register_samples(station)
            station.register('C', 'az', 'm/s²')
            self.intake_samples(station, device, 'A0', 'Q')
            self.record_sample(device, 'P')
            with self.assertRaises(IntakeInterrupted):
                station.ingest(device, 'P', before_member=interrupt_before_b)
            a = self.seal_sample(station, 'A')
            self.assertEqual(a.total, 18)
            manifest_b = station.close_batch('B', [1, 2])
            with self.assertRaises(BatchIncomplete) as incomplete:
                station.seal('B', manifest_b.manifest_id)
            self.assertEqual((incomplete.exception.missing, incomplete.exception.invalid), ((1,), ()))
            self.assertEqual(station.rows.get('B', 2).value, 31)
            self.assertIsNone(station.find_report_id('B'))
            self.assertEqual(station.connection.execute('SELECT count(*) FROM receipts').fetchone(), (1,))
            manifest_c = station.close_batch('C', [1])
            with self.assertRaises(BatchIncomplete):
                station.seal('C', manifest_c.manifest_id)
            device.record('ZERO', [('C', 1, 'az', 0)])
            self.assertEqual(station.ingest(device, 'ZERO').state, 'confirmed')
            c = station.seal('C', manifest_c.manifest_id)
            self.assert_report(c, [0], [('ZERO', 1)], 0)
            self.assertIsNone(station.find_report_id('B'))
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            self.assert_report(station.seal('B', manifest_b.manifest_id), [29, 31], [('P', 2), ('Q', 1)], 60)

    def test_empty_closure_exports_summary(self):
        with self.session() as (station, _):
            station.register('EMPTY', 'ax', 'm/s²')
            manifest = station.close_batch('EMPTY', [])
            report = station.seal('EMPTY', manifest.manifest_id)
            self.assert_report(report, [], [], 0)
            records = export_records(station.export_report(report.report_id))
            self.assertEqual(len(records), 1)
            self.assertEqual((records[0]['kind'], records[0]['total'], records[0]['receipt_id']),
                             ('summary', '0', report.receipt.receipt_id))

    def test_rollback(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0', 'P')
            manifest = station.close_batch('A', [1, 2])
            station.connection.execute('''
                CREATE TRIGGER deny_report BEFORE INSERT ON reports
                BEGIN SELECT RAISE(ABORT, 'publication blocked'); END
            ''')
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'publication blocked'):
                station.seal('A', manifest.manifest_id)
            self.assertIsNone(station.find_report_id('A'))
            self.assertEqual(station.rows.get('A', 2).value, 11)
            self.assertEqual(station.connection.execute('SELECT count(*) FROM receipts').fetchone(), (0,))
            station.connection.execute('DROP TRIGGER deny_report')
            report = station.seal('A', manifest.manifest_id)
            self.assert_report(report, [7, 11], [('A0', 1), ('P', 1)], 18)
            self.assertEqual(station.seal('A', manifest.manifest_id), report)

    def test_receipt_write_rollback(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'P', 'Q')
            manifest = station.close_batch('B', [1, 2])
            station.connection.execute('''
                CREATE TRIGGER deny_first_receipt BEFORE INSERT ON receipts
                BEGIN SELECT RAISE(ABORT, 'first receipt blocked'); END
            ''')
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'first receipt blocked'):
                station.seal('B', manifest.manifest_id)
        with self.session() as (station, _):
            self.assertIsNone(station.find_report_id('B'))
            with self.assertRaises(UnknownReport):
                station.claim('report:B')
            self.assertEqual(station.connection.execute('''
                SELECT (SELECT count(*) FROM reports), (SELECT count(*) FROM report_lines),
                       (SELECT count(*) FROM receipts)
            ''').fetchone(), (0, 0, 0))
            self.assertEqual(station.rows.get('B', 1).value, 29)
            station.connection.execute('DROP TRIGGER deny_first_receipt')
            report = station.seal('B', manifest.manifest_id)
            self.assertEqual(station.seal('B', manifest.manifest_id), report)
            self.assertEqual(station.claim(report.report_id), report)
            self.assertEqual(station.connection.execute('''
                SELECT (SELECT count(*) FROM reports), (SELECT count(*) FROM report_lines),
                       (SELECT count(*) FROM receipts)
            ''').fetchone(), (1, 2, 1))
