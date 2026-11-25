import json

from intake.dispatch import IntakeInterrupted
from intake.station import initialize_station
from recorder.ack import AckStatus
from recorder.buffer import BufferFull
from recorder.device_store import PacketIdentityConflict, PacketUnavailable
from recorder.packet import Origin, decode_packet
from runs.admission import RegistrationConflict
from tests.support import StationCase, interrupt_before_b


class CaptureTests(StationCase):
    def test_single(self):
        with self.session() as (station, device):
            first = station.register('A', 'ax', 'm/s²')
            self.assertEqual(station.register('A', 'ax', 'm/s²'), first)
            device.record('S', [('A', 1, 'ax', 0), ('A', 2, 'ax', 7)])
            first_result = station.ingest(device, 'S')
            self.assertEqual(first_result.state, 'confirmed')
            self.assertEqual(station.ingest(device, 'S'), first_result)
            self.assertEqual(device.store.read_position(), 1)
            self.assertEqual(station.connection.execute('SELECT count(*) FROM received_rows').fetchone(), (2,))
            report = self.seal_sample(station, 'A')
            self.assert_report(report, [0, 7], [('S', 1), ('S', 2)], 7)

    def test_registration_conflict(self):
        with self.session() as (station, device):
            original = station.register('A', 'ax', 'm/s²')
            self.assertEqual(station.register('A', 'ax', 'm/s²'), original)
            for channel, unit in [('az', 'm/s²'), ('ax', 'mm/s²')]:
                with self.subTest(channel=channel, unit=unit), self.assertRaises(RegistrationConflict):
                    station.register('A', channel, unit)
            self.assertEqual(station.catalog.find('A'), original)
            with self.assertRaises(ValueError):
                device.record('bad', [('A', 1, 'az', 7)])
            self.intake_samples(station, device, 'A0')
            self.assertEqual(station.rows.get('A', 1).value, 7)

    def test_initialization(self):
        with self.session(lost=('U',)) as (station, device):
            stored = station.register('USER', 'z', 'mm/s²')
            device.record('U', [('USER', 5, 'z', -8)])
            self.assertEqual(station.ingest(device, 'U').state, 'pending')
            manifest = station.close_batch('USER', [5])
            report = station.seal('USER', manifest.manifest_id)
            export = station.export_report(report.report_id)
            initialize_station(station.connection)
            self.assertEqual(station.custody.get_progress('U').state, 'pending')
        with self.session() as (station, device):
            self.assertEqual(station.catalog.find('USER'), stored)
            self.assertEqual(station.close_batch('USER', [5]), manifest)
            self.assertEqual(station.rows.get('USER', 5).value, -8)
            self.assertEqual(station.claim(report.report_id), report)
            self.assertEqual(station.export_report(report.report_id), export)
            self.assertEqual(station.custody.get_progress('U').state, 'pending')
            self.assertEqual(station.ingest(device, 'U').state, 'confirmed')
            device.record('NEXT', [('USER', 6, 'z', 14)])
            self.assertEqual(station.ingest(device, 'NEXT').state, 'confirmed')
            self.assertEqual(station.rows.get('USER', 6).value, 14)
            self.assertEqual(station.claim(report.report_id), report)

    def test_buffer_capacity(self):
        with self.session(capacity=1) as (station, device):
            self.register_samples(station)
            original = self.record_sample(device, 'A0')
            with self.assertRaises(BufferFull):
                self.record_sample(device, 'P')
            self.assertEqual(decode_packet(device.read_packet('A0')), original)
            self.assertEqual(device.confirmation_status('P'), AckStatus.UNKNOWN)
            self.assertEqual(station.ingest(device, 'A0').state, 'confirmed')
            with self.assertRaises(PacketUnavailable):
                device.read_packet('A0')
            self.assertEqual(self.record_sample(device, 'P').sequence, original.sequence + 1)

    def test_packet_identity(self):
        with self.session() as (station, device):
            self.register_samples(station)
            original = self.record_sample(device, 'P')
            self.assertEqual(self.record_sample(device, 'P'), original)
            for changed in ([('A', 2, 'ax', 12), ('B', 1, 'ay', 29)], list(reversed(self.sample.packets['P']))):
                with self.subTest(changed=changed), self.assertRaises(PacketIdentityConflict):
                    device.record('P', changed)
            payload = device.read_packet('P')
            self.assertEqual(decode_packet(payload), original)
            wrong_origin = json.loads(payload)
            wrong_origin['rows'][1][5] = 1
            with self.assertRaises(ValueError):
                decode_packet(json.dumps(wrong_origin).encode())
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            with self.assertRaises(PacketUnavailable):
                self.record_sample(device, 'P')
            with self.assertRaises(PacketIdentityConflict):
                device.record('P', [('A', 2, 'ax', 99)])
            self.assertEqual(station.rows.get('B', 1).origin, Origin('P', 2))

    def test_invalid_reading_does_not_retain_packet(self):
        with self.session() as (station, device):
            self.register_samples(station)
            for value in (None, True, 1.5, '7'):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    device.record('BAD', [('A', 1, 'ax', value)])
            self.assertEqual(device.confirmation_status('BAD'), AckStatus.UNKNOWN)
            self.assertIsNone(station.rows.get('A', 1))

    def test_interrupt(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'A0')
            original = self.record_sample(device, 'P')
            position = device.store.read_position()
            with self.assertRaises(IntakeInterrupted):
                station.ingest(device, 'P', before_member=interrupt_before_b)
            self.assertEqual(device.store.read_position(), position)
            self.assertIsNone(device.store.lookup_confirmation('P'))
            self.assertEqual(decode_packet(device.read_packet('P')), original)
            self.assertEqual([slot.secured for slot in station.custody.get_progress('P').members], [True, False])
            self.assertEqual(station.rows.get('A', 2).value, 11)
            self.assertIsNone(station.rows.get('B', 1))
        with self.session() as (station, device):
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            self.assertEqual(device.store.read_position(), position + 1)
            self.assertEqual(device.store.lookup_confirmation('P'), position + 1)
            self.assertEqual(station.connection.execute('SELECT count(*) FROM received_rows').fetchone(), (3,))
            self.assertEqual(station.rows.get('A', 2).origin, Origin('P', 1))
            self.assertEqual(station.rows.get('B', 1).origin, Origin('P', 2))
            self.assertEqual(station.rows.get('B', 1).value, 29)

    def test_conflict(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'P', 'Q')
            before = self.seal_sample(station, 'B')
            device.record('X', [('B', 1, 'ay', 39)])
            result = station.ingest(device, 'X')
            self.assertEqual((result.state, result.conflicts), ('conflict', (1,)))
            self.assertEqual(device.confirmation_status('X'), AckStatus.RETAINED)
            self.assertEqual(station.rows.get('B', 1), before.rows[0])
            self.assertEqual(station.claim(before.report_id), before)
            trace = station.connection.execute('SELECT proposed_value, proposed_packet FROM row_conflicts').fetchone()
            self.assertEqual(trace, ('0x27', 'X'))

    def test_same_value_new_origin(self):
        with self.session() as (station, device):
            self.register_samples(station)
            self.intake_samples(station, device, 'P')
            device.record('NEW', [('B', 1, 'ay', 29), ('A', 1, 'ax', 7)])
            result = station.ingest(device, 'NEW')
            self.assertEqual((result.state, result.conflicts), ('conflict', (1,)))
            self.assertEqual(device.confirmation_status('NEW'), AckStatus.RETAINED)
            self.assertEqual(station.rows.get('B', 1).origin, Origin('P', 2))
            self.assertEqual(station.rows.get('B', 1).value, 29)
            self.assertEqual(station.rows.get('A', 1).origin, Origin('NEW', 2))
            self.assert_report(self.seal_sample(station, 'A'), [7, 11], [('NEW', 2), ('P', 1)], 18)
