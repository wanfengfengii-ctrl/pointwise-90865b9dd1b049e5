import json
import sqlite3
import tempfile
from pathlib import Path

from compaction_fixture import (PACKETS, CompactLink, DelayedRead, block_device_commit,
                                pause_b, prepare_paused, prepare_same_value_paused)
from intake.dispatch import IntakeInterrupted
from intake.station import open_station
from recorder.ack import AckStatus
from recorder.buffer import BufferFull
from recorder.capture import open_recorder
from recorder.device_store import PacketIdentityConflict, PacketUnavailable
from recorder.packet import (Origin, PacketRow, decode_delivered_packet, decode_packet,
                             encode_compacted_packet, member_digest)
from tests.support import StationCase


class CompactionModeTests(StationCase):
    def _paused(self, **recorder_options):
        station_cm = open_station(self.station_path)
        station = station_cm.__enter__()
        self.addCleanup(station_cm.__exit__, None, None, None)
        device_cm = open_recorder(self.device_path, station.catalog, capacity=2,
                                  compaction=True, **recorder_options)
        device = device_cm.__enter__()
        self.addCleanup(device_cm.__exit__, None, None, None)
        replies = prepare_paused(station, device)
        return station, device, replies

    def test_compaction_frees_one_slot_for_r(self):
        station, device, _ = self._paused()
        with self.assertRaises(BufferFull):
            device.record('R', PACKETS['R'])
        result = station.compact(device, 'op:1', 'P', 'Q')
        self.assertEqual(result, 'compacted')
        packet, complete = decode_delivered_packet(device.read_packet('P'))
        self.assertFalse(complete)
        self.assertEqual([(row.origin.packet_id, row.origin.position, row.value) for row in packet.rows],
                         [('P', 2, 29)])
        q_packet, q_complete = decode_delivered_packet(device.read_packet('Q'))
        self.assertFalse(q_complete)
        self.assertEqual([(row.origin.packet_id, row.origin.position, row.value) for row in q_packet.rows],
                         [('Q', 2, 31)])
        device.record('R', PACKETS['R'])
        self.assertEqual(device.confirmation_status('R'), AckStatus.RETAINED)
        with self.assertRaises(BufferFull):
            device.record('S', PACKETS['S'])
        self.assertEqual(device.confirmation_status('S'), AckStatus.UNKNOWN)

    def test_wrong_evidence_keeps_originals_and_slots(self):
        station, device, _ = self._paused()
        link = CompactLink(device, wrong_position=True)
        self.assertEqual(station.compact(link, 'op:bad', 'P', 'Q'), 'ineligible')
        # The ineligible terminal is fixed for the operation id.
        self.assertEqual(station.compact(link, 'op:bad', 'P', 'Q'), 'ineligible')
        self.assertEqual(device.confirmation_status('P'), AckStatus.RETAINED)
        self.assertEqual(device.confirmation_status('Q'), AckStatus.RETAINED)
        self.assertTrue(decode_packet(device.read_packet('P')).rows[0].value == 7)
        self.assertEqual(len(decode_packet(device.read_packet('Q')).rows), 2)
        with self.assertRaises(BufferFull):
            device.record('R', PACKETS['R'])
        # A fresh operation with correct evidence still compacts.
        self.assertEqual(station.compact(device, 'op:good', 'P', 'Q'), 'compacted')
        device.record('R', PACKETS['R'])

    def test_commit_failure_keeps_originals_and_slots(self):
        station, device, _ = self._paused()
        with block_device_commit(device):
            with self.assertRaises(sqlite3.DatabaseError):
                station.compact(device, 'op:fail', 'P', 'Q')
        self.assertEqual(device.confirmation_status('P'), AckStatus.RETAINED)
        self.assertEqual(len(decode_packet(device.read_packet('P')).rows), 2)
        self.assertEqual(len(decode_packet(device.read_packet('Q')).rows), 2)
        with self.assertRaises(BufferFull):
            device.record('R', PACKETS['R'])
        self.assertEqual(station.compact(device, 'op:fail', 'P', 'Q'), 'compacted')
        device.record('R', PACKETS['R'])

    def test_lost_reply_reopens_to_original_terminal(self):
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog, capacity=2, compaction=True) as device:
            prepare_paused(station, device)
            link = CompactLink(device, lose_reply=True)
            self.assertEqual(station.compact(link, 'op:lost', 'P', 'Q'), 'pending')
            self.assertTrue(link.dropped)
            # R can already be recorded because the physical compaction committed.
            device.record('R', PACKETS['R'])
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog) as device:
            self.assertEqual(station.compact(device, 'op:lost', 'P', 'Q'), 'compacted')
            with self.assertRaises(ValueError):
                station.compact(device, 'op:lost', 'Q', 'P')
            self.assertEqual(device.confirmation_status('R'), AckStatus.RETAINED)
            p_packet, complete = decode_delivered_packet(device.read_packet('P'))
            self.assertFalse(complete)
            self.assertEqual(p_packet.rows[0].origin, Origin('P', 2))
            q_packet, _ = decode_delivered_packet(device.read_packet('Q'))
            self.assertEqual(q_packet.rows[0].origin, Origin('Q', 2))

    def test_independent_confirmations(self):
        station, device, _ = self._paused()
        self.assertEqual(station.compact(device, 'op:1', 'P', 'Q'), 'compacted')
        device.record('R', PACKETS['R'])
        self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
        self.assertEqual(device.confirmation_status('P'), AckStatus.CONFIRMED)
        self.assertEqual(device.confirmation_status('Q'), AckStatus.RETAINED)
        q_progress = station.custody.require_progress('Q')
        self.assertEqual([slot.secured for slot in q_progress.members], [True, False])
        # Q's waiting B reading must not be delivered while only P is confirmed.
        self.assertIsNone(station.rows.get('B', 2))
        with self.assertRaises(PacketUnavailable):
            device.read_packet('P')
        # Q still holds the shared slot and R holds the other one.
        with self.assertRaises(BufferFull):
            device.record('S', PACKETS['S'])
        # B is still missing its second line until Q is independently received.
        self.assertIsNone(station.rows.get('B', 2))
        self.assertEqual(station.ingest(device, 'Q').state, 'confirmed')
        self.assertEqual(device.confirmation_status('Q'), AckStatus.CONFIRMED)
        self.assertEqual(station.rows.get('B', 2).origin, Origin('Q', 2))
        # Once Q is reclaimed too, the shared slot frees and S fits again.
        device.record('S', PACKETS['S'])
        self.assertEqual(device.confirmation_status('S'), AckStatus.RETAINED)

    def test_old_full_reply_and_compacted_reply_share_membership(self):
        # Path 1: a full v1 reply delivered late still settles against the same members.
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog, capacity=2, compaction=True) as device:
            replies = prepare_paused(station, device)
            old_port = DelayedRead(device, 'P', replies['P'])
            self.assertEqual(station.ingest(old_port, 'P').state, 'confirmed')
            row = station.rows.get('B', 1)
            self.assertEqual((row.value, row.origin.packet_id, row.origin.position), (29, 'P', 2))
        # Path 2: the post-compaction v2 reply settles the same members.
        with tempfile.TemporaryDirectory(prefix='vibration-compact-reply-') as other:
            other_dir = Path(other)
            with open_station(other_dir / 'station.sqlite3') as station, open_recorder(
                    other_dir / 'device.sqlite3', station.catalog, capacity=2,
                    compaction=True) as device:
                prepare_paused(station, device)
                self.assertEqual(station.compact(device, 'op:1', 'P', 'Q'), 'compacted')
                self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
                row = station.rows.get('B', 1)
                self.assertEqual((row.value, row.origin.packet_id, row.origin.position), (29, 'P', 2))

    def test_normal_mode_rejects_compaction(self):
        with self.session(capacity=4) as (station, device):
            self.register_samples(station)
            self.record_sample(device, 'P')
            self.record_sample(device, 'Q')
            for packet_id in ('P', 'Q'):
                try:
                    station.ingest(device, packet_id, before_member=pause_b)
                except IntakeInterrupted:
                    pass
                else:
                    raise AssertionError(f'{packet_id} did not stop at the paused batch')
            self.assertFalse(device.store.compaction_enabled())
            self.assertEqual(station.compact(device, 'op:x', 'P', 'Q'), 'ineligible')
            self.assertEqual(device.confirmation_status('P'), AckStatus.RETAINED)
            self.assertEqual(len(decode_packet(device.read_packet('P')).rows), 2)

    def test_mode_and_capacity_persist_on_reopen(self):
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog, capacity=2, compaction=True) as device:
            prepare_paused(station, device)
            self.assertTrue(device.store.compaction_enabled())
            self.assertEqual(device.store._capacity(), 2)
        with open_station(self.station_path) as station:
            with open_recorder(self.device_path, station.catalog) as device:
                self.assertTrue(device.store.compaction_enabled())
                self.assertEqual(device.store._capacity(), 2)
            with self.assertRaises(ValueError):
                with open_recorder(self.device_path, station.catalog, compaction=False):
                    pass
            with self.assertRaises(ValueError):
                with open_recorder(self.device_path, station.catalog, capacity=4):
                    pass
            # Reopening with the original explicit settings still works.
            with open_recorder(self.device_path, station.catalog, capacity=2, compaction=True):
                pass

    def test_same_value_wrong_position_cannot_free_slot(self):
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog, capacity=2, compaction=True) as device:
            prepare_same_value_paused(station, device)
            # P's two readings both equal 29, but the receipt names position 2
            # while the matching value lives at position 1.
            link = CompactLink(device, wrong_position=True)
            self.assertEqual(station.compact(link, 'op:misplaced', 'P', 'Q'), 'ineligible')
            self.assertEqual(len(decode_packet(device.read_packet('P')).rows), 2)
            with self.assertRaises(BufferFull):
                device.record('R', PACKETS['R'])
            self.assertEqual(station.compact(device, 'op:correct', 'P', 'Q'), 'compacted')
            device.record('R', PACKETS['R'])

    def test_tampered_digest_is_ineligible(self):
        station, device, _ = self._paused()
        left_rows = decode_packet(device.read_packet('P')).rows
        right_rows = decode_packet(device.read_packet('Q')).rows
        evidence = (('P', left_rows[0].origin.position, '0' * 64),
                    ('Q', right_rows[0].origin.position, member_digest(right_rows[0])))
        self.assertEqual(device.compact_packets('op:tamper', 'P', 'Q', evidence), 'ineligible')
        # The terminal is fixed and never becomes compacted on retry.
        self.assertEqual(device.compact_packets('op:tamper', 'P', 'Q', evidence), 'ineligible')
        self.assertEqual(len(decode_packet(device.read_packet('P')).rows), 2)

    def test_station_rejects_compaction_without_persistent_receipt(self):
        station, device, _ = self._paused()
        # Confirming P first means it no longer waits for handover.
        self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
        self.assertEqual(station.compact(device, 'op:late', 'P', 'Q'), 'ineligible')
        self.assertEqual(device.confirmation_status('Q'), AckStatus.RETAINED)
        self.assertEqual(len(decode_delivered_packet(device.read_packet('Q'))[0].rows), 2)

    def test_second_compaction_of_same_packets_is_ineligible(self):
        station, device, _ = self._paused()
        self.assertEqual(station.compact(device, 'op:1', 'P', 'Q'), 'compacted')
        # The retained subsets can no longer participate in another compaction.
        self.assertEqual(station.compact(device, 'op:2', 'P', 'Q'), 'ineligible')
        self.assertEqual(decode_delivered_packet(device.read_packet('P'))[0].rows[0].origin,
                         Origin('P', 2))

    def test_operation_id_binds_the_ordered_pair(self):
        station, device, _ = self._paused()
        self.assertEqual(station.compact(device, 'op:pair', 'P', 'Q'), 'compacted')
        # Same operation id with a reordered pair is a programming error.
        with self.assertRaises(ValueError):
            device.compact_packets('op:pair', 'Q', 'P', ())
        # Replaying the exact ordered pair reclaims the stored terminal.
        self.assertEqual(station.compact(device, 'op:pair', 'P', 'Q'), 'compacted')
        # A different operation id still cannot compact the already-trimmed packets.
        self.assertEqual(station.compact(device, 'op:other', 'P', 'Q'), 'ineligible')


class CompactedPacketCodecTests(StationCase):
    def test_v2_roundtrip_and_v1_strictness(self):
        rows = (PacketRow('B', 1, 'ay', 29, Origin('P', 2)),)
        payload = encode_compacted_packet('P', 7, rows)
        packet, complete = decode_delivered_packet(payload)
        self.assertFalse(complete)
        self.assertEqual((packet.packet_id, packet.sequence, packet.rows), ('P', 7, rows))
        with self.assertRaises(ValueError):
            decode_packet(payload)
        document = json.loads(payload)
        for mutation in (lambda d: d['rows'][0].__setitem__(4, 'Q'),
                         lambda d: d.__setitem__('rows', d['rows'] * 2),
                         lambda d: d.__setitem__('rows', []),
                         lambda d: d.__setitem__('version', 3)):
            copy_doc = json.loads(payload)
            mutation(copy_doc)
            with self.subTest(document=copy_doc):
                with self.assertRaises(ValueError):
                    decode_delivered_packet(json.dumps(copy_doc).encode())


class CompactionDedupTests(StationCase):
    def test_tampered_compacted_member_has_no_intake_eligibility(self):
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog, capacity=2, compaction=True) as device:
            prepare_paused(station, device)
            self.assertEqual(station.compact(device, 'op:1', 'P', 'Q'), 'compacted')
            document = json.loads(device.read_packet('P'))
            document['rows'][0][3] = hex(99)

            class Tampered:
                def __init__(self, real):
                    self.real = real

                def read_packet(self, packet_id):
                    if packet_id == 'P':
                        return json.dumps(document).encode()
                    return self.real.read_packet(packet_id)

                def __getattr__(self, name):
                    return getattr(self.real, name)

            with self.assertRaises(PacketIdentityConflict):
                station.ingest(Tampered(device), 'P')
            self.assertEqual(device.confirmation_status('P'), AckStatus.RETAINED)
            self.assertIsNone(station.rows.get('B', 1))
            self.assertEqual(station.ingest(device, 'P').state, 'confirmed')
            self.assertEqual(station.rows.get('B', 1).value, 29)

    def test_redelivery_keeps_original_intake_and_rejects_misbound_member(self):
        with open_station(self.station_path) as station, open_recorder(
                self.device_path, station.catalog, capacity=2, compaction=True) as device:
            prepare_paused(station, device)
            self.assertEqual(station.compact(device, 'op:1', 'P', 'Q'), 'compacted')
            # Re-reading the compacted packet is idempotent before confirmation.
            first = station.ingest(device, 'P')
            self.assertEqual(first.state, 'confirmed')
            again = station.ingest(device, 'P')
            self.assertEqual(again.state, 'confirmed')
            self.assertEqual(
                station.connection.execute(
                    "SELECT count(*) FROM received_rows WHERE packet_id = 'P'").fetchone(),
                (2,))
            # Q's waiting member must survive P's confirmation.
            q_progress = station.custody.require_progress('Q')
            self.assertEqual([slot.secured for slot in q_progress.members], [True, False])
            self.assertIsNone(station.rows.get('B', 2))
            self.assertEqual(station.ingest(device, 'Q').state, 'confirmed')
            manifest = station.close_batch('B', [1, 2])
            report = station.seal('B', manifest.manifest_id)
            self.assertEqual([r.value for r in report.rows], [29, 31])
            self.assertEqual([(r.origin.packet_id, r.origin.position) for r in report.rows],
                             [('P', 2), ('Q', 2)])
            self.assertEqual(report.total, 60)
            report_id = report.report_id
            first_receipt = report.receipt
        # Stop the source: reopen station-only and claim the fixed result.
        with open_station(self.station_path) as station:
            self.assertEqual(station.find_report_id('B'), report_id)
            claimed = station.claim(report_id)
            self.assertEqual([r.value for r in claimed.rows], [29, 31])
            self.assertEqual(claimed.total, 60)
            self.assertEqual([(r.origin.packet_id, r.origin.position) for r in claimed.rows],
                             [('P', 2), ('Q', 2)])
            self.assertEqual(claimed.receipt, first_receipt)
            # Reclaiming reuses the original first receipt rather than issuing a new one.
            self.assertEqual(station.claim(report_id).receipt, first_receipt)
