# Reusable inputs and transport controls for the two-packet intake exercise.
import argparse
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from intake.dispatch import IntakeInterrupted
from intake.station import open_station
from recorder.ack import AckStatus
from recorder.buffer import BufferFull
from recorder.capture import open_recorder
from recorder.packet import Origin, decode_delivered_packet, decode_packet


RUNS = (("A", "ax"), ("B", "ay"), ("C", "ax"), ("D", "ax"))
PACKETS = {
    "P": [("A", 1, "ax", 7), ("B", 1, "ay", 29)],
    "Q": [("C", 1, "ax", 11), ("B", 2, "ay", 31)],
    "R": [("D", 1, "ax", 13), ("D", 2, "ax", 17)],
    "S": [("D", 3, "ax", 19)],
}


def pause_b(member):
    if member.batch == "B":
        raise IntakeInterrupted("B intake is paused")


def prepare_same_value_paused(station, device):
    """Same handover with P's A reading also equal to 29 (same value, different place)."""
    packets = {
        "P": [("A", 1, "ax", 29), ("B", 1, "ay", 29)],
        "Q": PACKETS["Q"],
    }
    for batch, channel in RUNS:
        station.register(batch, channel, "m/s²")
    replies = {}
    for name in ("P", "Q"):
        device.record(name, packets[name])
        replies[name] = device.read_packet(name)
        try:
            station.ingest(device, name, before_member=pause_b)
        except IntakeInterrupted:
            pass
        else:
            raise AssertionError(f"{name} did not stop at the paused batch")
    return replies


def prepare_paused(station, device, packets=PACKETS):
    for batch, channel in RUNS:
        station.register(batch, channel, "m/s²")
    replies = {}
    for name in ("P", "Q"):
        device.record(name, packets[name])
        replies[name] = device.read_packet(name)
        try:
            station.ingest(device, name, before_member=pause_b)
        except IntakeInterrupted:
            pass
        else:
            raise AssertionError(f"{name} did not stop at the paused batch")
    return replies


class DelayedRead:
    def __init__(self, device, packet_id, reply):
        self.device = device
        self.packet_id = packet_id
        self.reply = reply

    def read_packet(self, packet_id):
        if packet_id == self.packet_id:
            return self.reply
        return self.device.read_packet(packet_id)

    def __getattr__(self, name):
        return getattr(self.device, name)


class CompactLink:
    def __init__(self, device, *, wrong_position=False, lose_reply=False):
        self.device = device
        self.wrong_position = wrong_position
        self.lose_reply = lose_reply
        self.dropped = False

    def compact_packets(self, operation_id, left_id, right_id, evidence):
        if self.wrong_position:
            evidence = tuple(
                (packet_id, 3 - position, digest) if packet_id == left_id else (packet_id, position, digest)
                for packet_id, position, digest in evidence
            )
        status = self.device.compact_packets(operation_id, left_id, right_id, evidence)
        if self.lose_reply and status == "compacted":
            self.lose_reply = False
            self.dropped = True
            raise ConnectionError("compact reply lost after device commit")
        return status

    def __getattr__(self, name):
        return getattr(self.device, name)


@contextmanager
def block_device_commit(device):
    connection = device.store.connection

    def authorize(action, arg1, arg2, database, source):
        if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorize)
    try:
        yield
    finally:
        connection.set_authorizer(None)


def check_v1(directory):
    with open_station(directory / "station.sqlite3") as station, open_recorder(
            directory / "device.sqlite3", station.catalog, capacity=2) as device:
        replies = prepare_paused(station, device)
        for name in ("P", "Q"):
            progress = station.custody.require_progress(name)
            if tuple(member.secured for member in progress.members) != (True, False):
                raise AssertionError(f"unexpected intake progress for {name}")
            if device.read_packet(name) != replies[name]:
                raise AssertionError(f"{name} did not keep its original bytes")
            if device.confirmation_status(name) is not AckStatus.RETAINED:
                raise AssertionError(f"{name} is not retained")
        if any(station.rows.get("B", position) is not None for position in (1, 2)):
            raise AssertionError("a paused reading was received")
        try:
            device.record("R", PACKETS["R"])
        except BufferFull:
            pass
        else:
            raise AssertionError("R was admitted while both ordinary slots were occupied")
        if device.confirmation_status("R") is not AckStatus.UNKNOWN:
            raise AssertionError("rejected R was registered at the device")
    return {"retained": ["P", "Q"], "pending": [["P", 2], ["Q", 2]], "R": "buffer_full"}


def check_compaction(directory):
    """Exercise the full two-slot compaction handover, including a lost reply."""
    station_path, device_path = directory / "station.sqlite3", directory / "device.sqlite3"

    with open_station(station_path) as station, open_recorder(
            device_path, station.catalog, capacity=2, compaction=True) as device:
        replies = prepare_paused(station, device)
        for name in ("P", "Q"):
            if device.confirmation_status(name) is not AckStatus.RETAINED:
                raise AssertionError(f"{name} must stay retained before compaction")
        try:
            device.record("R", PACKETS["R"])
        except BufferFull:
            pass
        else:
            raise AssertionError("R was admitted while both slots were occupied")

        # A wrong-position receipt must not move any original reading.
        wrong_link = CompactLink(device, wrong_position=True)
        if station.compact(wrong_link, "op:wrong", "P", "Q") != "ineligible":
            raise AssertionError("a misplaced receipt was accepted as evidence")
        if len(decode_packet(device.read_packet("P")).rows) != 2:
            raise AssertionError("rejected evidence removed an original reading")

        # The successful physical compaction loses its reply once.
        lost_link = CompactLink(device, lose_reply=True)
        if station.compact(lost_link, "op:pair", "P", "Q") != "pending":
            raise AssertionError("a lost compaction reply must be reported as pending")
        compacted_p, p_complete = decode_delivered_packet(device.read_packet("P"))
        compacted_q, q_complete = decode_delivered_packet(device.read_packet("Q"))
        if p_complete or q_complete:
            raise AssertionError("compacted replies must be v2 subsets")
        if [r.origin for r in compacted_p.rows] != [Origin("P", 2)]:
            raise AssertionError("P must retain only its waiting B reading")
        if [r.origin for r in compacted_q.rows] != [Origin("Q", 2)]:
            raise AssertionError("Q must retain only its waiting B reading")

        # B still has no persistent original reading, so R now fits and S does not.
        if any(station.rows.get("B", position) is not None for position in (1, 2)):
            raise AssertionError("compaction delivered a paused B reading")
        device.record("R", PACKETS["R"])
        try:
            device.record("S", PACKETS["S"])
        except BufferFull:
            pass
        else:
            raise AssertionError("S was admitted while both slots stayed occupied")

    # Reopen both databases and settle the lost reply under the original operation id.
    with open_station(station_path) as station, open_recorder(device_path, station.catalog) as device:
        if station.compact(device, "op:pair", "P", "Q") != "compacted":
            raise AssertionError("reopening must recover the original compacted terminal")
        try:
            station.compact(device, "op:pair", "Q", "P")
        except ValueError:
            pass
        else:
            raise AssertionError("the same operation id rejected a different ordered pair")
        if device.confirmation_status("R") is not AckStatus.RETAINED:
            raise AssertionError("R must remain retained across the reopen")

        # A late full reply and the compacted reply verify the same P members.
        late_p = station.ingest(DelayedRead(device, "P", replies["P"]), "P")
        if late_p.state != "confirmed":
            raise AssertionError("the late full P reply did not confirm")
        if device.confirmation_status("Q") is not AckStatus.RETAINED:
            raise AssertionError("confirming P must not confirm Q")
        if [m.secured for m in station.custody.require_progress("Q").members] != [True, False]:
            raise AssertionError("P confirmation cleared Q's waiting reading")
        if station.rows.get("B", 2) is not None:
            raise AssertionError("P confirmation delivered Q's B reading")

        # Re-delivering the compacted P reply is idempotent and stays confirmed.
        if station.ingest(device, "P").state != "confirmed":
            raise AssertionError("P redelivery was not deduplicated")
        if station.ingest(device, "Q").state != "confirmed":
            raise AssertionError("Q could not finish its own independent confirmation")

        manifest = station.close_batch("B", [1, 2])
        report = station.seal("B", manifest.manifest_id)
        first_receipt = report.receipt
        values = [row.value for row in report.rows]
        origins = [[row.origin.packet_id, row.origin.position] for row in report.rows]
        if values != [29, 31] or report.total != 60:
            raise AssertionError("B did not keep its frozen order and exact sum")
        if origins != [["P", 2], ["Q", 2]]:
            raise AssertionError("B lost its compacted physical origins")

    # Stop the source and reopen station-only: claims are fixed by report id.
    with open_station(station_path) as station:
        report_id = station.find_report_id("B")
        claimed = station.claim(report_id)
        if claimed != report or claimed.receipt != first_receipt:
            raise AssertionError("the stop-source claim changed the original result or first receipt")
        again = station.claim(report_id)
        if again.receipt != first_receipt:
            raise AssertionError("reclaiming did not reuse the original first receipt")
        exported = station.export_report(report_id)

    return {
        "compaction": "compacted",
        "lost_reply": "pending_then_compacted",
        "wrong_evidence": "ineligible",
        "R": "recorded",
        "S": "buffer_full",
        "report_id": report_id,
        "values": values,
        "total": report.total,
        "origins": origins,
        "receipt_id": first_receipt.receipt_id,
        "export": next(line for line in exported.splitlines() if line.startswith("summary,")),
    }


def main():
    parser = argparse.ArgumentParser(description="Run the two-packet compaction exercise")
    parser.add_argument("command", choices=["check-v1", "demo"], nargs="?", default="check-v1")
    arguments = parser.parse_args()
    with TemporaryDirectory(prefix="vibration-compaction-") as directory:
        if arguments.command == "check-v1":
            result = check_v1(Path(directory))
            label = "POINTWISE_V1_CAPACITY"
        else:
            result = check_compaction(Path(directory))
            label = "POINTWISE_COMPACTION"
    print(label + " " + json.dumps(result, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
