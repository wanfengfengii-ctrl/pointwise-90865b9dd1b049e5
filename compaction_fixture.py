# Reusable inputs and transport controls for the two-packet intake exercise.
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


def main():
    with TemporaryDirectory(prefix="vibration-compaction-") as directory:
        result = check_v1(Path(directory))
    print("POINTWISE_V1_CAPACITY " + json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
