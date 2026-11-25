import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path

from recorder.ack import AckStatus, acknowledge_packet, query_ack_status
from recorder.buffer import read_packet, retain_packet
from recorder.device_store import DeviceStore
from recorder.packet import Origin, Packet, PacketRow, validate_packet_id, validate_row
from runs.catalog import RunCatalog
from runs.rules import validate_identifier, validate_recording_channel


class Recorder:
    def __init__(self, store: DeviceStore, catalog: RunCatalog, loss_after_reclaim: Iterable[str] = ()):
        self.store = store
        self.catalog = catalog
        self.loss_after_reclaim = frozenset(loss_after_reclaim)

    def record(self, packet_id: str, readings: Iterable[tuple[str, int, str, int]]) -> Packet:
        validate_packet_id(packet_id)
        rows = []
        for physical_position, (batch, position, channel, value) in enumerate(readings, 1):
            row = PacketRow(batch, position, channel, value, Origin(packet_id, physical_position))
            validate_row(row)
            validate_identifier(batch, 'batch')
            validate_recording_channel(self.catalog.require(batch).channel, channel)
            rows.append(row)
        if not rows:
            raise ValueError('an original packet must have at least one reading')
        return retain_packet(self.store, packet_id, tuple(rows))

    def read_packet(self, packet_id: str) -> bytes:
        return read_packet(self.store, packet_id)

    def acknowledge(self, packet_id: str) -> AckStatus:
        return acknowledge_packet(self.store, packet_id, lose_reply=packet_id in self.loss_after_reclaim)

    def confirmation_status(self, packet_id: str) -> AckStatus:
        return query_ack_status(self.store, packet_id)


@contextmanager
def open_recorder(path: str | Path, catalog: RunCatalog, *, capacity: int | None = None,
                  loss_after_reclaim: Iterable[str] = ()) -> Iterator[Recorder]:
    with closing(sqlite3.connect(path, isolation_level=None)) as connection:
        store = DeviceStore(connection, capacity)
        store.initialize()
        yield Recorder(store, catalog, loss_after_reclaim)
