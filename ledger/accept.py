from dataclasses import dataclass

from ledger.rows import ReceivedRow, RowStore
from recorder.packet import PacketRow, validate_row
from runs.catalog import RunCatalog
from runs.rules import validate_identifier, validate_recording_channel


@dataclass(frozen=True)
class Acceptance:
    state: str
    row: ReceivedRow


def accept_row(catalog: RunCatalog, rows: RowStore, member: PacketRow) -> Acceptance:
    """Decide one member within the caller's station write transaction."""
    validate_row(member)
    validate_identifier(member.batch, 'batch')
    validate_recording_channel(catalog.require(member.batch).channel, member.channel)
    stored = rows.get(member.batch, member.position)
    if stored is None:
        return Acceptance('new', rows.insert(member))
    if stored.as_packet_row() == member:
        return Acceptance('replayed', stored)
    rows.record_conflict(member)
    return Acceptance('conflict', stored)
