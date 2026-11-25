import sqlite3
from collections.abc import Sequence

from intake.custody import CustodyStore
from intake.read import RecorderPort
from ledger.rows import RowStore
from recorder.packet import Origin, member_digest, validate_packet_id
from runs.rules import validate_identifier


def build_member_evidence(custody: CustodyStore, rows: RowStore,
                          packet_id: str) -> tuple[tuple[str, int, str], int]:
    """Build the persistent receipt that authorizes removing one delivered copy.

    The evidence only exists when exactly one member of the retained packet has
    a durable, row-matching intake receipt. Returns the evidence item and the
    physical position of the member still waiting to be handed over.
    """
    validate_packet_id(packet_id)
    progress = custody.get_progress(packet_id)
    if progress is None or progress.state != 'collecting':
        raise CompactionIneligible(packet_id)
    secured = [slot for slot in progress.members if slot.secured]
    waiting = [slot for slot in progress.members if not slot.secured]
    if len(secured) != 1 or len(waiting) != 1:
        raise CompactionIneligible(packet_id)
    slot = secured[0]
    stored = rows.get(slot.batch, slot.line)
    if stored is None or stored.origin != Origin(packet_id, slot.position):
        raise CompactionIneligible(packet_id)
    digest = member_digest(stored.as_packet_row())
    if digest != slot.digest:
        raise CompactionIneligible(packet_id)
    return (packet_id, slot.position, digest), waiting[0].position


class CompactionIneligible(ValueError):
    pass


def request_compaction(connection: sqlite3.Connection, port: RecorderPort, operation_id: str,
                       left_id: str, right_id: str) -> str:
    validate_packet_id(operation_id)
    validate_identifier(left_id, 'packet id')
    validate_identifier(right_id, 'packet id')
    if left_id == right_id:
        raise ValueError('compaction requires two distinct original packets')
    custody, rows = CustodyStore(connection), RowStore(connection)
    try:
        left_evidence, _ = build_member_evidence(custody, rows, left_id)
        right_evidence, _ = build_member_evidence(custody, rows, right_id)
    except CompactionIneligible:
        return 'ineligible'
    evidence: Sequence[tuple[str, int, str]] = (left_evidence, right_evidence)
    try:
        return port.compact_packets(operation_id, left_id, right_id, evidence)
    except ConnectionError:
        # The device may already have committed; the same operation id reopens
        # the query once connectivity is restored.
        return 'pending'
