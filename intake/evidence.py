from dataclasses import dataclass

from intake.custody import CustodyError, CustodyStore, PacketProgress
from ledger.rows import RowStore
from recorder.packet import Origin, PacketRow, member_digest


@dataclass(frozen=True)
class MemberEvidence:
    origin: Origin
    batch: str
    line: int
    digest: str


def record_member_evidence(custody: CustodyStore, rows: RowStore, member: PacketRow) -> MemberEvidence:
    stored = rows.get(member.batch, member.position)
    if stored is None or stored.as_packet_row() != member:
        raise CustodyError(f'no matching received row for {member.origin!r}')
    evidence = MemberEvidence(member.origin, member.batch, member.position, member_digest(member))
    custody.record_member_evidence(evidence.origin.packet_id, evidence.origin.position,
                                   evidence.batch, evidence.line, evidence.digest)
    return evidence


def verify_packet_coverage(progress: PacketProgress, rows: RowStore) -> bool:
    if not progress.members:
        return False
    for expected_position, slot in enumerate(progress.members, 1):
        if not slot.secured or slot.position != expected_position:
            return False
        stored = rows.get(slot.batch, slot.line)
        if stored is None or stored.origin != Origin(progress.packet_id, slot.position):
            return False
        if member_digest(stored.as_packet_row()) != slot.digest:
            return False
    return True
