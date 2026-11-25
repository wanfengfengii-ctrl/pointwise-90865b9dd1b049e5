from dataclasses import dataclass

from intake.custody import CustodyError, CustodyStore
from intake.evidence import verify_packet_coverage
from intake.read import RecorderPort
from ledger.rows import RowStore
from recorder.ack import AckStatus


@dataclass(frozen=True)
class ResumePlan:
    packet_id: str
    action: str


def plan_resume(custody: CustodyStore, packet_id: str) -> ResumePlan:
    progress = custody.get_progress(packet_id)
    if progress is None:
        return ResumePlan(packet_id, 'fetch')
    if progress.state == 'confirmed':
        return ResumePlan(packet_id, 'done')
    if progress.state == 'pending':
        return ResumePlan(packet_id, 'query')
    if all(slot.secured for slot in progress.members):
        return ResumePlan(packet_id, 'send')
    return ResumePlan(packet_id, 'fetch')


def settle_confirmed(custody: CustodyStore, rows: RowStore, packet_id: str) -> None:
    with custody.connection:
        custody.connection.execute('BEGIN IMMEDIATE')
        progress = custody.require_progress(packet_id)
        if not verify_packet_coverage(progress, rows):
            raise CustodyError(f'cannot settle {packet_id} without complete member evidence')
        custody.set_ack_state(packet_id, 'confirmed')


def reconcile_confirmation(port: RecorderPort, custody: CustodyStore, rows: RowStore,
                           packet_id: str) -> AckStatus:
    try:
        status = port.confirmation_status(packet_id)
    except ConnectionError:
        return AckStatus.UNKNOWN
    if status is AckStatus.CONFIRMED:
        settle_confirmed(custody, rows, packet_id)
    return status
