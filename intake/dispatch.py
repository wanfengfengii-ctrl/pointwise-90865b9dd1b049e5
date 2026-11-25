import sqlite3
from collections.abc import Callable
from dataclasses import dataclass

from intake.custody import CustodyError, CustodyStore
from intake.evidence import record_member_evidence, verify_packet_coverage
from intake.read import RecorderPort, fetch_packet
from intake.resume import plan_resume, reconcile_confirmation, settle_confirmed
from ledger.accept import accept_row
from ledger.rows import RowStore
from recorder.ack import AckStatus
from recorder.device_store import PacketUnavailable
from recorder.packet import Packet, PacketRow, validate_packet_id
from runs.catalog import RunCatalog
from runs.rules import validate_identifier, validate_recording_channel


@dataclass(frozen=True)
class IngestResult:
    packet_id: str
    state: str
    conflicts: tuple[int, ...] = ()


class IntakeInterrupted(RuntimeError):
    pass


def dispatch_members(custody: CustodyStore, catalog: RunCatalog, rows: RowStore, packet: Packet,
                     before_member: Callable[[PacketRow], None] | None = None) -> tuple[int, ...]:
    for member in packet.rows:
        validate_identifier(member.batch, 'batch')
        validate_recording_channel(catalog.require(member.batch).channel, member.channel)
    connection = custody.connection
    with connection:
        connection.execute('BEGIN IMMEDIATE')
        progress = custody.open_packet(packet)
    secured = {slot.position for slot in progress.members if slot.secured}
    conflicts = []
    for member in packet.rows:
        physical_position = member.origin.position
        if physical_position in secured:
            continue
        if before_member is not None:
            before_member(member)
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            accepted = accept_row(catalog, rows, member)
            if accepted.state == 'conflict':
                conflicts.append(physical_position)
            else:
                record_member_evidence(custody, rows, member)
    return tuple(conflicts)


def finish_packet(port: RecorderPort, custody: CustodyStore, rows: RowStore, packet_id: str) -> IngestResult:
    with custody.connection:
        custody.connection.execute('BEGIN IMMEDIATE')
        progress = custody.require_progress(packet_id)
        if not verify_packet_coverage(progress, rows):
            raise CustodyError(f'cannot confirm {packet_id} without complete member evidence')
        state = custody.set_ack_state(packet_id, 'pending')
    if state == 'confirmed':
        return IngestResult(packet_id, state)
    try:
        status = port.acknowledge(packet_id)
    except ConnectionError:
        return IngestResult(packet_id, 'pending')
    if status is AckStatus.CONFIRMED:
        settle_confirmed(custody, rows, packet_id)
        return IngestResult(packet_id, 'confirmed')
    return IngestResult(packet_id, 'pending')


def _resume_ack(port: RecorderPort | None, custody: CustodyStore, rows: RowStore,
                packet_id: str, action: str) -> IngestResult:
    if action == 'done':
        return IngestResult(packet_id, 'confirmed')
    if port is None:
        return IngestResult(packet_id, 'pending' if action == 'query' else 'awaiting_source')
    if action == 'query':
        status = reconcile_confirmation(port, custody, rows, packet_id)
        if status is AckStatus.CONFIRMED:
            return IngestResult(packet_id, 'confirmed')
        if status is not AckStatus.RETAINED:
            return IngestResult(packet_id, 'pending')
    return finish_packet(port, custody, rows, packet_id)


def ingest(connection: sqlite3.Connection, port: RecorderPort | None, packet_id: str, *,
           before_member: Callable[[PacketRow], None] | None = None) -> IngestResult:
    validate_packet_id(packet_id)
    custody, rows = CustodyStore(connection), RowStore(connection)
    plan = plan_resume(custody, packet_id)
    if plan.action != 'fetch':
        return _resume_ack(port, custody, rows, packet_id, plan.action)
    if port is None:
        return IngestResult(packet_id, 'awaiting_source')
    try:
        packet = fetch_packet(port, packet_id)
    except PacketUnavailable:
        # A concurrent intake may have committed the safe handoff since the first read.
        plan = plan_resume(custody, packet_id)
        if plan.action == 'fetch':
            raise
        return _resume_ack(port, custody, rows, packet_id, plan.action)
    except ConnectionError:
        return IngestResult(packet_id, 'awaiting_source')
    conflicts = dispatch_members(custody, RunCatalog(connection), rows, packet, before_member)
    if conflicts:
        return IngestResult(packet_id, 'conflict', conflicts)
    return finish_packet(port, custody, rows, packet_id)
