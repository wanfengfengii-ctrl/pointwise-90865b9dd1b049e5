from dataclasses import dataclass

from ledger.rows import ReceivedRow, RowStore
from recorder.packet import validate_row
from runs.closure import Manifest


@dataclass(frozen=True)
class CompletenessResult:
    manifest: Manifest
    rows: tuple[ReceivedRow, ...]
    missing: tuple[int, ...]
    invalid: tuple[int, ...]


def evaluate_completeness(manifest: Manifest, rows: RowStore) -> CompletenessResult:
    received, missing, invalid = [], [], []
    seen_origins = set()
    for position in manifest.positions:
        try:
            row = rows.get(manifest.batch, position)
            if row is None:
                missing.append(position)
                continue
            validate_row(row.as_packet_row())
        except (TypeError, ValueError):
            invalid.append(position)
            continue
        if (row.batch, row.position, row.channel) != (manifest.batch, position, manifest.channel) or row.origin in seen_origins:
            invalid.append(position)
            continue
        seen_origins.add(row.origin)
        received.append(row)
    return CompletenessResult(manifest, tuple(received), tuple(missing), tuple(invalid))
