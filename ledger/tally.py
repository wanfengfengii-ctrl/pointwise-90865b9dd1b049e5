from dataclasses import dataclass

from ledger.completeness import CompletenessResult
from ledger.rows import ReceivedRow


class BatchIncomplete(ValueError):
    def __init__(self, result: CompletenessResult):
        self.batch = result.manifest.batch
        self.missing = result.missing
        self.invalid = result.invalid
        missing = ','.join(hex(position) for position in self.missing)
        invalid = ','.join(hex(position) for position in self.invalid)
        super().__init__(f'{self.batch} incomplete (missing [{missing}], invalid [{invalid}])')


@dataclass(frozen=True)
class BatchTally:
    batch: str
    manifest_id: str
    unit: str
    rows: tuple[ReceivedRow, ...]
    total: int


def prepare_tally(result: CompletenessResult) -> BatchTally:
    if result.missing or result.invalid:
        raise BatchIncomplete(result)
    manifest = result.manifest
    return BatchTally(manifest.batch, manifest.manifest_id, manifest.unit,
                      result.rows, sum(row.value for row in result.rows))
