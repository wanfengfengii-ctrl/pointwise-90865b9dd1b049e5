import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass

from runs.catalog import RunCatalog
from runs.rules import validate_identifier, validate_manifest_positions


@dataclass(frozen=True)
class Manifest:
    batch: str
    manifest_id: str
    channel: str
    unit: str
    positions: tuple[int, ...]


class ManifestConflict(ValueError):
    pass


def initialize_manifest_store(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS manifests (
            batch TEXT PRIMARY KEY REFERENCES runs(batch),
            manifest_id TEXT NOT NULL UNIQUE,
            channel TEXT NOT NULL,
            unit TEXT NOT NULL,
            positions TEXT NOT NULL
        )
    ''')


def get_manifest(connection: sqlite3.Connection, batch: str) -> Manifest | None:
    stored = connection.execute('''
        SELECT batch, manifest_id, channel, unit, positions FROM manifests WHERE batch = ?
    ''', (batch,)).fetchone()
    if stored is None:
        return None
    batch, manifest_id, channel, unit, encoded_positions = stored
    positions = tuple(int(part, 16) for part in json.loads(encoded_positions))
    return Manifest(batch, manifest_id, channel, unit, positions)


def close_batch(catalog: RunCatalog, batch: str, positions: Sequence[int]) -> Manifest:
    validate_identifier(batch, 'batch')
    ordered = validate_manifest_positions(positions)
    connection = catalog.connection
    with connection:
        connection.execute('BEGIN IMMEDIATE')
        run = catalog.require(batch)
        stored = get_manifest(connection, batch)
        if stored is not None:
            if stored.positions != ordered:
                raise ManifestConflict(batch)
            return stored
        manifest = Manifest(batch, f'close:{batch}', run.channel, run.unit, ordered)
        connection.execute('''
            INSERT INTO manifests (batch, manifest_id, channel, unit, positions)
            VALUES (?, ?, ?, ?, ?)
        ''', (batch, manifest.manifest_id, run.channel, run.unit,
              json.dumps([hex(position) for position in ordered])))
        return manifest
