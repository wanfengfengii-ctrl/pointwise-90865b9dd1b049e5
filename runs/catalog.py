import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Run:
    batch: str
    run_id: str
    channel: str
    unit: str


class UnknownRun(LookupError):
    pass


class RunCatalog:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def initialize(self) -> None:
        self.connection.execute('''
            CREATE TABLE IF NOT EXISTS runs (
                batch TEXT PRIMARY KEY,
                run_id TEXT NOT NULL UNIQUE,
                channel TEXT NOT NULL,
                unit TEXT NOT NULL
            )
        ''')

    def find(self, batch: str) -> Run | None:
        row = self.connection.execute(
            'SELECT batch, run_id, channel, unit FROM runs WHERE batch = ?', (batch,)
        ).fetchone()
        return Run(*row) if row is not None else None

    def require(self, batch: str) -> Run:
        run = self.find(batch)
        if run is None:
            raise UnknownRun(batch)
        return run

    def put_if_absent(self, run: Run) -> Run:
        self.connection.execute('''
            INSERT INTO runs (batch, run_id, channel, unit) VALUES (?, ?, ?, ?)
            ON CONFLICT(batch) DO NOTHING
        ''', (run.batch, run.run_id, run.channel, run.unit))
        return self.require(run.batch)
