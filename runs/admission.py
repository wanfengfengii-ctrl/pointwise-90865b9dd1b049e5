from runs.catalog import Run, RunCatalog
from runs.rules import validate_registration


class RegistrationConflict(ValueError):
    pass


def register(catalog: RunCatalog, batch: str, channel: str, unit: str) -> Run:
    validate_registration(batch, channel, unit)
    with catalog.connection:
        catalog.connection.execute('BEGIN IMMEDIATE')
        stored = catalog.put_if_absent(Run(batch, f'run:{batch}', channel, unit))
        if (stored.channel, stored.unit) != (channel, unit):
            raise RegistrationConflict(batch)
        return stored
