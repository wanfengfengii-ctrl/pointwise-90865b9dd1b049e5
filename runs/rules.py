import re
from collections.abc import Sequence


_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}')


def validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f'invalid {label}: {value!r}')


def validate_registration(batch: str, channel: str, unit: str) -> None:
    validate_identifier(batch, 'batch')
    validate_identifier(channel, 'channel')
    if not isinstance(unit, str) or not unit or unit != unit.strip() or not unit.isprintable():
        raise ValueError('invalid measurement unit')


def validate_recording_channel(registered_channel: str, channel: str) -> None:
    if channel != registered_channel:
        raise ValueError(f'channel {channel!r} does not match {registered_channel!r}')


def validate_manifest_positions(positions: Sequence[int]) -> tuple[int, ...]:
    if not isinstance(positions, Sequence) or isinstance(positions, (str, bytes, bytearray)):
        raise ValueError('manifest positions must be an ordered sequence')
    ordered = tuple(positions)
    if any(type(position) is not int or position < 1 for position in ordered):
        raise ValueError('manifest positions must be positive integers')
    if len(set(ordered)) != len(ordered):
        raise ValueError('duplicate manifest position')
    return ordered
