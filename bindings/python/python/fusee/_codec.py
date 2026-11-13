"""Values on the wire: JSON text, or raw bytes; and how annotations decide
what a value is encoded from and decoded into.

Plain data (dicts, lists, strings, numbers, booleans, None) encodes with
one call to a shared encoder. Dataclasses, pydantic models, msgspec
structs, enums, datetimes and UUIDs go through its ``default`` hook, which
runs only for them. Annotations are read once, when a definition is
made, and turned into a decoder that builds the annotated type back.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime as _dt
import enum
import json
import re
import types as _types
import typing
import uuid as _uuid
from typing import Any, Callable, Optional, Union

from ._errors import CodecError, FuseeError, RejectedError


def _default(v: Any) -> Any:
    if dataclasses.is_dataclass(v) and not isinstance(v, type):
        return {f.name: getattr(v, f.name) for f in dataclasses.fields(v)}
    dump = getattr(v, "model_dump", None)  # pydantic v2
    if dump is not None and callable(dump):
        return dump(mode="json")
    if hasattr(v, "__struct_fields__"):  # msgspec
        import msgspec

        return msgspec.to_builtins(v)
    if isinstance(v, enum.Enum):
        return v.value
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
        return v.isoformat()
    if isinstance(v, _uuid.UUID):
        return str(v)
    raise TypeError(f"{type(v).__name__} is not JSON")


# One encoder for every payload: json.dumps with keyword arguments builds a
# new encoder per call. NaN and infinities are refused: other SDKs cannot
# read them.
_ENCODER = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_default)
json_text = _ENCODER.encode


def encode(v: Any) -> bytes:
    return json_text(v).encode()


def wire(v: Any) -> Union[str, bytes]:
    """A payload for the engine: ``bytes`` as they are, anything else JSON."""
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v)
    return json_text(v)


def tagged(v: Union[str, bytes]) -> bytes:
    """A payload in its recorded form: raw bytes follow a zero byte."""
    return v.encode() if isinstance(v, str) else b"\0" + v


def decode(b: Optional[bytes]) -> Any:
    """A recorded (tagged) payload."""
    if not b:
        return None
    if b[0] == 0:
        return bytes(b[1:])
    return json.loads(b)


def from_wire(p: Union[None, str, bytes]) -> Any:
    """A payload from the typed boundary: ``str`` is JSON, ``bytes`` raw."""
    if p is None:
        return None
    if isinstance(p, str):
        return json.loads(p) if p else None
    return p


def wire_text(p: Union[None, str, bytes]) -> str:
    if p is None:
        return ""
    return p if isinstance(p, str) else p.decode("utf-8", "replace")


def tagged_to_wire(b: Optional[bytes]) -> Union[None, str, bytes]:
    if not b:
        return None
    return bytes(b[1:]) if b[0] == 0 else b.decode("utf-8", "replace")


def unencodable_path(v: Any, path: str = "$", depth: int = 0) -> Optional[tuple]:
    """Where in ``v`` a value JSON cannot encode is: ``(path, type name)``.
    Runs after an encoding failed, never on success."""
    if depth > 64:
        return None
    if v is None or isinstance(v, (str, bool, int)):
        return None
    if isinstance(v, float):
        return None if v == v and v not in (float("inf"), float("-inf")) else (path, "float " + repr(v))
    if isinstance(v, dict):
        for k, x in v.items():
            if not isinstance(k, (str, int, float, bool)) and k is not None:
                return (f"{path}[{k!r}]", f"key of type {type(k).__name__}")
            r = unencodable_path(x, f"{path}.{k}" if isinstance(k, str) and k.isidentifier() else f"{path}[{k!r}]",
                                 depth + 1)
            if r:
                return r
        return None
    if isinstance(v, (list, tuple)):
        for i, x in enumerate(v):
            r = unencodable_path(x, f"{path}[{i}]", depth + 1)
            if r:
                return r
        return None
    try:
        inner = _default(v)
    except TypeError:
        return (path, type(v).__name__)
    except Exception:  # noqa: BLE001 - a model that cannot dump itself
        return (path, type(v).__name__)
    return unencodable_path(inner, path, depth + 1)


def codec_error(what: str, v: Any, e: BaseException, step: Optional[str] = None) -> CodecError:
    found = unencodable_path(v)
    if found:
        path, tname = found
        return CodecError(
            f"{what} cannot be encoded as JSON: {tname} at {path}. Return plain data (dict, list, str, "
            "int, float, bool, None), a dataclass or a pydantic model",
            step=step, path=path)
    return CodecError(f"{what} cannot be encoded as JSON: {e}", step=step)


# ------------------------------------------------------------- durations

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$")
_UNIT = {"ms": 1, "s": 1e3, "m": 6e4, "h": 36e5, "d": 864e5}


def ms(d: Union[None, int, float, str, _dt.timedelta]) -> Optional[float]:
    """A duration in milliseconds: a number (ms), ``"500ms"``, ``"10s"``,
    ``"5m"``, ``"2h"``, ``"1d"``, or a ``timedelta``."""
    if d is None:
        return None
    if isinstance(d, bool):
        raise RejectedError(f"bad duration {d!r}")
    if isinstance(d, (int, float)):
        v = float(d)
    elif isinstance(d, _dt.timedelta):
        v = d.total_seconds() * 1000
    else:
        m = _DURATION.match(d)
        if not m:
            raise RejectedError(f"bad duration {d!r}: use a number of milliseconds or 500ms, 10s, 5m, 2h, 1d")
        v = float(m.group(1)) * _UNIT[m.group(2)]
    if v != v:
        raise RejectedError(f"bad duration {d!r}")
    # "Forever" (``float("inf")``, or anything past it) is as late as a
    # deadline goes: callers add it to the clock and hand the engine an
    # unsigned 64-bit time, which infinity is not.
    return min(v, FOREVER_MS)


#: The longest duration: about 146 million years, and ``now + FOREVER_MS``
#: still fits the engine's 64-bit times.
FOREVER_MS = float(2**62)


def at_ms(t: Union[int, float, _dt.datetime]) -> int:
    """A point in time as Unix milliseconds."""
    if isinstance(t, _dt.datetime):
        return int(t.timestamp() * 1000)
    return int(t)


# ------------------------------------------------------------- decoders by annotation

Decoder = Callable[[Any], Any]
#: ``X | Y`` (Python 3.10 and later).
_UNION_TYPE = getattr(_types, "UnionType", None)


def _identity(v: Any) -> Any:
    return v


def _is_typeddict(tp: Any) -> bool:
    return isinstance(tp, type) and issubclass(tp, dict) and hasattr(tp, "__total__")


def decoder_for(tp: Any, globalns: Optional[dict] = None) -> Optional[Decoder]:
    """A function building values of ``tp`` from decoded JSON, or ``None``
    when JSON values already are of that type (plain data, ``Any``)."""
    if tp is None or tp is Any or tp is object or tp is type(None):
        return None
    if isinstance(tp, str):
        try:
            tp = eval(tp, globalns or {}, vars(typing))  # noqa: S307 - an annotation
        except Exception:  # noqa: BLE001 - an annotation this cannot resolve: plain data
            return None
    if tp in (str, int, float, bool, dict, list, bytes):
        return None
    if _is_typeddict(tp):
        return None
    origin = typing.get_origin(tp)
    if origin is Union or (_UNION_TYPE is not None and origin is _UNION_TYPE):
        args = [a for a in typing.get_args(tp) if a is not type(None)]
        if len(args) == 1:
            inner = decoder_for(args[0], globalns)
            if inner is None:
                return None
            return lambda v: None if v is None else inner(v)
        return None
    if origin in (list, tuple, set, frozenset):
        args = typing.get_args(tp)
        inner = decoder_for(args[0], globalns) if args else None
        if inner is None:
            return None if origin is list else (lambda v: origin(v))  # type: ignore[misc]
        return lambda v: origin(inner(x) for x in v)  # type: ignore[misc]
    if origin is dict:
        args = typing.get_args(tp)
        inner = decoder_for(args[1], globalns) if len(args) == 2 else None
        if inner is None:
            return None
        return lambda v: {k: inner(x) for k, x in v.items()}
    if origin is not None:
        return None
    if not isinstance(tp, type):
        return None
    if dataclasses.is_dataclass(tp):
        return _dataclass_decoder(tp)
    validate = getattr(tp, "model_validate", None)  # pydantic v2
    if validate is not None:
        return validate
    if hasattr(tp, "__struct_fields__"):  # msgspec
        import msgspec

        return lambda v: msgspec.convert(v, tp)
    if issubclass(tp, enum.Enum):
        return tp
    if tp is _dt.datetime:
        return _dt.datetime.fromisoformat
    if tp is _dt.date:
        return _dt.date.fromisoformat
    if tp is _uuid.UUID:
        return _uuid.UUID
    return None


def _dataclass_decoder(tp: type) -> Decoder:
    try:
        hints = typing.get_type_hints(tp)
    except Exception:  # noqa: BLE001 - unresolvable hints: build from plain data
        hints = {}
    fields = [(f.name, decoder_for(hints.get(f.name))) for f in dataclasses.fields(tp) if f.init]

    def build(v: Any) -> Any:
        if isinstance(v, tp):
            return v
        if not isinstance(v, dict):
            raise CodecError(f"expected an object for {tp.__name__}, not {type(v).__name__}")
        kw = {}
        for name, dec in fields:
            if name in v:
                x = v[name]
                kw[name] = dec(x) if dec is not None and x is not None else x
        return tp(**kw)

    return build


def bytes_or_b64(v: Any) -> bytes:
    return v if isinstance(v, bytes) else base64.b64decode(v)
