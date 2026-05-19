from __future__ import annotations

import re
import uuid as _uuid
from typing import Any


_BYTES_REPR_RE = re.compile(r"^b'([^']+)'$")


def normalize_uuid_str(value: Any) -> str:
    """Normalize various uuid-like values to a canonical UUID string.

    Handles:
      - uuid.UUID
      - bytes (utf-8)
      - strings that look like a bytes repr: "b'...uuid...'"
    """

    if isinstance(value, _uuid.UUID):
        return str(value)

    if isinstance(value, bytes):
        s = value.decode("utf-8")
    else:
        s = str(value)

    s = s.strip()
    m = _BYTES_REPR_RE.match(s)
    if m:
        s = m.group(1)

    # Validate + canonicalize (lowercase + hyphenated).
    return str(_uuid.UUID(s))


def maybe_normalize_uuid_str(value: Any) -> str | None:
    try:
        return normalize_uuid_str(value)
    except Exception:
        return None
