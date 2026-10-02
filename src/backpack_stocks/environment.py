from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

from dotenv import dotenv_values


def load_environment(path: str | Path, names: Iterable[str]) -> None:
    """Load only credentials needed by this command; never search parent directories.

    Exported environment variables take precedence. No interpolation or shell evaluation.
    """
    target = Path(path)
    if not target.exists():
        return
    values = dotenv_values(target, interpolate=False)
    for name in names:
        value = values.get(name)
        if value is not None:
            os.environ.setdefault(name, value)
