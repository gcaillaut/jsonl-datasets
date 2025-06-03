import gzip
from pathlib import Path
from typing import Iterator, Union


def iter_lines(path: Union[str, Path]) -> Iterator[str]:
    """
    Lazily yield decoded lines from a file (optionally gzip-compressed).

    Args:
        path: Path to a plain text or `.gz` compressed file.

    Yields:
        Each line as a decoded UTF-8 string (without trailing newline).
    """
    path = Path(path)
    open_f = gzip.open if path.suffix == ".gz" else open
    mode = "rt"
    encoding = "utf-8"

    with open_f(path, mode, encoding=encoding) as f:
        yield from (line.rstrip("\n") for line in f)
