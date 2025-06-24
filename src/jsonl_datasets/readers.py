from pathlib import Path
import itertools
from typing import Iterator, Union
import orjson

from jsonl_datasets.io import iter_lines


def round_robin(*iterables: Iterator[str]) -> Iterator[str]:
    """Yield items from each iterable in round-robin fashion, skipping exhausted ones."""
    for group in itertools.zip_longest(*iterables, fillvalue=None):
        for item in group:
            if item is not None:
                yield item


class JsonlDatasetReader:
    def __init__(
        self,
        directory: Union[str, Path],
        read_strategy: str = "sequential",
        skip_on_error: bool = False,
    ):
        self.directory = Path(directory)
        if not self.directory.is_dir():
            raise ValueError(
                f"Provided path '{self.directory}' is not a valid directory."
            )

        self.files = sorted(self.directory.glob("*.jsonl*"))  # sort for reproducibility
        if not self.files:
            raise FileNotFoundError(f"No .jsonl files found in '{self.directory}'.")

        if read_strategy not in {"sequential", "round_robin"}:
            raise ValueError("read_strategy must be 'sequential' or 'round_robin'")
        self.read_strategy = read_strategy
        self.skip_on_error = skip_on_error

        self._len = None

    def _line_iterators(self) -> Iterator[str]:
        """Create iterators for lines from each file."""
        return (iter_lines(f) for f in self.files)

    def __iter__(self) -> Iterator[dict]:
        """Yield parsed JSON objects from all files."""
        if self.read_strategy == "sequential":
            lines_iter = itertools.chain.from_iterable(self._line_iterators())
        elif self.read_strategy == "round_robin":
            lines_iter = round_robin(*self._line_iterators())
        else:
            raise ValueError(f"Invalid read_strategy: {self.read_strategy}")

        for line in lines_iter:
            try:
                item = orjson.loads(line)
                yield item
            except orjson.JSONDecodeError as e:
                if not self.skip_on_error:
                    raise e

    def __len__(self) -> int:
        """Return the number of JSON objects across all files, caching the result."""
        if self._len is None:
            self._len = sum(1 for _ in self)
        return self._len
