from pathlib import Path
import itertools
from typing import Iterator, Union
import orjson

from jsonl_datasets.io import iter_lines, ParallelLineIterator


def round_robin(*iterables: Iterator[str]) -> Iterator[str]:
    """Yield items from each iterable in round-robin fashion, skipping exhausted ones."""
    for group in itertools.zip_longest(*iterables, fillvalue=None):
        for item in group:
            if item is not None:
                yield item


# class JsonlDatasetReader:
#     def __init__(
#         self,
#         directory: Union[str, Path],
#         read_strategy: str = "parallel",
#         skip_on_error: bool = False,
#         num_workers: int = -1,
#     ):
#         self.directory = Path(directory)
#         if not self.directory.is_dir():
#             raise ValueError(
#                 f"Provided path '{self.directory}' is not a valid directory."
#             )

#         self.files = sorted(
#             self.directory.rglob("*.jsonl*")
#         )  # sort for reproducibility
#         if not self.files:
#             raise FileNotFoundError(f"No .jsonl files found in '{self.directory}'.")

#         if read_strategy not in {"sequential", "round_robin", "parallel"}:
#             raise ValueError(
#                 "read_strategy must be 'parallel', 'sequential' or 'round_robin'"
#             )
#         self.read_strategy = read_strategy
#         self.skip_on_error = skip_on_error
#         self.num_workers = num_workers

#         self._len = None

#     def _line_iterators(self) -> Iterator[str]:
#         """Create iterators for lines from each file."""
#         return (iter_lines(f) for f in self.files)

#     def __iter__(self) -> Iterator[dict]:
#         """Yield parsed JSON objects from all files."""
#         if self.read_strategy == "sequential":
#             lines_iter = itertools.chain.from_iterable(self._line_iterators())
#         elif self.read_strategy == "round_robin":
#             lines_iter = round_robin(*self._line_iterators())
#         elif self.read_strategy == "parallel":
#             pass
#         else:
#             raise ValueError(f"Invalid read_strategy: {self.read_strategy}")

#         if self.read_strategy == "parallel":
#             with ParallelLineIterator(self.files, max_workers=self.num_workers) as it:
#                 for line in it:
#                     try:
#                         item = orjson.loads(line)
#                         yield item
#                     except orjson.JSONDecodeError as e:
#                         if not self.skip_on_error:
#                             raise e
#         else:
#             for line in lines_iter:
#                 try:
#                     item = orjson.loads(line)
#                     yield item
#                 except orjson.JSONDecodeError as e:
#                     if not self.skip_on_error:
#                         raise e

#     def __len__(self) -> int:
#         """Return the number of JSON objects across all files, caching the result."""
#         if self._len is None:
#             self._len = sum(1 for _ in self)
#         return self._len

#     def stream(self, limit=None) -> Iterator[dict]:
#         if limit is not None:
#             from itertools import islice

#             return islice(iter(self), limit)
#         return iter(self)


class JsonlDatasetReader:
    def __init__(
        self,
        directory: Union[str, Path],
        read_strategy: str = "parallel",
        skip_on_error: bool = False,
        num_workers: int = -1,
    ):
        self.directory = Path(directory)
        if not self.directory.is_dir():
            raise ValueError(f"'{self.directory}' is not a valid directory.")
        self.files = sorted(self.directory.rglob("*.jsonl*"))
        if not self.files:
            raise FileNotFoundError(f"No .jsonl files found in '{self.directory}'.")

        if read_strategy not in {"sequential", "round_robin", "parallel"}:
            raise ValueError(
                "read_strategy must be 'sequential', 'round_robin', or 'parallel'"
            )

        self.read_strategy = read_strategy
        self.skip_on_error = skip_on_error
        self.num_workers = num_workers
        self._len = None

    def _line_iterators(self) -> Iterator[str]:
        return (iter_lines(f) for f in self.files)

    def _line_stream(self) -> Iterator[str]:
        if self.read_strategy == "sequential":
            return itertools.chain.from_iterable(self._line_iterators())
        elif self.read_strategy == "round_robin":
            return round_robin(*self._line_iterators())
        elif self.read_strategy == "parallel":
            # Use generator context for correct cleanup
            def generator():
                with ParallelLineIterator(
                    self.files, max_workers=self.num_workers
                ) as lines:
                    for line in lines:
                        yield line

            return generator()
        else:
            raise ValueError(f"Invalid read_strategy: {self.read_strategy}")

    def stream(self, limit: int = None) -> Iterator[dict]:
        lines = self._line_stream()
        if limit is not None:
            lines = itertools.islice(lines, limit)
        for line in lines:
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:
                if not self.skip_on_error:
                    raise

    def __iter__(self) -> Iterator[dict]:
        return self.stream()

    def __len__(self) -> int:
        if self._len is None:
            self._len = sum(1 for _ in self)
        return self._len
