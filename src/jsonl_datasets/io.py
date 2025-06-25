import gzip
from pathlib import Path
from typing import Iterator, Union, List
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import threading
import os


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


class ParallelLineIterator:
    def __init__(self, paths, max_workers=-1, queue_size=10_000):
        self.paths = paths
        self.max_workers = max_workers
        self.queue_size = queue_size
        self.q = Queue(maxsize=queue_size)
        self.err_q = Queue()
        self.done_files = 0
        self._stop_event = threading.Event()

    def __iter__(self) -> Iterator[str]:
        _SENTINEL = object()
        num_files = len(self.paths)
        max_workers = (
            min(32, os.cpu_count(), num_files)
            if self.max_workers == -1
            else min(self.max_workers, num_files)
        )

        def line_feeder(file_path):
            try:
                for line in iter_lines(file_path):
                    if self._stop_event.is_set():
                        return
                    self.q.put(line)
            except Exception as e:
                self.err_q.put(e)
            finally:
                self.q.put(_SENTINEL)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for path in self.paths:
                executor.submit(line_feeder, path)

            while self.done_files < num_files:
                if not self.err_q.empty():
                    raise self.err_q.get()

                try:
                    item = self.q.get(timeout=0.1)
                    if item is _SENTINEL:
                        self.done_files += 1
                    else:
                        yield item
                except Empty:
                    continue

    def close(self):
        self._stop_event.set()


def multiple_files_lines_iterator(
    paths: List[Union[str, Path]], max_workers=-1
) -> Iterator[str]:
    it = ParallelLineIterator(paths, max_workers)
    try:
        yield from it
    finally:
        it.close()
