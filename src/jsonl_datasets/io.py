import gzip
from pathlib import Path
from typing import Iterator, Union, List
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty, Full
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
    def __init__(
        self, paths: List[Union[str, Path]], max_workers=-1, queue_size=10_000
    ):
        self.paths = paths
        self.max_workers = (
            min(32, os.cpu_count(), len(paths))
            if max_workers == -1
            else min(max_workers, len(paths))
        )
        self.queue_size = queue_size
        self.q = Queue(maxsize=queue_size)
        self.err_q = Queue()
        self._stop_event = threading.Event()
        self._executor = None
        self._SENTINEL = object()
        self._done_files = 0
        self._num_files = len(paths)

    def __enter__(self):
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        for path in self.paths:
            self._executor.submit(self._line_feeder, path)
        return self._run()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._stop_event.set()
        if self._executor:
            self._executor.shutdown(wait=True)

    def _line_feeder(self, file_path):
        try:
            for line in iter_lines(file_path):
                if self._stop_event.is_set():
                    return
                self.q.put(line)
        except Exception as e:
            self.err_q.put(e)
        finally:
            self.q.put(self._SENTINEL)

    def _run(self) -> Iterator[str]:
        while self._done_files < self._num_files:
            if not self.err_q.empty():
                raise self.err_q.get()

            try:
                item = self.q.get(timeout=0.1)
                if item is self._SENTINEL:
                    self._done_files += 1
                else:
                    yield item
            except Empty:
                continue
