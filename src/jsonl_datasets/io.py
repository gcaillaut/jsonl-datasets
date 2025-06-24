import gzip
from pathlib import Path
from typing import Iterator, Union, List
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty


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


_SENTINEL = object()


def multiple_files_lines_iterator(
    paths: List[Union[str, Path]], max_workers=-1
) -> Iterator[str]:
    def line_feeder(file_path: str, q: Queue, err_q: Queue):
        try:
            for line in iter_lines(file_path):
                q.put(line)
        except Exception as e:
            err_q.put(e)
        finally:
            q.put(_SENTINEL)

    num_files = len(paths)
    if max_workers == -1:
        max_workers = num_files
    else:
        max_workers = min(max_workers, num_files)
    q = Queue(maxsize=10_000)
    err_q = Queue(maxsize=max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for file in paths:
            executor.submit(line_feeder, file, q, err_q)

        done_files = 0
        while done_files < num_files:
            try:
                item = q.get(timeout=0.1)
                if item is _SENTINEL:
                    done_files += 1
                else:
                    yield item
            except Empty:
                if not err_q.empty():
                    raise err_q.get()
                continue
