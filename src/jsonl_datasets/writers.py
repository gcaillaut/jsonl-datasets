from pathlib import Path
import gzip
import orjson
from typing import Union, Optional, BinaryIO


class JsonlDatasetWriter:
    """
    Writes JSONL data into shard files with optional gzip compression.
    Automatically rotates shards based on a max sample threshold.
    """

    def __init__(
        self,
        directory: Union[str, Path],
        max_shard_size: int = 1_000_000,
        shard_prefix: str = "shard_",
        compress: bool = False,
    ):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

        self.max_shard_size = max_shard_size
        self.shard_prefix = shard_prefix
        self.compress = compress
        self._open_fn = gzip.open if compress else open

        self.current_shard_file: Optional[BinaryIO] = None
        self.current_shard: int = 0
        self.num_samples: int = 0

        self._initialize_shard()

    def _initialize_shard(self):
        """Determine where to resume writing (existing shard or new one)."""
        ext = "jsonl.gz" if self.compress else "jsonl"
        pattern = f"{self.shard_prefix}*.{ext}"
        existing_shards = sorted(self.directory.glob(pattern))

        if existing_shards:
            last_shard = existing_shards[-1]
            with self._open_fn(last_shard, mode="rt", encoding="utf-8") as f:
                count = sum(1 for _ in f)

            self.current_shard = len(existing_shards) - 1
            self.num_samples = count

            if count >= self.max_shard_size:
                self.current_shard += 1
                self.num_samples = 0
        else:
            self.current_shard = 0
            self.num_samples = 0

        self._open_current_shard_file()

    def _open_current_shard_file(self):
        """Open the current shard file for writing or appending."""
        self.close()

        ext = "jsonl.gz" if self.compress else "jsonl"
        filename = f"{self.shard_prefix}{self.current_shard:05d}.{ext}"
        filepath = Path(self.directory, filename)

        mode = "ab"
        self.current_shard_file = self._open_fn(filepath, mode)

    def _rotate_shard_if_needed(self):
        """Start a new shard if the sample limit is reached."""
        if self.num_samples >= self.max_shard_size:
            self.current_shard += 1
            self.num_samples = 0
            self._open_current_shard_file()

    def write_line(self, item: object):
        """
        Serialize and write a single item as JSON to the current shard.
        """
        self._rotate_shard_if_needed()  # need to do this before writing to ensure we don't exceed max_shard_size and we don’t write an empty file.
        if self.num_samples > 0:
            self.current_shard_file.write(b"\n")
        self.current_shard_file.write(orjson.dumps(item))
        self.num_samples += 1

    def flush(self):
        if self.current_shard_file:
            self.current_shard_file.flush()

    def close(self):
        """Close the current open shard file."""
        if self.current_shard_file:
            self.current_shard_file.close()
            self.current_shard_file = None

    def __enter__(self) -> "JsonlDatasetWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
