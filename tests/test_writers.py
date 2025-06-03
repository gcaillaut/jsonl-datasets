import tempfile
import shutil
import os
import gzip
import orjson
from pathlib import Path
import pytest
from jsonl_datasets.writers import JsonlDatasetWriter


def test_writer_creates_directory_and_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = JsonlDatasetWriter(tmpdir, max_shard_size=2)
        writer.write_line({"a": 1})
        writer.write_line({"b": 2})
        writer.close()
        files = list(Path(tmpdir).glob("*.jsonl"))
        assert len(files) == 1
        with open(files[0], "rb") as f:
            lines = f.read().split(b"\n")
            assert orjson.loads(lines[0]) == {"a": 1}
            assert orjson.loads(lines[1]) == {"b": 2}


def test_writer_shard_rotation():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = JsonlDatasetWriter(tmpdir, max_shard_size=1)
        writer.write_line({"a": 1})
        writer.write_line({"b": 2})
        writer.close()
        files = sorted(Path(tmpdir).glob("*.jsonl"))
        assert len(files) == 2
        with open(files[0], "rb") as f:
            assert orjson.loads(f.read()) == {"a": 1}
        with open(files[1], "rb") as f:
            assert orjson.loads(f.read()) == {"b": 2}


def test_writer_gzip_compression():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = JsonlDatasetWriter(tmpdir, max_shard_size=2, compress=True)
        writer.write_line({"a": 1})
        writer.write_line({"b": 2})
        writer.close()
        files = list(Path(tmpdir).glob("*.jsonl.gz"))
        assert len(files) == 1
        with gzip.open(files[0], "rb") as f:
            lines = f.read().split(b"\n")
            assert orjson.loads(lines[0]) == {"a": 1}
            assert orjson.loads(lines[1]) == {"b": 2}


def test_writer_context_manager():
    with tempfile.TemporaryDirectory() as tmpdir:
        with JsonlDatasetWriter(tmpdir, max_shard_size=2) as writer:
            writer.write_line({"a": 1})
            writer.write_line({"b": 2})
        files = list(Path(tmpdir).glob("*.jsonl"))
        assert len(files) == 1
        with open(files[0], "rb") as f:
            lines = f.read().split(b"\n")
            assert orjson.loads(lines[0]) == {"a": 1}
            assert orjson.loads(lines[1]) == {"b": 2}


def test_writer_resume_existing_shard():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = JsonlDatasetWriter(tmpdir, max_shard_size=2)
        writer.write_line({"a": 1})
        writer.close()
        # Reopen writer, should append to existing shard
        writer2 = JsonlDatasetWriter(tmpdir, max_shard_size=2)
        writer2.write_line({"b": 2})
        writer2.close()
        files = list(Path(tmpdir).glob("*.jsonl"))
        assert len(files) == 1
        with open(files[0], "rb") as f:
            lines = f.read().split(b"\n")
            assert orjson.loads(lines[0]) == {"a": 1}
            assert orjson.loads(lines[1]) == {"b": 2}
