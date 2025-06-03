import os
import tempfile
import shutil
import json
import pytest
from pathlib import Path
from jsonl_datasets import JsonlDatasetReader


def create_jsonl_file(path, records):
    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def test_init_valid_directory_and_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os.path.join(tmpdir, "a.jsonl")
        file2 = os.path.join(tmpdir, "b.jsonl")
        create_jsonl_file(file1, [{"a": 1}])
        create_jsonl_file(file2, [{"b": 2}])
        reader = JsonlDatasetReader(tmpdir)
        assert len(reader.files) == 2


def test_init_invalid_directory():
    with pytest.raises(ValueError):
        JsonlDatasetReader("/does/not/exist")


def test_init_no_jsonl_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(FileNotFoundError):
            JsonlDatasetReader(tmpdir)


def test_init_invalid_strategy():
    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os.path.join(tmpdir, "a.jsonl")
        create_jsonl_file(file1, [{"a": 1}])
        with pytest.raises(ValueError):
            JsonlDatasetReader(tmpdir, read_strategy="invalid")


def test_iter_sequential():
    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os.path.join(tmpdir, "a.jsonl")
        file2 = os.path.join(tmpdir, "b.jsonl")
        create_jsonl_file(file1, [{"a": 1}, {"a": 2}])
        create_jsonl_file(file2, [{"b": 3}])
        reader = JsonlDatasetReader(tmpdir, read_strategy="sequential")
        data = list(reader)
        assert data == [{"a": 1}, {"a": 2}, {"b": 3}]


def test_iter_round_robin():
    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os.path.join(tmpdir, "a.jsonl")
        file2 = os.path.join(tmpdir, "b.jsonl")
        create_jsonl_file(file1, [{"a": 1}, {"a": 2}])
        create_jsonl_file(file2, [{"b": 3}, {"b": 4}])
        reader = JsonlDatasetReader(tmpdir, read_strategy="round_robin")
        data = list(reader)
        # L'ordre round robin : a1, b3, a2, b4
        assert data == [{"a": 1}, {"b": 3}, {"a": 2}, {"b": 4}]


def test_len_caching():
    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = os.path.join(tmpdir, "a.jsonl")
        create_jsonl_file(file1, [{"a": 1}, {"a": 2}])
        reader = JsonlDatasetReader(tmpdir)
        l1 = len(reader)
        l2 = len(reader)
        assert l1 == 2
        assert l2 == 2
