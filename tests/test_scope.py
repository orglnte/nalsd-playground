from __future__ import annotations

from pathlib import Path

import pytest

from platform_api import BlockType, ServiceScope, load_scope


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "scope.toml"
    p.write_text(body)
    return p


def test_load_scope_valid(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
service_id = "demo"
allowed_blocks = ["transactional-store", "object-store"]
max_blocks = 3
""",
    )
    scope = load_scope(path)
    assert isinstance(scope, ServiceScope)
    assert scope.service_id == "demo"
    assert scope.allowed_blocks == {
        BlockType.TRANSACTIONAL_STORE,
        BlockType.OBJECT_STORE,
    }
    assert scope.max_blocks == 3


def test_load_scope_default_max_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
service_id = "demo"
allowed_blocks = ["ephemeral-kv-cache"]
""",
    )
    scope = load_scope(path)
    assert scope.max_blocks == 16


def test_load_scope_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_scope(tmp_path / "nope.toml")


def test_load_scope_malformed_toml(tmp_path: Path) -> None:
    path = _write(tmp_path, "service_id = [unterminated")
    with pytest.raises(ValueError, match="malformed scope file"):
        load_scope(path)


def test_load_scope_missing_required_key(tmp_path: Path) -> None:
    path = _write(tmp_path, 'service_id = "demo"\n')
    with pytest.raises(ValueError, match="missing required keys"):
        load_scope(path)


def test_load_scope_unknown_block_type(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
service_id = "demo"
allowed_blocks = ["transactional-store", "quantum-annealer"]
""",
    )
    with pytest.raises(ValueError, match="unknown block type"):
        load_scope(path)
