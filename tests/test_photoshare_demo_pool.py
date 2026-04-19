"""Tests for the photoshare pool sizing — verifies that the app scales
its psycopg pool against capacity hints surfaced by the daemon, not
against cell constants."""

from __future__ import annotations

from photoshare.main import plan_pool
from platform_api.types import BlockType, Credentials


def _creds(**extras: object) -> Credentials:
    return Credentials(
        block_type=BlockType.TRANSACTIONAL_STORE,
        name="db",
        host="127.0.0.1",
        port=15432,
        username="u",
        password="p",
        database="d",
        extras=dict(extras),
    )


def test_plan_pool_uses_max_connections_from_extras() -> None:
    pool_min, pool_max = plan_pool(_creds(max_connections=20))
    assert pool_max == 18  # 20 - 2 headroom
    assert pool_min == 2


def test_plan_pool_scales_with_provisioned_capacity() -> None:
    pool_min, pool_max = plan_pool(_creds(max_connections=80))
    assert pool_max == 78
    assert pool_min == 2


def test_plan_pool_handles_tiny_capacity() -> None:
    pool_min, pool_max = plan_pool(_creds(max_connections=3))
    assert pool_max == 1
    assert pool_min == 1  # clamped to <= max


def test_plan_pool_falls_back_to_safe_default_when_hint_missing() -> None:
    pool_min, pool_max = plan_pool(_creds())
    # Default of 20 (postgres baseline max_connections) - 2 headroom = 18.
    assert pool_max == 18
    assert pool_min == 2
