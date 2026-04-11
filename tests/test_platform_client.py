from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from platform_api import (
    BlockSpec,
    BlockType,
    CapabilityManifest,
    Credentials,
    PlatformClient,
    PrivilegeDroppedError,
    PrivilegeState,
    QuotaExceededError,
    UnknownBlockError,
)


@dataclass
class FakeEngine:
    """Records provision() calls without touching Pulumi or Docker."""

    provisioned: list[BlockSpec] = None  # type: ignore[assignment]
    destroyed: bool = False

    def __post_init__(self) -> None:
        if self.provisioned is None:
            self.provisioned = []

    def provision(
        self, spec: BlockSpec, *, existing_leases: dict[str, BlockSpec]
    ) -> Credentials:
        self.provisioned.append(spec)
        return Credentials(
            block_type=spec.block_type,
            name=spec.name,
            host="127.0.0.1",
            port=10000 + len(self.provisioned),
            username="user",
            password="pw",
            database="db" if spec.block_type == BlockType.TRANSACTIONAL_STORE else None,
        )

    def destroy(self) -> None:
        self.destroyed = True


def _manifest(**overrides: Any) -> CapabilityManifest:
    defaults: dict[str, Any] = {
        "service_id": "demo",
        "allowed_blocks": {
            BlockType.TRANSACTIONAL_STORE,
            BlockType.OBJECT_STORE,
            BlockType.EPHEMERAL_KV_CACHE,
        },
        "max_blocks": 4,
    }
    defaults.update(overrides)
    return CapabilityManifest(**defaults)


def _client(**engine_overrides: Any) -> tuple[PlatformClient, FakeEngine]:
    engine = FakeEngine(**engine_overrides)
    client = PlatformClient(
        service_id="demo", manifest=_manifest(), engine=engine
    )
    return client, engine


def test_initial_state_is_acquiring():
    client, _ = _client()
    assert client.state is PrivilegeState.ACQUIRING


def test_acquire_returns_credentials_and_records_lease():
    client, engine = _client()
    creds = client.acquire(
        BlockType.TRANSACTIONAL_STORE, name="photos", database="photos"
    )
    assert creds.block_type is BlockType.TRANSACTIONAL_STORE
    assert creds.name == "photos"
    assert creds.host == "127.0.0.1"
    assert len(engine.provisioned) == 1
    assert engine.provisioned[0].name == "photos"
    assert engine.provisioned[0].profile == "minimal"  # default


def test_acquire_accepts_string_block_type():
    client, _ = _client()
    creds = client.acquire("object-store", name="images")
    assert creds.block_type is BlockType.OBJECT_STORE


def test_acquire_idempotent_by_name():
    """Re-acquiring the same name is a no-op and returns cached credentials."""
    client, engine = _client()
    first = client.acquire(BlockType.EPHEMERAL_KV_CACHE, name="sessions")
    second = client.acquire(BlockType.EPHEMERAL_KV_CACHE, name="sessions")
    assert first == second
    assert len(engine.provisioned) == 1


def test_acquire_type_conflict_on_same_name():
    client, _ = _client()
    client.acquire(BlockType.EPHEMERAL_KV_CACHE, name="oops")
    with pytest.raises(ValueError, match="already exists"):
        client.acquire(BlockType.OBJECT_STORE, name="oops")


def test_drop_blocks_subsequent_acquire():
    client, _ = _client()
    client.acquire(BlockType.TRANSACTIONAL_STORE, name="photos")
    client.drop_to_scaling_only()
    assert client.state is PrivilegeState.OPERATIONAL
    with pytest.raises(PrivilegeDroppedError):
        client.acquire(BlockType.OBJECT_STORE, name="images")


def test_scale_hint_rejected_before_drop():
    client, _ = _client()
    client.acquire(BlockType.TRANSACTIONAL_STORE, name="photos")
    with pytest.raises(PrivilegeDroppedError):
        client.scale_hint("photos", load_factor=0.7)


def test_scale_hint_allowed_after_drop():
    client, _ = _client()
    client.acquire(BlockType.TRANSACTIONAL_STORE, name="photos")
    client.drop_to_scaling_only()
    client.scale_hint("photos", load_factor=0.7)  # no raise


def test_scale_hint_unknown_lease():
    client, _ = _client()
    client.drop_to_scaling_only()
    with pytest.raises(ValueError, match="unknown lease"):
        client.scale_hint("never-acquired", load_factor=0.5)


def test_double_drop_is_idempotent():
    client, _ = _client()
    client.drop_to_scaling_only()
    client.drop_to_scaling_only()  # no raise
    assert client.state is PrivilegeState.OPERATIONAL


def test_capability_manifest_rejects_unauthorized_block():
    engine = FakeEngine()
    restricted = CapabilityManifest(
        service_id="demo",
        allowed_blocks={BlockType.TRANSACTIONAL_STORE},
        max_blocks=4,
    )
    client = PlatformClient(
        service_id="demo", manifest=restricted, engine=engine
    )
    with pytest.raises(UnknownBlockError):
        client.acquire(BlockType.OBJECT_STORE, name="images")
    assert engine.provisioned == []


def test_quota_enforcement():
    engine = FakeEngine()
    small = CapabilityManifest(
        service_id="demo",
        allowed_blocks={BlockType.TRANSACTIONAL_STORE},
        max_blocks=2,
    )
    client = PlatformClient(service_id="demo", manifest=small, engine=engine)
    client.acquire(BlockType.TRANSACTIONAL_STORE, name="a", database="a")
    client.acquire(BlockType.TRANSACTIONAL_STORE, name="b", database="b")
    with pytest.raises(QuotaExceededError):
        client.acquire(BlockType.TRANSACTIONAL_STORE, name="c", database="c")


def test_manifest_service_id_mismatch_rejected():
    engine = FakeEngine()
    wrong = CapabilityManifest(
        service_id="other",
        allowed_blocks={BlockType.TRANSACTIONAL_STORE},
    )
    with pytest.raises(ValueError, match="service_id"):
        PlatformClient(service_id="demo", manifest=wrong, engine=engine)


def test_shutdown_transitions_state():
    client, engine = _client()
    client.shutdown()
    assert client.state is PrivilegeState.SHUTDOWN
    assert not engine.destroyed


def test_shutdown_with_destroy_calls_engine():
    client, engine = _client()
    client.shutdown(destroy=True)
    assert engine.destroyed


def test_shutdown_idempotent():
    client, _ = _client()
    client.shutdown()
    client.shutdown()  # no raise


def test_credentials_dsn_postgres():
    creds = Credentials(
        block_type=BlockType.TRANSACTIONAL_STORE,
        name="photos",
        host="127.0.0.1",
        port=15432,
        username="user",
        password="pw",
        database="appdb",
    )
    assert creds.as_dsn() == "postgresql://user:pw@127.0.0.1:15432/appdb"


def test_credentials_dsn_redis_without_password():
    creds = Credentials(
        block_type=BlockType.EPHEMERAL_KV_CACHE,
        name="cache",
        host="127.0.0.1",
        port=16379,
    )
    assert creds.as_dsn() == "redis://127.0.0.1:16379/0"


def test_credentials_dsn_minio():
    creds = Credentials(
        block_type=BlockType.OBJECT_STORE,
        name="images",
        host="127.0.0.1",
        port=19000,
    )
    assert creds.as_dsn() == "http://127.0.0.1:19000"
