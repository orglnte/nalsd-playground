"""
platform_client — tiny UDS client for platformd.

This is the surface that services link against. It speaks line-delimited
JSON-RPC to the daemon over a Unix domain socket and exposes three
methods: acquire, drop_to_scaling_only, scale_hint. The service does
NOT construct a scope locally — scope is owned by platformd and looked
up by the authenticated service_id.
"""

from platform_client.client import Client

__all__ = ["Client"]
