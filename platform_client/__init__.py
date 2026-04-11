"""
platform_client — tiny UDS client for platformd.

This is the surface that services link against. It speaks line-delimited
JSON-RPC to the daemon over a Unix domain socket. The same three public
methods as PlatformClient (acquire, drop_to_scaling_only, scale_hint)
are preserved, so the service author's code stays the same. The
service does NOT construct a scope locally — scope is owned by
platformd and looked up by authenticated service_id.
"""

from platform_client.client import Client

__all__ = ["Client"]
