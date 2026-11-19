"""Shared fixtures for the Swift Light functional tests (standard library only).

These tests own their temporary files and optional loopback servers. They never
start deploy.py, use its fixed ports, or change a running cluster's data.
"""
from __future__ import annotations

import json
import sys
import tempfile
import urllib.error
import urllib.request
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

# This file lives directly under src/test, so its parent is the src directory
# that holds the swift_light package.
SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from swift_light.client.client import Client, ClientError
from swift_light.data_server.storage import DataServerConfig, DataStore
from swift_light.data_server.data_HTTP_server import DataHTTPServer
from swift_light.proxy_server.consistent_hashing_ring import ConsistentHashingRing
from swift_light.proxy_server.proxy_HTTP_server import ProxyHTTPServer

__all__ = [
    "SRC_ROOT",
    "Client",
    "ClientError",
    "ConsistentHashingRing",
    "DataHTTPServer",
    "DataServerConfig",
    "DataStore",
    "ProxyHTTPServer",
    "cluster",
    "data_node",
    "data_servers",
    "http_request",
    "json_request",
    "lookup",
    "proxy_server",
]


def lookup(servers):
    return {"servers": tuple(servers), "replicate_count": 2, "slot": 0}


@contextmanager
def data_node(
    root,
    *,
    own_address: str = "http://127.0.0.1:1",
    proxy_address: str = "http://127.0.0.1:8000",
    atomic: bool = True,
):
    """One real data server on an OS-assigned port, serving from ``root``.

    Yields ``(server, base_url)``.  Boot handoff reconciliation is disabled so
    an isolated node never contacts an external proxy.
    """
    config = DataServerConfig(own_address, proxy_address)
    server = DataHTTPServer(
        config, store_root=root, atomic=atomic, host="127.0.0.1", port=0,
    )
    try:
        # Handoff discovery is unrelated to these tests. Do not contact any
        # external proxy when starting an isolated storage server.
        with patch.object(DataHTTPServer, "request_handoff_reconcile", return_value=False):
            server.start()
        yield server, f"http://127.0.0.1:{server.port}"
    finally:
        server.close()


@contextmanager
def data_servers(count=3):
    """Real HTTP handlers and stores, with ephemeral ports and no boot handoffs."""
    with tempfile.TemporaryDirectory(prefix="swift-light-http-") as directory, ExitStack() as stack:
        nodes = [
            stack.enter_context(data_node(Path(directory) / str(index)))
            for index in range(count)
        ]
        yield [server for server, _ in nodes], [url for _, url in nodes]


@contextmanager
def proxy_server(nodes, *, replicate_count=2, vnode_count=8, fail_after_seconds=30.0):
    """A real proxy on OS-assigned ports, routing with a ring built from ``nodes``.

    Yields ``(proxy, ring_path)``; read the ports back from ``proxy.user_port``
    and ``proxy.data_port``.
    """
    with tempfile.TemporaryDirectory(prefix="swift-light-proxy-") as directory:
        ring_path = Path(directory) / "ring.json"
        ConsistentHashingRing.build(
            len(nodes), replicate_count, nodes=nodes, vnode_count=vnode_count,
            path=ring_path,
        )
        proxy = ProxyHTTPServer(
            ring_path=ring_path, user_port=0, data_port=0, host="127.0.0.1",
            fail_after_seconds=fail_after_seconds,
        )
        proxy.start()
        try:
            yield proxy, ring_path
        finally:
            proxy.close()


@contextmanager
def cluster(count=3, *, replicate_count=2, vnode_count=8):
    """Real data servers plus a proxy whose ring points at them.

    Yields ``(servers, urls, proxy, ring_path)``, with ``servers[i]`` reachable
    at ``urls[i]``.  Client and proxy route through the same pieces production
    uses; only the addresses and the temporary directories are test-owned.
    """
    with data_servers(count) as (servers, urls):
        with proxy_server(
            urls, replicate_count=replicate_count, vnode_count=vnode_count
        ) as (proxy, ring_path):
            yield servers, urls, proxy, ring_path


def http_request(url, *, method="GET", data=None, headers=None, timeout=5.0):
    """One HTTP request; a 4xx/5xx answer is returned, not raised.

    Returns ``(status, headers, body)``.  Only a connection that cannot be made
    raises, which is what the caller usually wants to tell apart.
    """
    request = urllib.request.Request(
        url, data=data, method=method, headers=headers or {}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as reply:
            return reply.status, dict(reply.headers), reply.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def json_request(url, *, method="GET", data=None, headers=None, timeout=5.0):
    """One HTTP request whose answer is JSON; returns ``(status, payload)``."""
    status, _, body = http_request(
        url, method=method, data=data, headers=headers, timeout=timeout
    )
    return status, (json.loads(body) if body else None)
