#!/usr/bin/env python3
"""The swift_light proxy: two HTTP ports, one process.

A proxy is the tier clients talk to and the tier that talks to the data
servers.  It listens on two ports:

    port 8000   user information        -- client requests (server lookup, today)
    port 8010   data server information -- heartbeat, today

The client port answers one question so far -- *which data servers may hold
this key?* -- and it answers it from the ring alone:

    GET  /servers?key=<partition key>   the ordered data-server list for a key
    GET  /nodes                         every data server in the cluster

The node list is what a booting data server asks for so it can collect the
handoffs held for it and return the ones it holds.  The read/write routing that
will use the fan is not built yet: any other request is answered ``404``.

The data port is where the proxy answers *which data servers are alive?*  That
question already has an owner -- the monitor in :mod:`heartbeat` -- so this
module is only a thin HTTP skin over it:

    GET  /heartbeat   the health of every data server the proxy knows
    POST /heartbeat   re-probe every data server now, then answer as above
    GET  /nodes       every data server in the cluster, for handoff reconciliation

A re-probe is also where reconnection is noticed.  A data server that had been
counted failed and answers again is asked to reconcile its handoffs
(``POST <data server>/handoff``): the proxy routed its objects to substitutes
while it was unreachable, and those entries have to come home.  This is the
same reconciliation the data server runs at boot.

Which data servers exist is not a second list kept here.  Startup loads the
cluster's persisted ring once and keeps it on the proxy, then heartbeats every
node it lists -- so the proxy's view of membership and the ring's can never
drift apart.  The proxy only ever *reads* the ring: it is built once at cluster
setup, and the proxy never writes it back.

Transport
---------
``Heartbeat.send_heartbeat`` opens one HTTP GET to the data server's own
``/heartbeat`` endpoint and reads its JSON reply, so a re-probe here is real
network I/O.  The data server's side of that endpoint is
:mod:`swift_light.data_server.data_HTTP_server`.

Concurrency
-----------
``ThreadingHTTPServer`` gives each request its own thread, so the two ports are
served at once and a slow heartbeat cannot stall a client request.  The shared
monitor is locked where it is written -- a re-probe -- because two re-probes at
once would only race each other for no benefit.  Reading the status is not
locked: a reader sees a complete record either way.

Usage
-----
::

    from swift_light.proxy_server.proxy_HTTP_server import ProxyHTTPServer

    proxy = ProxyHTTPServer(ring_path="ring.json")   # probes every data server at boot

    proxy.user_port      # 8000 -- the client port
    proxy.data_port      # 8010 -- the data server port

    proxy.serve_forever()          # block, serving both ports

While it serves, the two ports answer::

    GET  /servers?key=photos/cat.jpg   # -> the ordered data-server list for a key
    GET  /heartbeat                    # -> the health of every data server

Tests start and stop the server without blocking, and let the OS pick free
ports::

    proxy = ProxyHTTPServer(ring_path=path, user_port=0, data_port=0)
    proxy.start()                  # returns immediately, serves in the background
    ...
    proxy.close()

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import json
import os
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from swift_light.proxy_server.consistent_hashing_ring import ConsistentHashingRing
from swift_light.proxy_server.heartbeat import (
    DEFAULT_FAIL_AFTER_SECONDS,
    Heartbeat,
)

__all__ = [
    "ProxyHTTPServer",
    "UserHandler",
    "DataServerHandler",
    "DEFAULT_USER_PORT",
    "DEFAULT_DATA_PORT",
    "DEFAULT_HOST",
    "DEFAULT_RECONCILE_TIMEOUT",
]

DEFAULT_USER_PORT = 8000
"""Port the proxy answers client (user) requests on."""

DEFAULT_DATA_PORT = 8010
"""Port the proxy answers data-server information (heartbeat) on."""

DEFAULT_HOST = ""
"""Bind address: the empty string binds every interface."""

DEFAULT_RECONCILE_TIMEOUT = 2.0
"""Seconds to wait for a reconnected data server to accept a reconcile request."""


class _ProxyRequestServer(ThreadingHTTPServer):
    """One listening socket of the proxy, carrying the proxy it belongs to.

    Handlers reach shared state through ``self.server.proxy``.  The heartbeat
    monitor lives on the proxy, not on a handler, so both ports and every
    request thread see the same one.
    """

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        proxy: "ProxyHTTPServer",
    ) -> None:
        super().__init__(address, handler_class)
        self.proxy = proxy


class _JsonHandler(BaseHTTPRequestHandler):
    """Shared plumbing for the proxy's HTTP handlers.

    Both ports answer JSON, stay quiet on stderr, and reach the proxy through
    ``self.server``; this is all of that, written once.
    """

    server_version = "swift_light-proxy/0.1"
    protocol_version = "HTTP/1.1"

    def _path(self) -> str:
        """The request path, query stripped and trailing slashes removed."""
        return urlparse(self.path).path.rstrip("/")

    @property
    def _proxy(self) -> "ProxyHTTPServer":
        """The proxy this request belongs to (``self.server`` is ours)."""
        return self.server.proxy

    def _send_json(self, status: int, payload: Any) -> None:
        """Reply with ``payload`` as JSON, with a length for keep-alive."""
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str) -> None:
        self._send_json(status, {"error": message})

    def _send_not_found(self) -> None:
        self._send_error_json(404, f"no such endpoint: {self.path}")

    def _nodes(self) -> None:
        """Answer ``GET /nodes``: every data server, straight from the ring."""
        self._send_json(200, {"nodes": list(self._proxy.ring.nodes)})

    def log_message(self, format: str, *args: Any) -> None:
        """Stay quiet -- request lines on stderr are noise, not information."""
        return


class DataServerHandler(_JsonHandler):
    """Serves the proxy's view of data-server health on the data port (8010).

    Routes:

        GET  /heartbeat   the status of every data server the proxy knows
        POST /heartbeat   re-probe every data server, then report the status
        GET  /nodes       every data server in the cluster, straight from the ring

    A re-probe also detects reconnection: a server that had failed and answers
    is asked to run its handoff reconciliation again.

    Anything else is a ``404``.  A method with no route is left to
    ``BaseHTTPRequestHandler``, which answers ``501 Not Implemented``.
    """

    def do_GET(self) -> None:
        path = self._path()
        if path == "/heartbeat":
            self._send_json(200, {"servers": self._proxy.heartbeat_status()})
        elif path == "/nodes":
            self._nodes()
        else:
            self._send_not_found()

    def do_POST(self) -> None:
        if self._path() == "/heartbeat":
            self._send_json(200, {"servers": self._proxy.refresh_heartbeats()})
        else:
            self._send_not_found()


class UserHandler(_JsonHandler):
    """The client-facing handler (port 8000).

    Two routes so far:

        GET /servers?key=<partition key>   the data servers that may hold the key
        GET /nodes                         every data server, straight from the ring

    The answer is the ring's ``fan(key)`` -- every data server that may hold the
    key, primary first, then its secondaries, then the handoff servers.  The
    proxy computes it from the ring it loaded at boot, with no coordination, so
    every proxy gives the same answer and no data server is asked anything.

    ``servers[0]`` is the primary, ``servers[1:replicate_count]`` the
    secondaries, and ``servers[replicate_count:]`` the handoff servers; the
    response carries ``replicate_count`` so the caller can slice them off.

    The read/write routing itself is not built yet: any other request is a
    ``404``.
    """

    def do_GET(self) -> None:
        path = self._path()
        if path == "/servers":
            self._servers()
        elif path == "/nodes":
            self._nodes()
        else:
            self._send_not_found()

    def _servers(self) -> None:
        """Answer ``GET /servers?key=<partition key>`` from the ring."""
        keys = parse_qs(urlparse(self.path).query).get("key")
        if not keys:
            self._send_error_json(400, "missing required query parameter 'key'")
            return
        key = keys[0]
        ring = self._proxy.ring
        self._send_json(
            200,
            {
                "key": key,
                "slot": ring.slot_for_key(key),
                "replicate_count": ring.replicate_count,
                "servers": list(ring.fan(key)),
            },
        )


class ProxyHTTPServer:
    """The swift_light proxy process: one heartbeat monitor, two HTTP ports.

    Constructing it loads the ring, probes every data server once, and binds
    both ports -- so the moment the constructor returns, ``data_port`` already
    answers with real health and :attr:`ring` is ready to route with.  Nothing
    is served until :meth:`start` or :meth:`serve_forever`.

    Parameters
    ----------
    ring_path:
        The persisted ring to read membership from (default: ``ring.json``
        beside the ring module).  The file must already exist -- the ring is
        built once, at cluster setup.
    user_port / data_port:
        The client and data-server ports.  ``0`` asks the OS for a free port,
        which is what tests do; read the chosen port back from :attr:`user_port`
        and :attr:`data_port`.
    host:
        Bind address for both ports (default: every interface).
    fail_after_seconds:
        How long a data server may stay quiet before it counts as failed;
        passed straight to :class:`~swift_light.proxy_server.heartbeat.Heartbeat`.
    """

    def __init__(
        self,
        *,
        ring_path: str | os.PathLike[str] | None = None,
        user_port: int = DEFAULT_USER_PORT,
        data_port: int = DEFAULT_DATA_PORT,
        host: str = DEFAULT_HOST,
        fail_after_seconds: float = DEFAULT_FAIL_AFTER_SECONDS,
    ) -> None:
        # Loaded from its persisted file once and kept: the ring is the
        # membership for the boot probe below and every later re-probe, and it
        # is what port 8000 will route with.  complete=False so a proxy can
        # boot mid-rebalance.
        self.ring = ConsistentHashingRing.load(ring_path, complete=False)
        self.heartbeat = Heartbeat(fail_after_seconds=fail_after_seconds)
        for server in self.ring.nodes:
            self.heartbeat.log_heartbeat(server)
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._user_server = _ProxyRequestServer(
            (host, user_port), UserHandler, proxy=self
        )
        self._data_server = _ProxyRequestServer(
            (host, data_port), DataServerHandler, proxy=self
        )

    # -- ports --------------------------------------------------------------

    @property
    def user_port(self) -> int:
        """The bound client port (8000, or the OS-picked port when 0 was asked)."""
        return self._user_server.server_address[1]

    @property
    def data_port(self) -> int:
        """The bound data port (8010, or the OS-picked port when 0 was asked)."""
        return self._data_server.server_address[1]

    # -- heartbeat ----------------------------------------------------------

    def heartbeat_status(self) -> dict[str, dict[str, Any]]:
        """The data servers' health, as ``Heartbeat.get_status()`` reports it."""
        return self.heartbeat.get_status()

    def refresh_heartbeats(self) -> dict[str, dict[str, Any]]:
        """Re-probe every data server on the ring, then return the fresh status.

        The ring is the membership, exactly as at startup, so a re-probe covers
        the same servers the boot probe did.  Probes are serialized under one
        lock; two re-probes at once would only race each other.

        A server that was failed before this probe and answers it has
        reconnected.  Handoffs were routed to substitutes while it was gone, so
        it is asked to reconcile them again -- after the lock is released, so a
        slow reconcile request never delays the next re-probe.
        """
        recovered: list[str] = []
        with self._lock:
            before = self.heartbeat.get_status()
            for server in self.ring.nodes:
                was_lost = server in before and not before[server]["healthy"]
                self.heartbeat.log_heartbeat(server)
                if was_lost and self.heartbeat.is_alive(server):
                    recovered.append(server)
        for server in recovered:
            self._request_handoff_reconcile(server)
        return self.heartbeat.get_status()

    def _request_handoff_reconcile(self, server: str) -> None:
        """Ask a reconnected data server to reconcile its handoffs now.

        Best effort: the server answered a heartbeat a moment ago, but it can
        still fail or time out here, and that must not disturb the re-probe.
        The data server acknowledges before it starts collecting, so this
        returns without waiting for its peers.
        """
        request = urllib.request.Request(
            server.rstrip("/") + "/handoff", data=b"", method="POST"
        )
        try:
            with urllib.request.urlopen(
                request, timeout=DEFAULT_RECONCILE_TIMEOUT
            ):
                pass
        except Exception:  # noqa: BLE001 - best effort, like the data server's sync
            pass

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        """Serve both ports in background threads and return immediately.

        Calling it twice is a no-op, so a caller does not have to track whether
        the server is already running.
        """
        if self._threads:
            return
        self._threads = [
            threading.Thread(
                target=server.serve_forever,
                name=f"proxy-port-{server.server_address[1]}",
                daemon=True,
            )
            for server in (self._user_server, self._data_server)
        ]
        for thread in self._threads:
            thread.start()

    def serve_forever(self) -> None:
        """Serve both ports and block until interrupted, then close."""
        self.start()
        try:
            for thread in self._threads:
                thread.join()
        except KeyboardInterrupt:
            self.close()

    def shutdown(self) -> None:
        """Stop serving both ports; the sockets stay open until :meth:`close`."""
        self._user_server.shutdown()
        self._data_server.shutdown()

    def close(self) -> None:
        """Stop serving and release both sockets."""
        self.shutdown()
        self._user_server.server_close()
        self._data_server.server_close()
