#!/usr/bin/env python3
"""Heartbeat health checks for swift_light's proxy tier.

The proxy has to know which data servers are alive before it routes a request
to one.  This module is that check, and nothing else:

    heartbeat(server)       send one heartbeat to ``server``; return its reply
    log_heartbeat(server)   heartbeat, then keep the reply as that server's last message
    get_status()            the full list of data server status, keyed by server
    is_alive(server)        whether one server counts as alive right now

and one classmethod that boots a monitor from the cluster:

    Heartbeat.from_ring(path)   load the ring, heartbeat every data server once

``server`` is the data server's base URL (for example ``http://10.0.0.3:8080``),
and it is also the key the status is filed under.  The proxy's HTTP handlers
call this module; it knows nothing about HTTP itself.

Transport
---------
``heartbeat()`` opens one HTTP GET to ``server + "/heartbeat"`` and reads the
JSON reply the data server answers with::

    {"server": "http://10.0.0.5:8080", "message": "alive"}

The returned message is that reply's ``"message"`` field.  ``send_heartbeat()``
is the only method here that touches the network, so swapping the transport (or
stubbing it in a test) means replacing just that one method; everything above it
-- the status record, the kept message, the return value -- stays the same.  A
server that does not answer within ``DEFAULT_HEARTBEAT_TIMEOUT`` seconds,
refuses the connection, or sends something that is not the expected JSON counts
as down: the error is caught in ``heartbeat()`` and recorded as unhealthy.

History
-------
The heartbeat history lives **in memory only**; there is no log file.  To keep
it small, the monitor stores just the last message it got back from each data
server, never a growing list: ``log_heartbeat()`` keeps one entry per server and
replaces it on the next check.

Startup
-------
On boot the proxy does not yet know who its data servers are -- but the ring
does.  ``Heartbeat.from_ring()`` loads the persisted ring, reads its node list
(the nodes are the data servers' URLs), and immediately heartbeats every one of
them, so ``get_status()`` is populated from the first moment.  A ring caught
half-way through a rebalance is accepted, so a proxy can always start.

Status
------
``get_status()`` answers with a dict keyed by server URL::

    {
        "http://10.0.0.3:8080": {
            "healthy": True,
            "message": "ok",
            "last_checked": "2026-10-02T14:03:21-04:00",
        },
        ...
    }

Only servers that have been checked appear.  ``message`` is the last reply kept
by ``log_heartbeat()``, and is ``None`` for a server that was only probed with
``heartbeat()``.  ``heartbeat()`` never raises for a server that is simply down:
the failure is caught, recorded as unhealthy, and returned as the message, so
one dead data server cannot take the proxy with it.

Aliveness
---------
A check is a point in time; "is this server alive?" is not.  The proxy has to
keep hearing from a data server, so a server is considered **failed** once its
last message is ``DEFAULT_FAIL_AFTER_SECONDS`` (10 seconds) old or older.
``healthy`` in ``get_status()`` is therefore computed when the status is read,
not when the check ran, and ``is_alive(server)`` asks the same question about
one server.  A server never checked, a check that failed, and a message that
has gone stale all count as failed.

Usage
-----
::

    from swift_light.proxy_server.heartbeat import Heartbeat

    # On startup: read the ring's data server list and ping every one of them.
    beat = Heartbeat.from_ring("ring.json")

    beat.is_alive("http://10.0.0.3:8080")        # alive right now?
    beat.get_status()                            # everything recorded so far

    # Or drive single servers yourself:
    beat.log_heartbeat("http://10.0.0.4:8080")   # check + keep the reply
    beat.heartbeat("http://10.0.0.5:8080")       # check only

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime
from typing import Any

from swift_light.proxy_server.consistent_hashing_ring import ConsistentHashingRing

__all__ = [
    "Heartbeat",
    "DEFAULT_FAIL_AFTER_SECONDS",
    "DEFAULT_HEARTBEAT_TIMEOUT",
]

DEFAULT_FAIL_AFTER_SECONDS = 10.0
"""A server is considered failed once its last message is this many seconds old."""

DEFAULT_HEARTBEAT_TIMEOUT = 5.0
"""Seconds to wait for a data server's heartbeat reply before giving up."""


class Heartbeat:
    """Health checks for the data servers, one heartbeat at a time."""

    __slots__ = ("_status", "_history", "_checked_at", "_fail_after_seconds")

    def __init__(
        self,
        *,
        fail_after_seconds: float = DEFAULT_FAIL_AFTER_SECONDS,
    ) -> None:
        if fail_after_seconds <= 0:
            raise ValueError(
                f"fail_after_seconds ({fail_after_seconds}) must be positive"
            )
        self._fail_after_seconds = float(fail_after_seconds)
        self._status: dict[str, dict[str, Any]] = {}
        self._history: dict[str, str] = {}
        self._checked_at: dict[str, float] = {}

    @classmethod
    def from_ring(
        cls,
        path: str | os.PathLike[str] | None = None,
        *,
        fail_after_seconds: float = DEFAULT_FAIL_AFTER_SECONDS,
    ) -> "Heartbeat":
        """Start the monitor from the cluster's persisted ring.

        Loads the ring from ``path`` (default: ``ring.json`` beside the ring
        module), then immediately heartbeats every data server it lists -- the
        ring's node names are the data servers' URLs -- so the status is
        populated from the first moment.  Returns the ready monitor.

        The ring is read with ``complete=False``: a proxy must be able to boot
        even if a rebalance was interrupted half-way.
        """
        ring = ConsistentHashingRing.load(path, complete=False)
        monitor = cls(fail_after_seconds=fail_after_seconds)
        for server in ring.nodes:
            monitor.log_heartbeat(server)
        return monitor

    # -- checking -----------------------------------------------------------

    def heartbeat(self, server: str) -> str:
        """Send one heartbeat to ``server`` and record the reply.

        Returns the reply message.  A server that cannot be reached is *not* an
        error here: the failure is recorded as an unhealthy status and returned
        as the message, so the caller always gets an answer.
        """
        try:
            healthy, message = self.send_heartbeat(server)
        except Exception as exc:  # a dead server must not take the proxy down
            healthy, message = False, f"{type(exc).__name__}: {exc}"
        self._status[server] = {
            "healthy": healthy,
            "last_checked": _local_timestamp(),
        }
        self._checked_at[server] = time.monotonic()
        return message

    def log_heartbeat(self, server: str) -> str:
        """Heartbeat ``server`` and keep the reply as its last message.

        The monitor keeps only the latest message per server, in memory, so this
        replaces whatever the server returned before.  Returns the reply
        message, so a caller can act on it as well as log it.
        """
        message = self.heartbeat(server)
        self._history[server] = message
        return message

    def is_alive(self, server: str) -> bool:
        """Whether ``server`` counts as alive right now.

        Alive means the last check succeeded *and* the last message is younger
        than ``fail_after_seconds``.  A server that was never checked, one whose
        last check failed, and one that has gone quiet all count as failed.
        """
        record = self._status.get(server)
        if record is None or not record["healthy"]:
            return False
        return self._age(server) < self._fail_after_seconds

    def get_status(self) -> dict[str, dict[str, Any]]:
        """The full list of data server status, keyed by server URL.

        Only servers that have been checked so far appear.  ``healthy`` is
        computed when this is called, not when the check ran: a server whose
        last message is ``fail_after_seconds`` or older is reported failed, and
        so is a server whose last check failed.  The result is a copy, so
        changing it does not change what the monitor recorded.
        """
        status: dict[str, dict[str, Any]] = {}
        for server, record in self._status.items():
            status[server] = {
                "healthy": self.is_alive(server),
                "message": self._history.get(server),
                "last_checked": record["last_checked"],
            }
        return status

    def _age(self, server: str) -> float:
        """Seconds since ``server`` was last checked."""
        return time.monotonic() - self._checked_at[server]

    # -- transport ----------------------------------------------------------

    def send_heartbeat(
        self,
        server: str,
        *,
        timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
    ) -> tuple[bool, str]:
        """Ask ``server`` whether it is alive; return ``(healthy, message)``.

        Opens one HTTP GET to ``server + "/heartbeat"`` and reads the JSON the
        data server answers with, for example
        ``{"server": "http://10.0.0.5:8080", "message": "alive"}``.  A 2xx
        reply is healthy, and its ``"message"`` field is the message.

        Only a reachable, well-formed reply returns normally.  A refused
        connection, a timeout, a non-2xx status and a body that is not the
        expected JSON all raise, which :meth:`heartbeat` catches and records as
        an unhealthy status -- a down data server never takes the proxy down.
        """
        url = server.rstrip("/") + "/heartbeat"
        with urllib.request.urlopen(url, timeout=timeout) as reply:
            payload = json.loads(reply.read().decode("utf-8"))
        return True, str(payload["message"])


def _local_timestamp() -> str:
    """The current local time as ISO-8601, e.g. ``2026-10-02T14:03:21-04:00``."""
    return datetime.now().astimezone().isoformat(timespec="seconds")
