#!/usr/bin/env python3
"""Handoff reconciliation for a booting swift_light data server.

When a node is unreachable a proxy asks a substitute to hold the object until
the owner comes back, and the substitute stores it in its own transfer
directory marked with the owner's address (see
:mod:`swift_light.data_server.storage`).  A substitute is therefore holding data
that belongs to somebody else, and an owner may have copies waiting on a
substitute.  This module closes both gaps when a server boots.

Boot reconciliation runs in two directions:

collect
    Ask every other data server whether it is holding handoffs for *this* one.
    Each entry is applied to this server's own store -- a real object is
    written, a delete placeholder deletes the local copy -- and then cleared on
    the substitute, so the debt is settled.

deliver
    Send every handoff this server is holding for somebody else to its
    designated owner, using the owner's normal ``/files`` endpoint, and clear it
    locally once the owner has taken it over.  This is what catches the case
    collect cannot: a server that reboots *after* the owner already booted and
    collected, and so would otherwise never hand the object back.

The list of other data servers is not guessed: it is asked of the proxy this
server reports to (``proxy_address``), whose answer is the ring's membership.

    GET <proxy_address>/nodes   ->  {"nodes": ["http://10.0.0.5:8080", ...]}

Both directions are best effort.  A peer that is down, or an entry that cannot
be moved, is recorded in the report and skipped -- reconciliation never raises
and never stops the server from serving.  It is normally run once, in the
background, from :meth:`swift_light.data_server.data_HTTP_server.DataHTTPServer.start`.

Usage
-----
::

    from swift_light.data_server.handoff import HandoffSync

    sync = HandoffSync(store, own_address, proxy_address)
    sync.reconcile()          # {"delivered": [...], "collected": [...]}

The report is a list of small dicts -- one per entry moved, or per failure.  A
moved entry carries ``peer``/``owner``, ``key`` and ``action``
(``"written"`` or ``"deleted"``); a failure carries an ``error`` instead.

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

from swift_light.data_server.storage import DataStore

__all__ = [
    "HandoffSync",
    "HandoffSyncError",
    "DEFAULT_HANDOFF_TIMEOUT",
]

DEFAULT_HANDOFF_TIMEOUT = 5.0
"""Seconds to wait for any one handoff request before giving up on it."""

_DELETE_HEADER = "X-Handoff-Delete"
"""Header the handoff read route sets when the entry is a delete placeholder."""


class HandoffSyncError(RuntimeError):
    """A handoff request reached a server but was not accepted."""


class HandoffSync:
    """Move handoffs between a booting data server and the rest of the cluster.

    Parameters
    ----------
    store:
        This server's file store -- the source of the handoffs it is holding and
        the destination of the ones it collects.
    own_address:
        This server's own URL.  It is the owner other servers file handoffs
        under, and the address the proxy's membership is compared against.
    proxy_address:
        The proxy to ask for the cluster's membership (``GET /nodes``).
    timeout:
        Seconds to wait for any one request.
    """

    def __init__(
        self,
        store: DataStore,
        own_address: str,
        proxy_address: str,
        *,
        timeout: float = DEFAULT_HANDOFF_TIMEOUT,
    ) -> None:
        self._store = store
        self._own = own_address.rstrip("/")
        self._proxy = proxy_address.rstrip("/")
        self._timeout = timeout

    # -- discovery ----------------------------------------------------------

    def peers(self) -> list[str]:
        """Every other data server in the cluster, from the proxy, unsorted."""
        status, _, body = _http("GET", self._proxy + "/nodes", timeout=self._timeout)
        if status != 200:
            raise HandoffSyncError(f"proxy {self._proxy} answered {status} for /nodes")
        nodes = json.loads(body).get("nodes", [])
        others = [str(node).rstrip("/") for node in nodes]
        return [node for node in others if node != self._own]

    # -- collect: bring my handoffs home ------------------------------------

    def collect(self) -> list[dict[str, Any]]:
        """Pull every handoff the other servers hold for this one."""
        try:
            peers = self.peers()
        except Exception as exc:  # noqa: BLE001 - best effort, report and carry on
            return [{"error": _describe(exc)}]
        report: list[dict[str, Any]] = []
        for peer in peers:
            report.extend(self._collect_from(peer))
        return report

    def _collect_from(self, peer: str) -> list[dict[str, Any]]:
        """Pull and clear everything ``peer`` holds for this server."""
        try:
            status, _, body = _http(
                "GET", _owner_url(peer, self._own), timeout=self._timeout
            )
            if status != 200:
                return [{"peer": peer, "error": f"list answered {status}"}]
            keys = json.loads(body).get("keys", [])
        except Exception as exc:  # noqa: BLE001
            return [{"peer": peer, "error": _describe(exc)}]

        report: list[dict[str, Any]] = []
        for key in keys:
            try:
                status, headers, body = _http(
                    "GET", _entry_url(peer, self._own, key), timeout=self._timeout
                )
                if status != 200:
                    report.append({"peer": peer, "key": key, "error": f"read answered {status}"})
                    continue
                is_delete = headers.get(_DELETE_HEADER.lower()) == "true"
                _apply(self._store, key, body, is_delete)
                _clear_remote(peer, self._own, key, self._timeout)
            except Exception as exc:  # noqa: BLE001
                report.append({"peer": peer, "key": key, "error": _describe(exc)})
            else:
                report.append(
                    {"peer": peer, "key": key, "action": "deleted" if is_delete else "written"}
                )
        return report

    # -- deliver: send the handoffs I hold home -----------------------------

    def deliver(self) -> list[dict[str, Any]]:
        """Send every handoff this server holds to its designated owner."""
        report: list[dict[str, Any]] = []
        for owner in self._store.list_handoff_owners():
            for key in self._store.list_handoffs(owner):
                try:
                    data = self._store.read_handoff(key, owner)
                    is_delete = self._store.is_delete_handoff(key, owner)
                    _send_to_owner(owner, key, data, is_delete, self._timeout)
                    self._store.clear_handoff(key, owner)
                except FileNotFoundError:
                    continue  # another server moved it first; the debt is settled
                except Exception as exc:  # noqa: BLE001
                    report.append({"owner": owner, "key": key, "error": _describe(exc)})
                    continue
                report.append(
                    {"owner": owner, "key": key, "action": "deleted" if is_delete else "written"}
                )
        return report

    # -- both ---------------------------------------------------------------

    def reconcile(self) -> dict[str, Any]:
        """Collect this server's handoffs, then deliver the ones it holds.

        Never raises: each side records its own successes and failures.
        """
        return {"collected": self.collect(), "delivered": self.deliver()}


# ---------------------------------------------------------------------------
# Applying an entry, and the HTTP transport
# ---------------------------------------------------------------------------


def _apply(store: DataStore, key: str, data: bytes, is_delete: bool) -> None:
    """Make the local store match a collected handoff entry."""
    if is_delete:
        try:
            store.delete(key)
        except FileNotFoundError:
            pass  # the owner never held it, or already removed it
    else:
        store.write(key, data)


def _clear_remote(peer: str, owner: str, key: str, timeout: float) -> None:
    """Drop an entry on the substitute once it has been collected.

    A ``404`` means the entry is already gone -- another server moved it first
    -- which is exactly the outcome being asked for.
    """
    status, _, _ = _http("DELETE", _entry_url(peer, owner, key), timeout=timeout)
    if status not in (200, 204, 404):
        raise HandoffSyncError(f"{peer} answered {status} for clearing {key!r}")


def _send_to_owner(
    owner: str, key: str, data: bytes, is_delete: bool, timeout: float
) -> None:
    """Deliver one handoff to its owner through the owner's ``/files`` route."""
    url = _files_url(owner, key)
    if is_delete:
        status, _, _ = _http("DELETE", url, timeout=timeout)
        if status not in (200, 204, 404):
            raise HandoffSyncError(f"{owner} answered {status} for deleting {key!r}")
    else:
        status, _, _ = _http("PUT", url, data=data, timeout=timeout)
        if status not in (200, 201):
            raise HandoffSyncError(f"{owner} answered {status} for writing {key!r}")


def _http(
    method: str, url: str, *, data: bytes | None = None, timeout: float
) -> tuple[int, dict[str, str], bytes]:
    """One HTTP request, returning ``(status, headers, body)``.

    The status is returned rather than raised for a 4xx/5xx so a caller can
    treat a missing object or a duplicate delete as a normal outcome.  Headers
    are keyed lower-case, so header lookup does not depend on the server's
    capitalisation.
    """
    request = urllib.request.Request(url, data=data, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as reply:
            return reply.status, _lower(reply.headers.items()), reply.read()
    except urllib.error.HTTPError as exc:
        return exc.code, _lower((exc.headers or {}).items()), exc.read()


def _lower(items: Any) -> dict[str, str]:
    return {name.lower(): value for name, value in items}


def _owner_url(peer: str, owner: str) -> str:
    """``peer``'s list route for the handoffs it holds for ``owner``."""
    return f"{peer}/handoff?owner={quote(owner, safe='')}"


def _entry_url(peer: str, owner: str, key: str) -> str:
    """``peer``'s single-entry route for ``key`` owed to ``owner``."""
    return f"{_owner_url(peer, owner)}&key={quote(key, safe='')}"


def _files_url(owner: str, key: str) -> str:
    """The owner's own object route, used to deliver a handoff back."""
    return f"{owner}/files?key={quote(key, safe='')}"


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"
