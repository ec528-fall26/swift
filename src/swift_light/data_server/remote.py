#!/usr/bin/env python3
"""Drive a membership change against live data servers over HTTP.

The ring moves files through a small interface -- ``list_files``, ``write`` and
``write_handoff`` (see
:class:`~swift_light.proxy_server.consistent_hashing_ring.DataServer`).  This
module is that interface, implemented against running data servers instead of
an in-process store, so a rebalance can move real objects between real
processes::

    ring = ConsistentHashingRing.add_node(
        "http://127.0.0.1:9004",
        data=RemoteDataServer(ring),
        path="ring.json",
    )

The three calls map onto the data server's routes:

    list_files(node, start, end)   GET  <node>/keys          filtered by slot
    write(key, to_node)            GET  <source>/files?key=  then
                                   PUT  <to_node>/files?key=
    write_handoff(key, from_node)  DELETE <from_node>/files?key=

``list_files`` is called once per slot range and remembers which node it was
asked about; the ``write`` calls that follow read from that node, because that
is where the rebalance found the keys.  ``write_handoff`` runs only for keys
this instance has already copied successfully, so a failure can never delete a
node's last copy: the copy is on every gaining node before the losing node's
own is dropped.  A key the losing node no longer holds is a ``404`` and is
treated as done.

Locking
-------
A migration must not have the files change underneath it.  ``lock(*nodes)`` is
a context manager that takes each node's write lock (``POST /lock``), performs
the block with the token on every request, and gives the locks back afterwards
(``DELETE /lock``).  Nodes are locked smallest-URL-first, so two coordinators
that touch an overlapping set cannot deadlock; a lock already held by another,
unexpired token makes the transfer fail rather than proceed unlocked.  The
lease is ``DEFAULT_LOCK_SECONDS`` and is renewed while a long transfer keeps
going, so a crashed coordinator frees the nodes by itself.

Only mutating routes honour the lock: reads, ``GET /keys`` and heartbeats stay
available, and the data server answers ``503`` to anyone else's write while the
block is held.

Usage
-----
::

    from swift_light.data_server.remote import RemoteDataServer

    data = RemoteDataServer(ring, timeout=10.0)
    data.lock(source, *joined, *left)     # context manager
    ...
    ConsistentHashingRing.add_node(url, data=data, path=ring_path)

One instance drives one rebalance -- it remembers the node ``list_files`` last
looked at, so it is not safe to share between concurrent changes.

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from typing import Any, Iterator
from urllib.parse import quote

from swift_light.data_server.data_HTTP_server import (
    DEFAULT_LOCK_SECONDS,
    LOCK_TOKEN_HEADER,
)
from swift_light.proxy_server.consistent_hashing_ring import ConsistentHashingRing

__all__ = [
    "DEFAULT_REQUEST_TIMEOUT",
    "MigrationError",
    "RemoteDataServer",
]

DEFAULT_REQUEST_TIMEOUT = 10.0
"""Seconds to wait for any one migration request."""

_RENEW_MARGIN = 30.0
"""Seconds before a lease lapses at which a transfer renews it."""


class MigrationError(RuntimeError):
    """A data server could not carry out part of a membership change."""


class RemoteDataServer:
    """The ring's ``DataServer`` protocol, implemented over HTTP.

    Parameters
    ----------
    ring:
        The ring the rebalance is moving *from*.  Only its ``slot_for_key`` is
        used -- placement is a pure function of the key -- so the deployed ring
        is the right one to hand in.
    timeout:
        Seconds to wait for any one request.

    The instance tracks the node the last :meth:`list_files` asked about and
    which of those keys have been copied, which is what lets :meth:`write`
    read from the right place and :meth:`write_handoff` avoid deleting the only
    remaining copy.  That state spans one arc, not the whole rebalance, so one
    instance serves one migration and is not shared.
    """

    __slots__ = (
        "_ring",
        "_timeout",
        "_source",
        "_copied",
        "_token",
        "_locked",
        "_renew_at",
    )

    def __init__(
        self,
        ring: ConsistentHashingRing,
        *,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        if not isinstance(ring, ConsistentHashingRing):
            raise TypeError(
                f"ring must be a ConsistentHashingRing, got {type(ring).__name__}"
            )
        self._ring = ring
        self._timeout = float(timeout)
        self._source: str | None = None
        self._copied: set[str] = set()
        self._token: str | None = None
        self._locked: list[str] = []
        self._renew_at = 0.0

    # -- the DataServer protocol --------------------------------------------

    def list_files(
        self, node: str, start_slot: int, end_slot: int
    ) -> list[str]:
        """The keys ``node`` holds whose slot is in ``[start_slot, end_slot)``.

        Asks the node for everything it has (``GET /keys``) and filters here,
        because the slot is a property of the key and the ring, not of the
        server.  The node becomes the source for the ``write`` calls that
        follow.
        """
        keys = self._keys_on(node)
        self._source = node.rstrip("/")
        self._copied = set()
        return [
            key
            for key in keys
            if start_slot <= self._ring.slot_for_key(key) < end_slot
        ]

    def write(self, key: str, to_node: str) -> None:
        """Copy ``key`` from the current source to ``to_node``."""
        if self._source is None:
            raise MigrationError(
                "write() needs a source: call list_files() for the arc first"
            )
        self._renew_if_needed()
        data = self._read_file(self._source, key)
        self._put_file(to_node, key, data)
        self._copied.add(key)

    def write_handoff(self, key: str, from_node: str) -> None:
        """Tell ``from_node`` to give up ``key`` once the copies are in place.

        Only a key this arc has already copied is dropped; if it was not (the
        write failed, or ``from_node`` never held it) this is a no-op or a
        tolerated ``404``.  That keeps the ordering safe: the gaining nodes have
        the bytes before the losing node stops.
        """
        if key not in self._copied:
            return
        self._renew_if_needed()
        status, _, _ = self._request(
            "DELETE", _files_url(from_node, key)
        )
        if status not in (200, 204, 404):
            raise MigrationError(
                f"{from_node} answered {status} for releasing {key!r}"
            )

    # -- locking ------------------------------------------------------------

    @contextmanager
    def lock(self, *nodes: str) -> Iterator[None]:
        """Hold the write lock on every node in ``nodes`` for the block.

        Sorted acquisition keeps two coordinators from deadlocking; the first
        refusal releases what was already taken and raises
        :class:`MigrationError`.  The token is attached to every request made
        inside the block, so the migration's own writes are allowed through.
        """
        targets = list(dict.fromkeys(node.rstrip("/") for node in nodes))
        token = uuid.uuid4().hex
        acquired: list[str] = []
        previous, previous_renew = self._token, self._renew_at
        try:
            for node in sorted(targets):
                self._acquire(node, token)
                acquired.append(node)
            self._token = token
            self._locked = acquired
            self._renew_at = time.monotonic() + DEFAULT_LOCK_SECONDS
            yield
        finally:
            self._token, self._renew_at = previous, previous_renew
            self._locked = []
            for node in reversed(acquired):
                self._release(node, token)

    def _renew_if_needed(self) -> None:
        """Extend every held lease shortly before it lapses."""
        if self._token is None or not self._locked:
            return
        if time.monotonic() < self._renew_at - _RENEW_MARGIN:
            return
        for node in self._locked:
            self._acquire(node, self._token)
        self._renew_at = time.monotonic() + DEFAULT_LOCK_SECONDS

    def _acquire(self, node: str, token: str) -> None:
        status, _, body = self._request(
            "POST",
            node.rstrip("/") + "/lock",
            headers={LOCK_TOKEN_HEADER: token},
        )
        if status != 200:
            raise MigrationError(
                f"{node} refused the migration lock ({status}): {body[:200]!r}"
            )

    def _release(self, node: str, token: str) -> None:
        """Give a lease back; best effort, because the block may be unwinding."""
        try:
            self._request(
                "DELETE",
                node.rstrip("/") + "/lock",
                headers={LOCK_TOKEN_HEADER: token},
            )
        except MigrationError:
            pass  # the lease lapses on its own

    # -- transport ----------------------------------------------------------

    def _keys_on(self, node: str) -> list[str]:
        """Every key ``node`` holds, normal store only."""
        status, _, body = self._request("GET", node.rstrip("/") + "/keys")
        if status != 200:
            raise MigrationError(f"{node} answered {status} for /keys")
        try:
            keys = json.loads(body.decode("utf-8")).get("keys", [])
        except (ValueError, AttributeError) as exc:
            raise MigrationError(f"{node} sent no usable /keys body: {exc}") from exc
        return [str(key) for key in keys]

    def _read_file(self, node: str, key: str) -> bytes:
        status, _, body = self._request("GET", _files_url(node, key))
        if status == 404:
            raise MigrationError(f"{node} no longer holds {key!r}")
        if status != 200:
            raise MigrationError(f"{node} answered {status} for {key!r}")
        return body

    def _put_file(self, node: str, key: str, data: bytes) -> None:
        status, _, body = self._request("PUT", _files_url(node, key), data=data)
        if status not in (200, 201):
            raise MigrationError(
                f"{node} answered {status} for writing {key!r}: {body[:200]!r}"
            )

    def _request(
        self,
        method: str,
        url: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        """One HTTP request; 4xx/5xx come back as a status, no connection raises.

        The migration token is attached automatically while a lock is held, so
        the data servers let this instance's writes through and refuse everyone
        else's.
        """
        request = urllib.request.Request(url, data=data, method=method)
        for name, value in (headers or {}).items():
            request.add_header(name, value)
        if self._token is not None and LOCK_TOKEN_HEADER not in (headers or {}):
            request.add_header(LOCK_TOKEN_HEADER, self._token)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as reply:
                return reply.status, dict(reply.headers), reply.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read()
        except (urllib.error.URLError, OSError) as exc:
            raise MigrationError(f"{method} {url} failed: {exc}") from exc


def _files_url(node: str, key: str) -> str:
    """``node``'s object route for ``key``."""
    return f"{node.rstrip('/')}/files?key={quote(key, safe='')}"
