#!/usr/bin/env python3
"""Consistent-hashing ring for swift_light -- in memory, on disk, rebalancing.

The ring answers one question -- *where does a file live?* -- from the file's
name alone, so every proxy can compute placement with no shared state::

    slot = hash(key) mod RING_SIZE          # 0 .. 65535
    fan  = ordered list of ALL data servers that may hold that file
           fan[0]                    -> primary
           fan[1:replicate_count]    -> secondaries
           fan[replicate_count:]     -> handoff servers

Layout
------
The ring is cut into **fan areas**.  A fan area is one contiguous arc owned by
a single *virtual node* (vnode).  Every data server owns ``vnode_count`` vnodes,
hashed onto the ring at ``hash("vnode|<node>|<index>")``.  Because an arc is
placed by hashing rather than by position in a list, adding or removing a data
server only moves the arcs it owns.

Every fan area carries the list of *all* data servers holding points, in
preference order: the primary first, then the remaining servers walking
clockwise around the ring and recording each distinct server the first time it
is met.  The first ``replicate_count`` entries are the replica set (primary
plus secondaries); everything after them is a **handoff** server -- the
substitute a proxy falls back to when a replica is down, and the node that
hands the data back once the replica returns (sloppy quorum).

``replicate_count`` counts *total copies of a file*, so the primary is one of
them and there are ``replicate_count - 1`` secondaries.

In memory
---------
Four parallel views of the same ring:

    _nodes        the data servers holding at least one vnode point
    _slots        one slot per vnode point, sorted clockwise
    _point_nodes  the server owning each point
    _fans         the whole fan area at each point

``_slots``, ``_point_nodes`` and ``_fans`` share an index, so the ring is a
list of fan areas laid out clockwise.

A ring is *complete* when every server owns its full ``vnode_count`` points,
which is what ``build()`` produces.  A ring caught half-way through a
rebalance is *partial*: a joining server owns only the points inserted so far.
Partial rings are legal on disk and are what ``validate(complete=False)`` and
``load(..., complete=False)`` are for.

On disk
-------
``build()`` lays out the ring and flushes it to ``ring.json``; ``load()`` reads
it back.  Building is a one-time, cluster-setup step, not a runtime one:
``build()`` refuses to overwrite a ring that already exists, so a restarting
proxy can only load, never silently re-place every stored file.

The write is atomic -- temp file in the same directory, ``fsync``,
``os.replace``, then ``fsync`` the directory -- so a crash can never leave a
half-written ring in place of a good one.  ``load()`` validates what it read
before handing back a ring, because the file is the one thing here that can be
corrupt.

``rebalance()`` uses three files, all beside one another:

    ring.json            the layout the cluster is serving from right now
    ring.json.temp       the ring after the step currently being taken
    balanced_ring.json   the finished target

Rebalancing
-----------
``rebalance()`` is called after a data server joins or leaves.  It does not
move placement all at once, because every change has to move real files.  The
finished target is built and flushed first, then taken one vnode point at a
time.  Each step does four things, and the order is the whole point:

1. insert or drop *one* vnode point in ``ring.json.temp`` and flush it, so the
   intention is on disk before any file moves;
2. move the files whose placement changed -- ``write()`` to the server taking
   them on, ``write_handoff()`` to the one giving them up;
3. rename ``ring.json.temp`` over ``ring.json`` -- the commit point;
4. flush the directory, so the rename survives a crash.

``ring.json`` therefore only ever describes a layout whose files have already
been moved, and it is what a rebooting server loads.  A crash anywhere earlier
leaves ``ring.json`` untouched, so the interrupted step is simply redone; file
moves are idempotent, so redoing them is safe.

``balanced_ring.json`` is the crash marker: it exists while a rebalance is in
flight and is removed once the last step commits.  Its presence is how a reboot
tells "interrupted" from "settled", and ``rebalance()`` resumes from it.

A step's worth of movement is not only the arc that changed owner.  Inserting a
point also puts the arriving server into other arcs' fan lists, as a secondary
for the arcs just after its point, so those replicas have to be settled too --
otherwise the ring would claim a copy that was never written.

Usage
-----
Build the cluster's ring -- once, during setup::

    from swift_light.proxy_server.consistent_hashing_ring import ConsistentHashingRing

    ring = ConsistentHashingRing.build(10, 3, path="ring.json")   # lays out + flushes
    same = ConsistentHashingRing.load("ring.json")                # every proxy reads

    ConsistentHashingRing.build(10, 3, path="ring.json")          # refuses:
    # RingAlreadyBuiltError: a ring already exists at ring.json ...

Ask where a file lives -- the call a proxy makes on every request::

    ring = ConsistentHashingRing.load("ring.json")   # each proxy, once
    fan  = ring.fan(object_name)                     # ordered server list

    fan[0]                          # primary: where a read goes first
    fan[1:ring.replicate_count]     # secondaries
    fan[ring.replicate_count:]      # handoffs: substitutes for a dead replica

The list comes from the ring alone, so any proxy answers without asking anyone.

Read the membership itself -- the data servers on the ring -- with ``nodes``::

    ring.nodes     # ('node_0', 'node_1', ...), in ring order

Add or remove one data server.  Each reads the current membership off disk,
finishes any rebalance a crash left in flight, then moves the cluster onto the
new membership::

    ring = ConsistentHashingRing.add_node("node_10", data=data_server, path="ring.json")
    ring = ConsistentHashingRing.remove_node("node_3", data=data_server, path="ring.json")

Move the cluster to a whole new membership at once, and pick up an interrupted
run::

    ring = ConsistentHashingRing.rebalance(
        ["node_0", ..., "node_10"], data=data_server, path="ring.json"
    )

    # On reboot, the same call: it resumes if a rebalance was interrupted, and
    # returns immediately if there is nothing left to move.
    ring = ConsistentHashingRing.load("ring.json", complete=False)
    ConsistentHashingRing.rebalance(membership, data=data_server, path="ring.json")

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from bisect import bisect_right
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Protocol, runtime_checkable

__all__ = [
    "ConsistentHashingRing",
    "DataServer",
    "RingAlreadyBuiltError",
    "RING_SIZE",
    "DEFAULT_VNODE_COUNT",
    "DEFAULT_RING_FILENAME",
]

RING_SIZE = 2**16
"""Number of slots the ring is divided into."""

DEFAULT_VNODE_COUNT = 100
"""Default number of virtual nodes each data server contributes."""

DEFAULT_HASH_ALGORITHM = "sha1"
DEFAULT_NODE_PREFIX = "node_"
DEFAULT_RING_FILENAME = "ring.json"

SCHEMA_VERSION = 1
"""Version of the on-disk ring format."""

_HASH_SEP = "\x1f"
_TEMP_SUFFIX = ".tmp"

# A vnode point, and one step of a rebalance.  An edit is
# ("insert"|"drop", slot, node).
_Point = tuple[int, str]
_Edit = tuple[str, int, str]


class RingAlreadyBuiltError(FileExistsError):
    """Raised when a ring is built where a ring already exists.

    ``build()`` is a cluster-setup step, so a second build would silently
    re-place every stored file.  Subclasses :class:`FileExistsError`, so
    ``except FileExistsError`` catches it too.
    """


@runtime_checkable
class DataServer(Protocol):
    """The part of a data server that :meth:`ConsistentHashingRing.rebalance`
    drives.  Not implemented yet -- this is the contract to build against.

    Slots are half-open: a file belongs to the arc ``[start_slot, end_slot)``.
    """

    def list_files(self, node: str, start_slot: int, end_slot: int) -> Iterable[str]:
        """Every file this server holds for slots ``[start_slot, end_slot)``."""
        ...

    def write(self, key: str, to_node: str) -> None:
        """Put ``key`` on ``to_node``, normally replicated."""
        ...

    def write_handoff(self, key: str, from_node: str) -> None:
        """Tell ``from_node`` to hand ``key`` over, so it may stop owning it."""
        ...


class ConsistentHashingRing:
    """Fan areas on a consistent-hashing ring, in memory and on disk.

    Parameters
    ----------
    tot_node_count:
        Total number of *data* servers in the cluster (proxies excluded).
        Every fan area lists exactly this many servers.
    replicate_count:
        Total number of copies kept for each file, primary included.  Must be
        between 1 and ``tot_node_count``.
    vnode_count:
        Virtual nodes per data server.  Higher spreads each server over more,
        smaller arcs, so primary load is more even and a membership change
        moves fewer keys.
    """

    __slots__ = (
        "tot_node_count",
        "replicate_count",
        "vnode_count",
        "ring_size",
        "hash_algorithm",
        "node_prefix",
        "_nodes",
        "_slots",
        "_point_nodes",
        "_fans",
    )

    # -- construction -------------------------------------------------------

    def __init__(
        self,
        tot_node_count: int,
        replicate_count: int,
        *,
        vnode_count: int = DEFAULT_VNODE_COUNT,
        ring_size: int = RING_SIZE,
        hash_algorithm: str = DEFAULT_HASH_ALGORITHM,
        node_prefix: str = DEFAULT_NODE_PREFIX,
    ) -> None:
        _require_positive("tot_node_count", tot_node_count)
        _require_positive("replicate_count", replicate_count)
        _require_positive("vnode_count", vnode_count)
        _require_positive("ring_size", ring_size)
        if replicate_count > tot_node_count:
            raise ValueError(
                f"replicate_count ({replicate_count}) cannot exceed "
                f"tot_node_count ({tot_node_count})"
            )
        if hash_algorithm not in hashlib.algorithms_available:
            raise ValueError(f"unsupported hash algorithm {hash_algorithm!r}")

        self.tot_node_count = int(tot_node_count)
        self.replicate_count = int(replicate_count)
        self.vnode_count = int(vnode_count)
        self.ring_size = int(ring_size)
        self.hash_algorithm = str(hash_algorithm)
        self.node_prefix = str(node_prefix)

        self._nodes: tuple[str, ...] = ()
        self._slots: tuple[int, ...] = ()
        self._point_nodes: tuple[str, ...] = ()
        self._fans: tuple[tuple[str, ...], ...] = ()

    @classmethod
    def build(
        cls,
        tot_node_count: int,
        replicate_count: int,
        *,
        vnode_count: int = DEFAULT_VNODE_COUNT,
        ring_size: int = RING_SIZE,
        hash_algorithm: str = DEFAULT_HASH_ALGORITHM,
        node_prefix: str = DEFAULT_NODE_PREFIX,
        nodes: Iterable[str] | None = None,
        path: str | os.PathLike[str] | None = None,
    ) -> "ConsistentHashingRing":
        """Build the cluster's ring, once, and flush it to disk.

        This is a cluster-setup step, not a runtime one.  The ring file *is*
        the cluster's layout, so building it a second time would silently move
        every stored file to a different server.  If a ring already exists at
        ``path`` this raises :class:`RingAlreadyBuiltError` rather than
        overwriting it -- delete the file only when you truly mean to rebuild
        the cluster.

        Defaults to ``ring.json`` next to this module.  ``nodes`` overrides the
        generated ``<node_prefix>0, <node_prefix>1, ...`` names, which is how a
        caller pins placement to a known cluster.
        """
        ring = cls(
            tot_node_count,
            replicate_count,
            vnode_count=vnode_count,
            ring_size=ring_size,
            hash_algorithm=hash_algorithm,
            node_prefix=node_prefix,
        )

        target = _resolve_path(path)
        if target.exists():
            raise RingAlreadyBuiltError(
                f"a ring already exists at {target}; the ring is built once, at "
                f"cluster setup -- remove the file only to rebuild the cluster"
            )

        if nodes is None:
            members = tuple(
                f"{ring.node_prefix}{index}" for index in range(ring.tot_node_count)
            )
        else:
            members = tuple(nodes)
        ring._install(members)

        ring.save(target)
        return ring

    # -- membership ---------------------------------------------------------

    @classmethod
    def add_node(
        cls,
        node: str,
        *,
        data: DataServer,
        path: str | os.PathLike[str] | None = None,
    ) -> "ConsistentHashingRing":
        """Add ``node`` to the cluster and rebalance onto it.

        The membership is read off disk rather than handed in, so a server that
        has just booted -- or that was never told the cluster's shape -- can
        still join one.  Any rebalance a crash left in flight is finished first,
        so this starts from a layout whose files are all in place; the joining
        node's name is all the caller has to know.

        Raises ``ValueError`` if ``node`` is already a member, leaving the
        cluster exactly as it was.  There is no settled no-op here: a new server
        always owns points, so there is always something to move.
        """
        members = cls._settled_members(path, data)
        if node in members:
            raise ValueError(f"{node!r} is already a member of the cluster")
        return cls.rebalance((*members, node), data=data, path=path)

    @classmethod
    def remove_node(
        cls,
        node: str,
        *,
        data: DataServer,
        path: str | os.PathLike[str] | None = None,
    ) -> "ConsistentHashingRing":
        """Remove ``node`` from the cluster and rebalance off it.

        The mirror of :meth:`add_node`: membership is read off disk, an
        interrupted rebalance is finished first, and a node that is not a member
        raises ``ValueError`` without moving anything.

        A cluster may not shrink below one server, nor below its
        ``replicate_count``.  Both are caught while the target is built, which
        is before the first file moves, so the cluster survives the mistake.
        """
        members = cls._settled_members(path, data)
        if node not in members:
            raise ValueError(f"{node!r} is not a member of the cluster")
        return cls.rebalance(
            tuple(name for name in members if name != node), data=data, path=path
        )

    # -- rebalancing --------------------------------------------------------

    @classmethod
    def rebalance(
        cls,
        nodes: Iterable[str],
        *,
        data: DataServer,
        path: str | os.PathLike[str] | None = None,
    ) -> "ConsistentHashingRing":
        """Move the cluster to ``nodes``, one vnode point at a time.

        Called after a data server joins or leaves.  The target is built and
        flushed first, then taken one step at a time -- insert or drop a single
        vnode point, move the files that disturbs, commit by renaming.  See the
        module docstring for the ordering and why it is safe.

        Safe to call again after a crash: it resumes from wherever ``ring.json``
        got to, and returns immediately when there is nothing left to move.
        Call it again on reboot even if you are not sure -- a settled cluster
        costs one file check.

        ``data`` supplies ``list_files`` / ``write`` / ``write_handoff``; see
        :class:`DataServer`.
        """
        members = tuple(nodes)
        if not isinstance(data, DataServer):
            raise TypeError(
                "data must provide list_files(node, start_slot, end_slot), "
                "write(key, to_node) and write_handoff(key, from_node)"
            )

        live_path = _resolve_path(path)
        balanced_path = _balanced_path(live_path)
        temp_path = _temp_path(live_path)

        # ring.json is what the cluster is serving from, so it may legitimately
        # be half-way through a rebalance.
        current = cls.load(live_path, complete=False)

        if balanced_path.is_file():
            # A run was interrupted.  Carry on towards the target it left.
            balanced = cls.load(balanced_path)
            if set(balanced._nodes) != set(members):
                raise ValueError(
                    f"{balanced_path.name} targets {len(balanced._nodes)} servers, "
                    f"not the {len(members)} asked for; finish or remove it first"
                )
        elif _is_settled(current, members):
            return current
        else:
            balanced = cls.build(
                len(members),
                current.replicate_count,
                vnode_count=current.vnode_count,
                ring_size=current.ring_size,
                hash_algorithm=current.hash_algorithm,
                nodes=members,
                path=balanced_path,
            )

        for edit in _plan_steps(current, balanced):
            following = current._after_edit(edit, balanced._nodes)
            following.save(temp_path)
            _move_files(current, following, data)
            _install_prepared(temp_path, live_path)
            current = following

        if not _is_settled(current, members):
            raise RuntimeError(
                f"rebalance stalled with {len(current._slots)} of "
                f"{len(members) * current.vnode_count} vnode points in place"
            )

        if balanced_path.exists():
            balanced_path.unlink()
            _fsync_directory(balanced_path.parent)
        return current

    # -- membership ---------------------------------------------------------

    @property
    def nodes(self) -> tuple[str, ...]:
        """The data servers on this ring -- the membership, in ring order.

        The same list as the ``nodes`` field on disk.  A proxy reads it once at
        startup to learn which data servers to talk to -- for example, to
        heartbeat every one of them.  Immutable, so a caller cannot edit the
        membership out from under the ring.
        """
        return self._nodes

    # -- lookup -------------------------------------------------------------

    def slot_for_key(self, key: str) -> int:
        """The slot a file's name maps to: ``hash(key) mod ring_size``.

        A pure function of the name, so every proxy in the cluster computes the
        same slot with no shared state and no coordination.
        """
        return self._hash_slot(key)

    def fan(self, key: str) -> tuple[str, ...]:
        """The ordered list of data servers that may hold ``key``.

        The one call a proxy makes to answer *where does this file live?*  The
        result is the whole fan area covering the file's slot -- every server
        listed exactly once, in the order to try them:

            fan[0]                    primary -- where a read goes first
            fan[1:replicate_count]    secondaries
            fan[replicate_count:]     handoff servers, tried before giving up

        A client walks the list: send to the primary, drop to the next server
        when one is down, and read an answer that came from a handoff server as
        the explicitly-marked fallback, meaning the data is still catching up to
        its proper replica.

        Read straight off the ring in memory -- one hash and one binary search.
        On a ring loaded with ``complete=False`` this describes the layout being
        served right now, mid-rebalance included.
        """
        return self._fans[self._index_for_slot(self.slot_for_key(key))]

    # -- disk ---------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """The ring as plain, JSON-ready data."""
        return {
            "schema_version": SCHEMA_VERSION,
            "ring_size": self.ring_size,
            "hash_algorithm": self.hash_algorithm,
            "tot_node_count": self.tot_node_count,
            "replicate_count": self.replicate_count,
            "vnode_count": self.vnode_count,
            "nodes": list(self._nodes),
            "points": [
                [slot, node] for slot, node in zip(self._slots, self._point_nodes)
            ],
            "fans": [list(fan) for fan in self._fans],
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        complete: bool = True,
    ) -> "ConsistentHashingRing":
        """Rebuild a ring from :meth:`to_dict` output, validating it first.

        ``complete=False`` accepts a ring half-way through a rebalance.
        """
        if not isinstance(data, Mapping):
            raise TypeError(f"ring data must be a mapping, got {type(data).__name__}")
        version = data.get("schema_version", SCHEMA_VERSION)
        if version != SCHEMA_VERSION:
            raise ValueError(f"unsupported ring schema version {version!r}")

        try:
            tot_node_count = int(data["tot_node_count"])
            replicate_count = int(data["replicate_count"])
            vnode_count = int(data["vnode_count"])
            nodes = tuple(str(node) for node in data["nodes"])
            points = list(data["points"])
            fans = list(data["fans"])
        except KeyError as exc:
            raise ValueError(f"ring data is missing field {exc.args[0]!r}") from None

        ring = cls(
            tot_node_count,
            replicate_count,
            vnode_count=vnode_count,
            ring_size=int(data.get("ring_size", RING_SIZE)),
            hash_algorithm=str(data.get("hash_algorithm", DEFAULT_HASH_ALGORITHM)),
        )

        slots: list[int] = []
        point_nodes: list[str] = []
        for point in points:
            try:
                slot, node = point
            except (TypeError, ValueError):
                raise ValueError(
                    f"each ring point must be a [slot, node] pair, got {point!r}"
                ) from None
            slots.append(_as_slot(slot, ring.ring_size))
            point_nodes.append(str(node))

        ring._nodes = nodes
        ring._slots = tuple(slots)
        ring._point_nodes = tuple(point_nodes)
        ring._fans = tuple(tuple(str(node) for node in fan) for fan in fans)
        ring.validate(complete=complete)
        return ring

    def save(self, path: str | os.PathLike[str] | None = None) -> Path:
        """Atomically write the ring to ``path`` and return where it landed.

        Defaults to ``ring.json`` next to this module.
        """
        target = _resolve_path(path)
        _atomic_write_text(target, json.dumps(self.to_dict(), indent=2) + "\n")
        return target

    @classmethod
    def load(
        cls,
        path: str | os.PathLike[str] | None = None,
        *,
        complete: bool = True,
    ) -> "ConsistentHashingRing":
        """Read the ring back from ``path`` (default: beside this module).

        Pass ``complete=False`` to accept a ring that a rebalance left
        half-way, which is what a rebooting server needs.
        """
        source = _resolve_path(path)
        if not source.is_file():
            raise FileNotFoundError(f"no ring file at {source}")
        with source.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return cls.from_dict(data, complete=complete)

    # -- integrity ----------------------------------------------------------

    def validate(self, *, complete: bool = True) -> None:
        """Raise ``ValueError`` unless this ring is self-consistent.

        ``complete=True`` also requires every server to hold its full
        ``vnode_count`` points, which is what ``build()`` produces.  A ring
        caught half-way through a rebalance holds only some of them, so it is
        checked with ``complete=False``; every other rule still applies.
        """
        total = len(self._slots)
        full_total = self.tot_node_count * self.vnode_count

        if len(set(self._nodes)) != self.tot_node_count:
            raise ValueError("ring does not define tot_node_count distinct servers")
        if total > full_total:
            raise ValueError(
                f"ring has {total} vnode points, more than the {full_total} of a "
                f"complete layout ({self.tot_node_count} servers x "
                f"{self.vnode_count} vnodes)"
            )
        if complete and total != full_total:
            raise ValueError(
                f"ring has {total} vnode points, expected {full_total} "
                f"({self.tot_node_count} servers x {self.vnode_count} vnodes)"
            )
        if list(self._slots) != sorted(self._slots):
            raise ValueError("vnode points are not sorted clockwise")
        if len(self._point_nodes) != total or len(self._fans) != total:
            raise ValueError("points and fans must describe the same fan areas")
        if set(self._point_nodes) != set(self._nodes):
            raise ValueError(
                "every server must own at least one vnode point, and no point may "
                "name a server outside the node set"
            )

        owned = Counter(self._point_nodes)
        for name in self._nodes:
            if complete and owned[name] != self.vnode_count:
                raise ValueError(
                    f"server {name} owns {owned[name]} vnode points, "
                    f"expected {self.vnode_count}"
                )

        for index, fan in enumerate(self._fans):
            if len(fan) != self.tot_node_count:
                raise ValueError(
                    f"fan area {index} lists {len(fan)} servers, "
                    f"expected all {self.tot_node_count}"
                )
            head = self._point_nodes[index]
            if fan[0] != head:
                raise ValueError(f"fan area {index} does not start with its primary")
            if set(fan) != set(self._nodes):
                raise ValueError(f"fan area {index} is not a permutation of the servers")
            following = self._fans[(index + 1) % total]
            expected_fan = (head,) + tuple(node for node in following if node != head)
            if fan != expected_fan:
                raise ValueError(
                    f"fan area {index} is not a clockwise walk from its primary"
                )

    # -- dunder -------------------------------------------------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ConsistentHashingRing):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        return (
            f"ConsistentHashingRing(nodes={len(self._nodes)}, "
            f"replicas={self.replicate_count}, vnodes={self.vnode_count}, "
            f"ring_size={self.ring_size})"
        )

    # -- internals ----------------------------------------------------------

    def _install(self, members: tuple[str, ...]) -> None:
        """Lay out one vnode per (server, index) and derive every fan area."""
        if len(members) != self.tot_node_count:
            raise ValueError(
                f"got {len(members)} node names for tot_node_count="
                f"{self.tot_node_count}"
            )
        if len(set(members)) != len(members):
            raise ValueError("node names must be unique")

        points: list[tuple[int, str, int]] = []
        for node in members:
            for index in range(self.vnode_count):
                points.append((self._hash_slot("vnode", node, str(index)), node, index))
        # Slot order is the ring; the extra keys only break hash collisions
        # deterministically, so two runs on the same input agree.
        points.sort(key=lambda point: (point[0], point[1], point[2]))

        self._nodes = members
        self._slots = tuple(point[0] for point in points)
        self._point_nodes = tuple(point[1] for point in points)
        self._fans = self._build_fans(self._point_nodes)
        self.validate()

    def _install_points(self, members: tuple[str, ...], points: list[_Point]) -> None:
        """Lay out an explicit, already-sorted point list and derive the fans.

        The rebalance path uses this instead of :meth:`_install`, because a
        partial ring's points are not a function of the node set.
        """
        if len(set(members)) != len(members):
            raise ValueError("node names must be unique")
        known = set(members)
        for slot, node in points:
            _as_slot(slot, self.ring_size)
            if node not in known:
                raise ValueError(f"vnode point names {node!r}, not in the node set")

        self._nodes = tuple(members)
        self._slots = tuple(slot for slot, _ in points)
        self._point_nodes = tuple(node for _, node in points)
        self._fans = self._build_fans(self._point_nodes)
        self.validate(complete=False)

    @classmethod
    def _settled_members(
        cls,
        path: str | os.PathLike[str] | None,
        data: DataServer,
    ) -> tuple[str, ...]:
        """Node names of the settled cluster, finishing an interrupted rebalance.

        A membership change has to build on a layout whose files are all in
        place, so if the crash marker is present the run it left is driven to
        completion first -- :meth:`rebalance` resumes from that marker and hands
        back the settled ring.  With no marker this is a single read of
        ``ring.json``.

        The names come back in ring order, which is the order the caller's new
        membership is built in, so the last rebalance step lands exactly on the
        target instead of on a permutation of it.
        """
        live_path = _resolve_path(path)
        balanced_path = _balanced_path(live_path)
        if balanced_path.is_file():
            pending = cls.load(balanced_path)
            return cls.rebalance(pending._nodes, data=data, path=path)._nodes
        return cls.load(live_path)._nodes

    def _after_edit(
        self, edit: _Edit, target_order: tuple[str, ...]
    ) -> "ConsistentHashingRing":
        """A copy of this ring with one vnode point inserted or dropped.

        ``target_order`` is the membership being rebalanced towards; it fixes
        the order servers are listed in, so the last step lands exactly on the
        target rather than on a permutation of it.
        """
        kind, slot, node = edit
        points = list(zip(self._slots, self._point_nodes))
        if kind == "insert":
            points.append((slot, node))
        elif kind == "drop":
            points.remove((slot, node))
        else:
            raise ValueError(f"unknown rebalance edit {kind!r}")
        points.sort()

        present = {name for _, name in points}
        order = tuple(
            name
            for name in dict.fromkeys(target_order + self._nodes)
            if name in present
        )

        following = type(self)(
            len(order),
            self.replicate_count,
            vnode_count=self.vnode_count,
            ring_size=self.ring_size,
            hash_algorithm=self.hash_algorithm,
        )
        following._install_points(order, points)
        return following

    def _index_for_slot(self, slot: int) -> int:
        """Index of the vnode whose arc covers ``slot``."""
        if not self._slots:
            raise RuntimeError("ring has no vnode points")
        slot = _as_slot(slot, self.ring_size)
        index = bisect_right(self._slots, slot) - 1
        if index < 0:
            # Before the first point: the last arc wraps through slot 0.
            index = len(self._slots) - 1
        return index

    def _replica_set_at(self, slot: int) -> tuple[str, ...]:
        """The replica set covering ``slot``."""
        return self._fans[self._index_for_slot(slot)][: self.replicate_count]

    def _build_fans(self, sequence: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
        """Fan area per vnode, each a clockwise walk of all distinct servers.

        Walking every start from scratch is ``O(V^2)``.  Instead walk index 0
        once, then reuse: the walk from ``i`` is the walk from ``i + 1`` with
        ``sequence[i]`` promoted to the front, because moving the start point
        forward can only remove one server's first appearance.
        """
        total = len(sequence)
        if total == 0:
            return ()
        fans: list[tuple[str, ...]] = [()] * total
        fans[0] = self._walk(sequence, 0)
        for index in range(total - 1, 0, -1):
            following = fans[index + 1] if index + 1 < total else fans[0]
            head = sequence[index]
            fans[index] = (head,) + tuple(node for node in following if node != head)
        return tuple(fans)

    def _walk(self, sequence: tuple[str, ...], start: int) -> tuple[str, ...]:
        """Reference clockwise walk: first ``tot_node_count`` distinct servers."""
        seen: list[str] = []
        seen_set: set[str] = set()
        total = len(sequence)
        for step in range(total):
            node = sequence[(start + step) % total]
            if node not in seen_set:
                seen_set.add(node)
                seen.append(node)
                if len(seen) == self.tot_node_count:
                    break
        return tuple(seen)

    def _hash_slot(self, *parts: str) -> int:
        """Map a tuple of strings onto a slot, identically on every proxy."""
        payload = _HASH_SEP.join(parts).encode("utf-8")
        digest = hashlib.new(self.hash_algorithm, payload).digest()
        return int.from_bytes(digest[:8], "big") % self.ring_size


# ---------------------------------------------------------------------------
# Rebalance planning and file movement
# ---------------------------------------------------------------------------


def _is_settled(ring: ConsistentHashingRing, members: tuple[str, ...]) -> bool:
    """True when ``ring`` is already the complete layout for ``members``.

    Points are a pure function of the node set, so a complete ring naming
    exactly these servers is the target already.  Membership is treated as a
    set: the order the servers were listed in has no effect on placement.
    """
    return (
        set(ring._nodes) == set(members)
        and len(ring._slots) == len(members) * ring.vnode_count
    )


def _plan_steps(
    current: ConsistentHashingRing, balanced: ConsistentHashingRing
) -> list[_Edit]:
    """One edit per vnode point that still has to be added or removed.

    Points are compared as multisets rather than by server, because a server
    part-way through joining already holds some of its points and still needs
    the rest -- which is exactly the state a resumed rebalance starts from.
    """
    wanted = Counter(zip(balanced._slots, balanced._point_nodes))
    have = Counter(zip(current._slots, current._point_nodes))

    # Grow before shrinking: spare capacity is never the worse position.
    inserts = sorted((wanted - have).elements())
    drops = sorted((have - wanted).elements())
    return [("insert", slot, node) for slot, node in inserts] + [
        ("drop", slot, node) for slot, node in drops
    ]


def _arc_ranges(bounds: list[int], ring_size: int) -> list[list[tuple[int, int]]]:
    """Half-open slot ranges for the arcs cut by ``bounds``, as pieces.

    Almost every arc is one range.  The arc that wraps through slot 0 is two,
    because a slot range cannot wrap.
    """
    count = len(bounds)
    if count == 0:
        return []
    if count == 1:
        return [[(0, ring_size)]]

    ranges = [
        [(bounds[index], bounds[index + 1])] for index in range(count - 1)
    ]
    pieces: list[tuple[int, int]] = []
    if bounds[0] > 0:
        pieces.append((0, bounds[0]))
    if bounds[-1] < ring_size:
        pieces.append((bounds[-1], ring_size))
    ranges.append(pieces or [(0, ring_size)])
    return ranges


def _changed_arcs(
    before: ConsistentHashingRing, after: ConsistentHashingRing
) -> Iterator[tuple[list[tuple[int, int]], tuple[str, ...], tuple[str, ...], tuple[str, ...]]]:
    """Yield ``(pieces, old_set, joined, left)`` per arc whose replicas changed.

    The arcs are cut at every slot where either ring has a point, so both rings
    are constant across each one and a single probe slot describes it whole.
    """
    bounds = sorted(set(before._slots) | set(after._slots))
    for index, pieces in enumerate(_arc_ranges(bounds, before.ring_size)):
        probe = bounds[index]
        old_set = before._replica_set_at(probe)
        new_set = after._replica_set_at(probe)
        if set(old_set) == set(new_set):
            continue
        joined = tuple(node for node in new_set if node not in old_set)
        left = tuple(node for node in old_set if node not in new_set)
        if joined or left:
            yield pieces, old_set, joined, left


def _move_files(
    before: ConsistentHashingRing, after: ConsistentHashingRing, data: DataServer
) -> int:
    """Move every file whose placement changed, and count them."""
    moved = 0
    for pieces, old_set, joined, left in _changed_arcs(before, after):
        source = old_set[0]  # the arc's old primary, which held these files
        for start, end in pieces:
            if start >= end:
                continue
            for key in data.list_files(source, start, end):
                for node in joined:
                    data.write(key, node)
                for node in left:
                    data.write_handoff(key, node)
                moved += 1
    return moved


# ---------------------------------------------------------------------------
# Paths and durable writes
# ---------------------------------------------------------------------------


def _resolve_path(path: str | os.PathLike[str] | None) -> Path:
    """The caller's path, or ``ring.json`` beside this module."""
    if path is None:
        return Path(__file__).with_name(DEFAULT_RING_FILENAME)
    return Path(path)


def _temp_path(path: Path) -> Path:
    """The working file for the step in flight."""
    return path.with_name(path.name + ".temp")


def _balanced_path(path: Path) -> Path:
    """The finished target, and the marker that a rebalance is in flight."""
    return path.with_name("balanced_" + path.name)


def _install_prepared(prepared: Path, target: Path) -> None:
    """Swap a fully written ring file into place and flush the directory.

    The rename is the commit point, so the directory flush is what makes the
    commit durable rather than merely visible.
    """
    os.replace(prepared, target)
    _fsync_directory(target.parent)


def _require_positive(name: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if value < 1:
        raise ValueError(f"{name} must be at least 1, got {value}")


def _as_slot(slot: Any, ring_size: int) -> int:
    if isinstance(slot, bool) or not isinstance(slot, int):
        raise TypeError(f"slot must be an int, got {type(slot).__name__}")
    if not 0 <= slot < ring_size:
        raise ValueError(f"slot {slot} is outside [0, {ring_size})")
    return slot


def _fsync_directory(directory: Path) -> None:
    """Flush the directory entry so a completed rename survives a crash.

    Best effort: some platforms (notably Windows) refuse to open a directory
    for reading, and there the rename has already been ordered by the OS.
    """
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _atomic_write_text(target: Path, text: str) -> None:
    """Write ``text`` to ``target`` so a reader sees only a complete file.

    Temp file in the *same directory* (so the swap is a rename, not a copy),
    flushed to storage, renamed over the target, then the directory flushed.
    ``mkstemp`` gives each caller its own name, so two concurrent saves cannot
    clobber each other's work.
    """
    directory = target.parent
    directory.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=target.name + ".", suffix=_TEMP_SUFFIX, dir=str(directory)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    _fsync_directory(directory)
