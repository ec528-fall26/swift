#!/usr/bin/env python3
"""Data server configuration for swift_light -- in memory and on disk.

A data server needs two things before it can do any work: where it listens,
and which proxy to talk to.  Both are set once when the server is brought up
and then change rarely, so they are kept in one small record -- the server's
*global parameters*::

    own_address     this data server's base URL      http://10.0.0.5:8080
    proxy_address   the proxy it reports to          http://10.0.0.1:8000

Both are full URLs (``scheme://host[:port]``), the same form the ring uses for
its nodes, so an ``own_address`` can be put straight into the ring's membership
without rewriting it.

The record is immutable.  A server that is re-addressed makes a new
:class:`DataServerConfig` and saves it; nothing mutates a config in place.

On disk
-------
``save()`` writes the record to ``data_server.json`` beside this module (unless
another path is given); ``load()`` reads it back.  The write is atomic -- temp
file in the same directory, ``fsync``, ``os.replace``, then ``fsync`` the
directory -- so a crash can never leave a half-written record where the old one
was.  That is the same durability rule the ring builder follows.

Unlike ``ring.json``, this file is *not* write-once.  Reconfiguring a server --
moving it, or pointing it at a new proxy -- is a normal event, so saving over an
existing file is exactly how that change is recorded.

Write modes
-----------
A store is *atomic* (the default) or *non-atomic*.  Atomic mode keeps every
write crash-safe: the bytes go to a temp file in the same directory, are flushed
to storage, and are then renamed over the destination, so a reader -- or a
reboot -- only ever sees a complete file.  Non-atomic mode skips all that: the
bytes are written straight to the destination file and flushed to disk.  That is
one file opened instead of a temp file plus a rename, but an interruption in the
middle of the write leaves the destination truncated instead of untouched.

The mode is chosen when the cluster is deployed -- ``deploy.py up atomic`` (the
default) or ``deploy.py up non_atomic`` -- and is fixed for the life of a
:class:`DataStore`.

Files
-----
Besides its parameters the server holds the files themselves.  :class:`DataStore`
turns a *partition key* into a path under a store directory (``store/`` beside
this module by default) and reads or writes the bytes there.  A key is the
object's path written with ``-`` between its segments::

    JohnDoe-home-documents-hello.txt   ->   store/JohnDoe/home/documents/hello.txt

A read for a key the store does not hold raises :class:`FileNotFoundError`; a key
that cannot be a safe path -- empty, ``.``/``..``, a path separator -- raises
:class:`InvalidKeyError`.  Writes go through the same atomic temp-file-and-rename
step :meth:`DataServerConfig.save` uses, so a crash never leaves a half-written
file.  A delete is atomic too -- the file is renamed to a throwaway name first --
and the empty directory branch it leaves behind is then pruned up to the store
root.

Handoff
-------
When a proxy cannot reach the node that owns a key -- the primary, or a
secondary -- it asks a substitute server to hold the object until the owner
comes back.  That substitute runs the same store, but nests each object under a
directory naming the node it belongs to::

    http%3A%2F%2F10.0.0.5%3A8080/JohnDoe/home/documents/hello.txt

The owner's address is percent-encoded, so the whole URL is one directory name,
and the object path below it is exactly what a normal write would produce.  A
handed-off delete has no bytes to store -- the copy being removed lives on the
down owner -- so the substitute writes :data:`HANDOFF_DELETE_MARKER` in the
object's place, and :meth:`DataStore.is_delete_handoff` tells that placeholder
apart from a real object::

    store.write_handoff("JohnDoe-home-documents-hello.txt", owner, b"hello")
    store.read_handoff("JohnDoe-home-documents-hello.txt", owner)      # b"hello"
    store.delete_handoff("JohnDoe-home-documents-hello.txt", owner)
    store.is_delete_handoff("JohnDoe-home-documents-hello.txt", owner)  # True

A substitute can enumerate what it holds -- :meth:`DataStore.list_handoff_owners`
for the nodes it owes, :meth:`DataStore.list_handoffs` for the keys owed to one --
and drop an entry with :meth:`DataStore.clear_handoff` once it has been handed
back.  That is what a booting server reconciles with.

Usage
-----
Set the parameters and persist them::

    from swift_light.data_server.storage import DataServerConfig

    config = DataServerConfig(
        own_address="http://10.0.0.5:8080",
        proxy_address="http://10.0.0.1:8000",
    )
    config.save()                       # -> .../data_server.json

Read them back at startup::

    config = DataServerConfig.load()
    config.own_address                  # 'http://10.0.0.5:8080'
    config.proxy_address                # 'http://10.0.0.1:8000'

Move the server to a new address by saving a new record::

    DataServerConfig("http://10.0.0.7:8080", config.proxy_address).save()

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, unquote, urlsplit

__all__ = [
    "DataServerConfig",
    "DataStore",
    "InvalidKeyError",
    "DEFAULT_CONFIG_FILENAME",
    "DEFAULT_STORE_DIRNAME",
    "HANDOFF_DELETE_MARKER",
    "SCHEMA_VERSION",
]

DEFAULT_CONFIG_FILENAME = "data_server.json"
"""Default file the parameters are kept in, beside this module."""

DEFAULT_STORE_DIRNAME = "store"
"""Default directory the files are kept in, beside this module."""

SCHEMA_VERSION = 1
"""Version of the on-disk configuration format."""

HANDOFF_DELETE_MARKER = b"__deleted__"
"""The bytes a handed-off delete leaves where the object was.

Written by :meth:`DataStore.delete_handoff` and recognised by
:meth:`DataStore.is_delete_handoff`.  A write handoff and a delete handoff for
one key occupy the same path, so the last one to arrive wins.
"""

_TEMP_SUFFIX = ".tmp"


class DataServerConfig:
    """A data server's global parameters: its own address and the proxy's.

    Both addresses are full URLs, the form the ring uses for its nodes.
    Immutable -- make a new one to reconfigure and save it.
    """

    __slots__ = ("_own_address", "_proxy_address")

    def __init__(self, own_address: str, proxy_address: str) -> None:
        self._own_address = _valid_address("own_address", own_address)
        self._proxy_address = _valid_address("proxy_address", proxy_address)

    # -- parameters ---------------------------------------------------------

    @property
    def own_address(self) -> str:
        """This data server's base URL, e.g. ``http://10.0.0.5:8080``."""
        return self._own_address

    @property
    def proxy_address(self) -> str:
        """The proxy this server reports to, e.g. ``http://10.0.0.1:8000``."""
        return self._proxy_address

    # -- disk ---------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """The parameters as plain, JSON-ready data."""
        return {
            "schema_version": SCHEMA_VERSION,
            "own_address": self._own_address,
            "proxy_address": self._proxy_address,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DataServerConfig":
        """Rebuild the parameters from :meth:`to_dict` output, validating them."""
        if not isinstance(data, Mapping):
            raise TypeError(
                f"config data must be a mapping, got {type(data).__name__}"
            )
        version = data.get("schema_version", SCHEMA_VERSION)
        if version != SCHEMA_VERSION:
            raise ValueError(f"unsupported config schema version {version!r}")

        try:
            own_address = data["own_address"]
            proxy_address = data["proxy_address"]
        except KeyError as exc:
            raise ValueError(f"config data is missing field {exc.args[0]!r}") from None
        return cls(own_address, proxy_address)

    def save(self, path: str | os.PathLike[str] | None = None) -> Path:
        """Atomically write the parameters to ``path`` and return where they landed.

        Defaults to ``data_server.json`` next to this module.  An existing file
        is replaced: reconfiguration is a normal event.
        """
        target = _resolve_path(path)
        _atomic_write_text(target, json.dumps(self.to_dict(), indent=2) + "\n")
        return target

    @classmethod
    def load(cls, path: str | os.PathLike[str] | None = None) -> "DataServerConfig":
        """Read the parameters back from ``path`` (default: beside this module)."""
        source = _resolve_path(path)
        if not source.is_file():
            raise FileNotFoundError(f"no data server config at {source}")
        with source.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return cls.from_dict(data)

    # -- dunder -------------------------------------------------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataServerConfig):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        return (
            f"DataServerConfig(own_address={self._own_address!r}, "
            f"proxy_address={self._proxy_address!r})"
        )


# ---------------------------------------------------------------------------
# The file store
# ---------------------------------------------------------------------------


class InvalidKeyError(ValueError):
    """A partition key that cannot be turned into a safe path."""


class DataStore:
    """The files a data server holds, addressed by partition key.

    A partition key is the object's path written with ``-`` between its
    segments: ``JohnDoe-home-documents-hello.txt`` names
    ``JohnDoe/home/documents/hello.txt``.  The key is turned into a path under
    :attr:`root` and the bytes are read or written there.

    In atomic mode (the default) writes and deletes are crash-safe -- the same
    temp-file-and-rename step :meth:`DataServerConfig.save` uses -- so a crash
    never leaves a half-written or half-deleted file.  In non-atomic mode the
    bytes are written straight to the destination and flushed, with no temp file
    and no rename.  A read for a key that is not held raises
    :class:`FileNotFoundError` rather than returning nothing, and a delete prunes
    any directory branch the file alone kept alive.
    """

    __slots__ = ("_root", "_atomic")

    def __init__(
        self,
        root: str | os.PathLike[str] | None = None,
        *,
        atomic: bool = True,
    ) -> None:
        if not isinstance(atomic, bool):
            raise TypeError(f"atomic must be a bool, got {type(atomic).__name__}")
        self._root = (
            Path(root)
            if root is not None
            else Path(__file__).with_name(DEFAULT_STORE_DIRNAME)
        )
        self._atomic = atomic

    @property
    def root(self) -> Path:
        """The directory the files live under."""
        return self._root

    @property
    def atomic(self) -> bool:
        """Whether writes and deletes go through a temp file and a rename."""
        return self._atomic

    def path_for(self, key: str) -> Path:
        """The path ``key`` names, checked to stay inside :attr:`root`.

        Raises :class:`InvalidKeyError` for a key that is empty, contains a path
        separator, or would escape the store directory.
        """
        target = self._root / _key_to_relative_path(key)
        if not target.resolve().is_relative_to(self._root.resolve()):
            raise InvalidKeyError(f"partition key {key!r} escapes the store")
        return target

    def exists(self, key: str) -> bool:
        """Whether a file is stored for ``key``."""
        return self.path_for(key).is_file()

    def read(self, key: str) -> bytes:
        """The bytes stored for ``key``, or raise :class:`FileNotFoundError`."""
        path = self.path_for(key)
        if not path.is_file():
            raise FileNotFoundError(f"no file for partition key {key!r}")
        return path.read_bytes()

    def write(self, key: str, data: bytes) -> Path:
        """Store ``data`` for ``key`` and return where it landed.

        Atomic in atomic mode (temp file, flush, rename); a direct write and a
        flush in non-atomic mode.
        """
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data must be bytes-like, got {type(data).__name__}")
        path = self.path_for(key)
        _write_bytes(path, bytes(data), atomic=self._atomic)
        return path

    def delete(self, key: str) -> Path:
        """Atomically remove the file for ``key``; prune the empty branch.

        Raises :class:`FileNotFoundError` if no file is stored for ``key``.  In
        atomic mode the file is renamed out of the way before it is unlinked
        (see :func:`_atomic_delete`); in non-atomic mode it is unlinked in
        place.  Either way any directory that only this file kept alive is then
        removed, up to :attr:`root`.
        """
        path = self.path_for(key)
        if not path.is_file():
            raise FileNotFoundError(f"no file for partition key {key!r}")
        _remove_file(path, atomic=self._atomic)
        _remove_empty_branches(path.parent, self._root)
        return path

    # -- handoff ------------------------------------------------------------

    def handoff_path_for(self, key: str, from_node: str) -> Path:
        """The path ``key`` has under ``from_node``'s handoff tree.

        ``http://10.0.0.5:8080`` and ``JohnDoe-home-documents-hello.txt`` give
        ``<root>/http%3A%2F%2F10.0.0.5%3A8080/JohnDoe/home/documents/hello.txt``
        -- the same object path a normal write would produce, under a directory
        naming the node the object belongs to.
        """
        target = self._root / _handoff_dirname(from_node) / _key_to_relative_path(key)
        if not target.resolve().is_relative_to(self._root.resolve()):
            raise InvalidKeyError(f"handoff for {key!r} escapes the store")
        return target

    def write_handoff(self, key: str, from_node: str, data: bytes) -> Path:
        """Atomically hold ``data`` for ``key``, owed to ``from_node``.

        The same atomic temp-file-and-rename step :meth:`write` uses, so a
        substitute's copy is as crash-safe as the owner's would be.
        """
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data must be bytes-like, got {type(data).__name__}")
        path = self.handoff_path_for(key, from_node)
        _write_bytes(path, bytes(data), atomic=self._atomic)
        return path

    def read_handoff(self, key: str, from_node: str) -> bytes:
        """The bytes held for ``key``, or raise :class:`FileNotFoundError`.

        A handed-off delete reads back as :data:`HANDOFF_DELETE_MARKER`; check
        :meth:`is_delete_handoff` before treating the result as an object.
        """
        path = self.handoff_path_for(key, from_node)
        if not path.is_file():
            raise FileNotFoundError(
                f"no handoff for partition key {key!r} from {from_node!r}"
            )
        return path.read_bytes()

    def delete_handoff(self, key: str, from_node: str) -> Path:
        """Record a handed-off delete for ``key`` by writing the placeholder.

        There is nothing to remove here -- the copy being deleted lives on
        ``from_node``, which is unavailable -- so this does not require the key
        to be held and never raises :class:`FileNotFoundError`.  The placeholder
        is written atomically, at the same path a write handoff uses.
        """
        path = self.handoff_path_for(key, from_node)
        _write_bytes(path, HANDOFF_DELETE_MARKER, atomic=self._atomic)
        return path

    def is_delete_handoff(self, key: str, from_node: str) -> bool:
        """Whether the handoff held for ``key`` is a delete placeholder."""
        path = self.handoff_path_for(key, from_node)
        return path.is_file() and path.read_bytes() == HANDOFF_DELETE_MARKER

    def list_handoff_owners(self) -> list[str]:
        """The node addresses this store is holding handoffs for, sorted.

        A directory under :attr:`root` counts as an owner only if its name
        decodes back to a full URL, so an ordinary account directory is not
        mistaken for one.
        """
        if not self._root.is_dir():
            return []
        owners = []
        for child in self._root.iterdir():
            if not child.is_dir():
                continue
            try:
                owners.append(_valid_address("from_node", unquote(child.name)))
            except (TypeError, ValueError):
                continue
        return sorted(owners)

    def list_handoffs(self, from_node: str) -> list[str]:
        """The partition keys held for ``from_node``, sorted.

        Walks ``from_node``'s handoff directory and turns each stored path back
        into a key -- the inverse of :meth:`handoff_path_for`.  The round trip is
        sound because a key's segments can never contain the ``-`` the key is
        split on.  Leftover ``.tmp`` files from an interrupted write are skipped.
        """
        owner_dir = self._root / _handoff_dirname(from_node)
        if not owner_dir.is_dir():
            return []
        keys = [
            "-".join(path.relative_to(owner_dir).parts)
            for path in owner_dir.rglob("*")
            if path.is_file() and not path.name.endswith(_TEMP_SUFFIX)
        ]
        return sorted(keys)

    def clear_handoff(self, key: str, from_node: str) -> Path:
        """Remove a handoff once it has been applied or delivered.

        Like :meth:`delete`, but in the handoff tree: the entry is removed (in
        the store's configured mode) and the empty branch pruned.  Raises
        :class:`FileNotFoundError` if nothing is held for ``key``.
        """
        path = self.handoff_path_for(key, from_node)
        if not path.is_file():
            raise FileNotFoundError(
                f"no handoff for partition key {key!r} from {from_node!r}"
            )
        _remove_file(path, atomic=self._atomic)
        _remove_empty_branches(path.parent, self._root)
        return path


# ---------------------------------------------------------------------------
# Validation, paths and durable writes
# ---------------------------------------------------------------------------


def _valid_address(name: str, value: Any) -> str:
    """Return ``value`` stripped, or raise unless it is a usable full URL."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    address = value.strip()
    if not address:
        raise ValueError(f"{name} must not be empty")
    parts = urlsplit(address)
    if not parts.scheme or not parts.netloc:
        raise ValueError(
            f"{name} must be a full URL like http://10.0.0.5:8080, got {value!r}"
        )
    return address


def _handoff_dirname(node: str) -> str:
    """One safe directory name for ``node``, a full URL percent-encoded.

    ``http://10.0.0.5:8080`` becomes ``http%3A%2F%2F10.0.0.5%3A8080``: the
    separators are escaped, so the whole URL is a single path segment that can
    never climb out of the store.  The address is validated first, so a blank or
    scheme-less value is refused rather than becoming a bogus directory.
    """
    return quote(_valid_address("from_node", node), safe="")


def _resolve_path(path: str | os.PathLike[str] | None) -> Path:
    """The caller's path, or ``data_server.json`` beside this module."""
    if path is None:
        return Path(__file__).with_name(DEFAULT_CONFIG_FILENAME)
    return Path(path)


def _key_to_relative_path(key: str) -> Path:
    """Turn a ``-``-separated partition key into a relative path.

    ``JohnDoe-home-documents-hello.txt`` becomes
    ``JohnDoe/home/documents/hello.txt``.  Anything that is not a plain
    sequence of path segments is refused, so a key can never climb out of the
    store.
    """
    if not isinstance(key, str):
        raise TypeError(f"partition key must be a str, got {type(key).__name__}")
    if not key:
        raise InvalidKeyError("partition key must not be empty")
    segments = key.split("-")
    for segment in segments:
        if (
            segment in ("", ".", "..")
            or "/" in segment
            or "\\" in segment
            or ":" in segment
            or "\x00" in segment
        ):
            raise InvalidKeyError(f"partition key {key!r} is not a usable path")
    return Path(*segments)


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
    """Write ``text`` to ``target`` as UTF-8, atomically."""
    _atomic_write_bytes(target, text.encode("utf-8"))


def _write_bytes(target: Path, data: bytes, *, atomic: bool) -> None:
    """Store ``data`` at ``target`` in the mode the store was configured with.

    ``atomic`` routes to :func:`_atomic_write_bytes` (temp file, flush, rename);
    otherwise the bytes go straight to the destination and are flushed there.
    """
    if atomic:
        _atomic_write_bytes(target, data)
    else:
        _write_bytes_direct(target, data)


def _write_bytes_direct(target: Path, data: bytes) -> None:
    """Write ``data`` straight to ``target`` and flush it -- no rename.

    The destination is truncated before the bytes are written, so an
    interruption leaves it half-written.  That is the trade a non-atomic write
    makes: one file and one flush instead of a temp file and a rename.
    """
    directory = target.parent
    directory.mkdir(parents=True, exist_ok=True)
    with open(target, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(directory)


def _remove_file(target: Path, *, atomic: bool) -> None:
    """Remove ``target`` in the mode the store was configured with.

    ``atomic`` renames the file out of the way before unlinking it (see
    :func:`_atomic_delete`); otherwise it is unlinked in place and the directory
    flushed.
    """
    if atomic:
        _atomic_delete(target)
        return
    target.unlink()
    _fsync_directory(target.parent)


def _atomic_write_bytes(target: Path, data: bytes) -> None:
    """Write ``data`` to ``target`` so a reader sees only a complete file.

    Temp file in the *same directory* (so the swap is a rename, not a copy),
    flushed to storage, renamed over the target, then the directory flushed.
    ``mkstemp`` gives each caller its own name, so two concurrent writes cannot
    clobber each other's work.
    """
    directory = target.parent
    directory.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=target.name + ".", suffix=_TEMP_SUFFIX, dir=str(directory)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    _fsync_directory(directory)


def _atomic_delete(target: Path) -> None:
    """Remove ``target`` so a reader sees it either fully present or gone.

    The file is renamed to a unique throwaway name in the *same directory* -- an
    atomic step, so the file disappears from its key at once -- the directory is
    flushed, and only then is the renamed file unlinked.  A crash in between
    leaves an unreachable temp file, never a half-deleted file.
    """
    directory = target.parent
    descriptor, tombstone_name = tempfile.mkstemp(
        prefix=target.name + ".", suffix=_TEMP_SUFFIX, dir=str(directory)
    )
    os.close(descriptor)
    tombstone = Path(tombstone_name)
    try:
        os.replace(target, tombstone)
        _fsync_directory(directory)
    finally:
        tombstone.unlink(missing_ok=True)
    _fsync_directory(directory)


def _remove_empty_branches(directory: Path, root: Path) -> None:
    """Remove ``directory`` and its parents while they are empty, up to ``root``.

    Stops at the first directory that still holds something, or at ``root``, so
    only the branch that the deleted file alone kept alive is removed.  Best
    effort: a directory that is in use is simply left in place.
    """
    root = root.resolve()
    current = directory
    while True:
        resolved = current.resolve()
        if resolved == root or not resolved.is_relative_to(root):
            return
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent
