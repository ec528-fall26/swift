#!/usr/bin/env python3
"""The swift_light data server's HTTP endpoint.

A data server is mostly storage -- it holds the files a proxy sends it -- and it
answers two kinds of request: a proxy asking whether it is alive, and a client
reading, writing, or deleting a file by its partition key.  Both are served on
the port the server persisted as its ``own_address``::

    GET    /heartbeat                   this server is alive, and here is who it is
    GET    /files?key=<partition key>   read the file stored for that key
    PUT    /files?key=<partition key>   store the request body as that file
    DELETE /files?key=<partition key>   remove the file stored for that key

    GET    /handoff?owner=<url>         list the keys held for that owner
    GET    /handoff?owner=<url>&key=<k> read one handoff held for that owner
    DELETE /handoff?owner=<url>&key=<k> clear one handoff once it is collected

The heartbeat body is exactly
:func:`~swift_light.data_server.heartbeat.heartbeat_response`::

    GET /heartbeat
    -> {"server": "http://10.0.0.5:8080", "message": "alive"}

which is the payload a proxy files under this server's URL.  The probe is driven
by the proxy (:mod:`swift_light.proxy_server.heartbeat`); the data server answers
and never dials out.

A client that asks the wrong server -- one that does not hold the key -- gets a
``404``; a key that is missing or cannot be a path is ``400``.  The file
bytes live under a store directory
(:class:`~swift_light.data_server.storage.DataStore`), so a restart does not lose
them.

A data server also borrows and lends file storage: when a node is unreachable a
proxy leaves the object on a substitute, filed under the owner's address.  Those
*handoff* entries are the fourth family of route.  At boot the server reconciles
them (:mod:`swift_light.data_server.handoff`) -- it asks the proxy for its peers,
collects the handoffs they hold for it, and delivers the ones it holds to their
owners.

Address
-------
The port is not a constant here.  It is the port in the ``own_address`` the
server already saved in its global parameters
(:class:`~swift_light.data_server.storage.DataServerConfig`), so the address a
proxy routes to and the address this server binds can never disagree.  Tests
override ``host``/``port`` to sit on an ephemeral port instead.

The parameters are loaded once, when the server is constructed, and kept for the
handler -- a heartbeat is answered from memory, not by re-reading
``data_server.json`` on every probe.

Concurrency
-----------
``ThreadingHTTPServer`` gives each request its own thread, so a slow request
cannot block another.  The handler reads only the immutable parameters, so a
heartbeat needs no lock; a write goes through the store's atomic temp-file swap,
so two writers to one key cannot produce a torn file (the last swap wins).

Usage
-----
::

    from swift_light.data_server.data_HTTP_server import DataHTTPServer

    server = DataHTTPServer()          # binds the configured own_address
    server.host, server.port           # where it listens

    # then, from a client:
    #   PUT /files?key=JohnDoe-home-documents-hello.txt   (body = file bytes)
    #   GET /files?key=JohnDoe-home-documents-hello.txt   -> the bytes, or 404

    server.serve_forever()             # block

Tests start it without blocking, on an OS-picked port::

    server = DataHTTPServer(host="127.0.0.1", port=0)
    server.start()
    ...
    server.close()

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse, urlsplit

from swift_light.data_server.handoff import HandoffSync
from swift_light.data_server.heartbeat import heartbeat_response
from swift_light.data_server.storage import (
    HANDOFF_DELETE_MARKER,
    DataServerConfig,
    DataStore,
    InvalidKeyError,
)

__all__ = [
    "DataHTTPServer",
    "DataHandler",
    "DEFAULT_HOST",
]

DEFAULT_HOST = ""
"""Bind address when ``own_address`` names no host: the empty string binds every interface."""

_DEFAULT_PORTS = {"http": 80, "https": 443}
"""Port to bind when ``own_address`` gives none."""


class _RequestServer(ThreadingHTTPServer):
    """The data server's listening socket, carrying the server it belongs to.

    The handler reaches the loaded global parameters through
    ``self.server.data_server`` -- so the address the socket binds and the
    address the handler answers with come from the same object.
    """

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        data_server: "DataHTTPServer",
    ) -> None:
        super().__init__(address, handler_class)
        self.data_server = data_server


class _JsonHandler(BaseHTTPRequestHandler):
    """Shared plumbing for the data server's HTTP handlers.

    Mirrors the proxy's handler base: JSON answers, a length on every reply so
    keep-alive works, a silenced request log, and access to the server through
    ``self.server``.
    """

    server_version = "swift_light-data/0.1"
    protocol_version = "HTTP/1.1"

    def _path(self) -> str:
        """The request path, query stripped and trailing slashes removed."""
        return urlparse(self.path).path.rstrip("/")

    @property
    def _data_server(self) -> "DataHTTPServer":
        """The data server this request belongs to (``self.server`` is ours)."""
        return self.server.data_server

    def _send_json(self, status: int, payload: Any) -> None:
        """Reply with ``payload`` as JSON, with a length for keep-alive."""
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, status: int, body: bytes) -> None:
        """Reply with raw ``body`` bytes, with a length for keep-alive."""
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_handoff(self, status: int, body: bytes, is_delete: bool) -> None:
        """Reply with handoff bytes, flagging a delete placeholder in a header."""
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("X-Handoff-Delete", "true" if is_delete else "false")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_no_content(self, status: int = 204) -> None:
        """Reply with no body (a successful delete)."""
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _body(self) -> bytes | None:
        """The request body, or ``None`` if its length is missing or invalid."""
        raw = self.headers.get("Content-Length")
        if raw is None:
            return None
        try:
            length = int(raw)
        except ValueError:
            return None
        if length < 0:
            return None
        return self.rfile.read(length)

    def _query_key(self) -> str | None:
        """The ``key`` query parameter, or ``None`` when absent or blank."""
        values = parse_qs(urlparse(self.path).query).get("key")
        return values[0] if values else None

    def _query_owner(self) -> str | None:
        """The ``owner`` query parameter, or ``None`` when absent or blank."""
        values = parse_qs(urlparse(self.path).query).get("owner")
        return values[0] if values else None

    def _send_error_json(self, status: int, message: str) -> None:
        self._send_json(status, {"error": message})

    def _send_not_found(self) -> None:
        self._send_error_json(404, f"no such endpoint: {self.path}")

    def log_message(self, format: str, *args: Any) -> None:
        """Stay quiet -- request lines on stderr are noise, not information."""
        return


class DataHandler(_JsonHandler):
    """Serves the data server's routes on its own port.

    Routes:

        GET /heartbeat                   this server is alive, and here is who it is
        GET /files?key=<partition key>   read the file stored for that key
        PUT /files?key=<partition key>   store the request body as that file
        DELETE /files?key=<partition key>   remove the file stored for that key

        GET /handoff?owner=<url>         list the keys held for that owner
        GET /handoff?owner=<url>&key=<k> read one handoff held for that owner
        DELETE /handoff?owner=<url>&key=<k> clear one handoff once it is collected

    The heartbeat body is
    :func:`~swift_light.data_server.heartbeat.heartbeat_response`, built from the
    parameters the server loaded at construction.  A read for a key this server
    does not hold is ``404``; a key that is missing or cannot be a path is
    ``400``.  Anything else is a ``404``.

    The handoff routes answer a booting server taking its borrowed objects back:
    ``owner`` names the node the entries belong to.  A read of a delete
    placeholder is a normal ``200`` whose ``X-Handoff-Delete`` header is ``true``.
    """

    def do_GET(self) -> None:
        path = self._path()
        if path == "/heartbeat":
            self._send_json(200, heartbeat_response(self._data_server.config))
        elif path == "/files":
            self._read_file()
        elif path == "/handoff":
            key = self._query_key()
            if key is None:
                self._list_handoffs()
            else:
                self._read_handoff(key)
        else:
            self._send_not_found()

    def do_PUT(self) -> None:
        if self._path() == "/files":
            self._write_file()
        else:
            self._send_not_found()

    def do_DELETE(self) -> None:
        path = self._path()
        if path == "/files":
            self._delete_file()
        elif path == "/handoff":
            key = self._query_key()
            if key is None:
                self._send_error_json(400, "missing query parameter 'key'")
            else:
                self._delete_handoff(key)
        else:
            self._send_not_found()

    # -- file routes --------------------------------------------------------

    def _read_file(self) -> None:
        """Answer ``GET /files``: the bytes for the key, or a 4xx error."""
        key = self._query_key()
        if key is None:
            self._send_error_json(400, "missing query parameter 'key'")
            return
        try:
            data = self._data_server.store.read(key)
        except InvalidKeyError as exc:
            self._send_error_json(400, str(exc))
        except FileNotFoundError:
            self._send_error_json(404, f"no file for partition key {key!r}")
        except OSError as exc:
            self._send_error_json(500, str(exc))
        else:
            self._send_bytes(200, data)

    def _write_file(self) -> None:
        """Answer ``PUT /files``: store the body under the key."""
        key = self._query_key()
        if key is None:
            self._send_error_json(400, "missing query parameter 'key'")
            return
        data = self._body()
        if data is None:
            self._send_error_json(
                400, "a Content-Length header with the file size is required"
            )
            return
        try:
            self._data_server.store.write(key, data)
        except InvalidKeyError as exc:
            self._send_error_json(400, str(exc))
        except OSError as exc:
            self._send_error_json(500, str(exc))
        else:
            self._send_json(201, {"key": key, "bytes": len(data)})

    def _delete_file(self) -> None:
        """Answer ``DELETE /files``: remove the file for the key."""
        key = self._query_key()
        if key is None:
            self._send_error_json(400, "missing query parameter 'key'")
            return
        try:
            self._data_server.store.delete(key)
        except InvalidKeyError as exc:
            self._send_error_json(400, str(exc))
        except FileNotFoundError:
            self._send_error_json(404, f"no file for partition key {key!r}")
        except OSError as exc:
            self._send_error_json(500, str(exc))
        else:
            self._send_no_content()

    # -- handoff routes -----------------------------------------------------

    def _list_handoffs(self) -> None:
        """Answer ``GET /handoff``: the keys held here for ``owner``."""
        owner = self._query_owner()
        if owner is None:
            self._send_error_json(400, "missing query parameter 'owner'")
            return
        try:
            keys = self._data_server.store.list_handoffs(owner)
        except (TypeError, ValueError) as exc:
            self._send_error_json(400, str(exc))
        except OSError as exc:
            self._send_error_json(500, str(exc))
        else:
            self._send_json(200, {"owner": owner, "keys": keys})

    def _read_handoff(self, key: str) -> None:
        """Answer ``GET /handoff`` with a key: the bytes held for ``owner``."""
        owner = self._query_owner()
        if owner is None:
            self._send_error_json(400, "missing query parameter 'owner'")
            return
        try:
            data = self._data_server.store.read_handoff(key, owner)
        except InvalidKeyError as exc:
            self._send_error_json(400, str(exc))
        except FileNotFoundError:
            self._send_error_json(404, f"no handoff held for key {key!r}")
        except (TypeError, ValueError) as exc:
            self._send_error_json(400, str(exc))
        except OSError as exc:
            self._send_error_json(500, str(exc))
        else:
            self._send_handoff(200, data, data == HANDOFF_DELETE_MARKER)

    def _delete_handoff(self, key: str) -> None:
        """Answer ``DELETE /handoff``: drop an entry once it has been collected."""
        owner = self._query_owner()
        if owner is None:
            self._send_error_json(400, "missing query parameter 'owner'")
            return
        try:
            self._data_server.store.clear_handoff(key, owner)
        except InvalidKeyError as exc:
            self._send_error_json(400, str(exc))
        except FileNotFoundError:
            self._send_error_json(404, f"no handoff held for key {key!r}")
        except (TypeError, ValueError) as exc:
            self._send_error_json(400, str(exc))
        except OSError as exc:
            self._send_error_json(500, str(exc))
        else:
            self._send_no_content()


class DataHTTPServer:
    """The data server's HTTP listener: one port, its routes.

    Constructing it loads the server's global parameters, opens its file store,
    binds the port its ``own_address`` names, and keeps both for the handler.
    Nothing is served until :meth:`start` or :meth:`serve_forever`.

    Parameters
    ----------
    config:
        Already-loaded global parameters.  Pass them when the process loaded
        them at startup; leave it out and they are read from ``config_path``.
    config_path:
        Where to load the parameters from when ``config`` is not given (default:
        ``data_server.json`` beside the storage module).
    store_root:
        Directory the files are stored under.  Leave it out for the default
        store beside the storage module; tests pass a temporary directory.
    host / port:
        Bind address and port.  Leave them out to use the ones ``own_address``
        names; tests pass ``port=0`` for an OS-picked free port and read the
        choice back from :attr:`port`.

    Binding and serving are separate from reconciliation: :meth:`start` begins
    serving and then runs the boot handoff reconciliation once, in the
    background, so a slow or unreachable peer never delays the listener.
    """

    def __init__(
        self,
        config: DataServerConfig | None = None,
        *,
        config_path: str | os.PathLike[str] | None = None,
        store_root: str | os.PathLike[str] | None = None,
        host: str | None = None,
        port: int | None = None,
    ) -> None:
        self.config = config if config is not None else DataServerConfig.load(config_path)
        self.store = DataStore(store_root)
        bind_host, bind_port = _bind_address(self.config.own_address)
        if host is not None:
            bind_host = host
        if port is not None:
            bind_port = port
        self._server = _RequestServer(
            (bind_host, bind_port), DataHandler, data_server=self
        )
        self._thread: threading.Thread | None = None
        self._sync_thread: threading.Thread | None = None
        self.handoff_report: dict[str, Any] | None = None

    # -- address ------------------------------------------------------------

    @property
    def host(self) -> str:
        """The bound host (``own_address``'s, or the ``host`` override)."""
        return self._server.server_address[0]

    @property
    def port(self) -> int:
        """The bound port (``0`` was replaced by the OS-picked port)."""
        return self._server.server_address[1]

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        """Serve in a background thread and return immediately.

        Calling it twice is a no-op, so a caller does not have to track whether
        the server is already running.
        """
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"data-server-port-{self.port}",
            daemon=True,
        )
        self._thread.start()
        self._sync_thread = threading.Thread(
            target=self._reconcile_quietly,
            name=f"data-server-handoff-{self.port}",
            daemon=True,
        )
        self._sync_thread.start()

    def serve_forever(self) -> None:
        """Serve and block until interrupted, then close."""
        self.start()
        try:
            self._thread.join()
        except KeyboardInterrupt:
            self.close()

    def shutdown(self) -> None:
        """Stop serving; the socket stays open until :meth:`close`."""
        self._server.shutdown()

    def close(self) -> None:
        """Stop serving and release the socket."""
        self.shutdown()
        self._server.server_close()

    # -- handoff reconciliation ---------------------------------------------

    def reconcile_handoffs(self) -> dict[str, Any]:
        """Collect this server's handoffs and deliver the ones it holds.

        Asks the proxy for the other data servers, pulls the handoffs they hold
        for this one, and sends the handoffs this one holds to their owners.
        Best effort: failures are reported in the result, never raised.
        """
        return HandoffSync(
            self.store, self.config.own_address, self.config.proxy_address
        ).reconcile()

    def _reconcile_quietly(self) -> None:
        """Run :meth:`reconcile_handoffs` at boot without ever failing startup."""
        try:
            self.handoff_report = self.reconcile_handoffs()
        except Exception as exc:  # noqa: BLE001 - a boot step must not be fatal
            self.handoff_report = {"error": f"{type(exc).__name__}: {exc}"}


def _bind_address(own_address: str) -> tuple[str, int]:
    """The ``(host, port)`` ``own_address`` names.

    The host falls back to ``DEFAULT_HOST`` and the port to the scheme's
    default, so an address like ``http://10.0.0.5`` still binds something
    sensible.
    """
    parts = urlsplit(own_address)
    host = parts.hostname or DEFAULT_HOST
    port = parts.port
    if port is None:
        port = _DEFAULT_PORTS.get(parts.scheme, 80)
    return host, port
