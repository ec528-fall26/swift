#!/usr/bin/env python3
"""Heartbeat replies for swift_light's data servers.

A data server's part in a heartbeat is small: when the proxy asks whether it is
alive, it says yes, and names itself so the answer can be filed under the right
server.  This module is that answer, and nothing else::

    heartbeat_response()   the reply payload for one heartbeat probe

The exchange is driven by the proxy -- it probes every data server it knows on
its data port (8010) -- so a data server answers; it never dials out.  What the
probe looks like on the wire belongs to the proxy
(:mod:`swift_light.proxy_server.heartbeat`); this module only produces the body
the reply carries.

The server's own address is not passed in by hand.  It is the ``own_address``
the server already persisted in its global parameters
(:class:`~swift_light.data_server.storage.DataServerConfig`), so the reply
always names the address the ring and the proxies know this server by.

Usage
-----
::

    from swift_light.data_server.heartbeat import heartbeat_response

    heartbeat_response()
    # {'server': 'http://10.0.0.5:8080', 'message': 'alive'}

A server that already loaded its global parameters at startup passes them in,
so a probe does not read the file again::

    config = DataServerConfig.load()
    heartbeat_response(config)

Tests live in ``src/test/``; this module has no entry point of its own.
"""

from __future__ import annotations

import os
from typing import Any

from swift_light.data_server.storage import DataServerConfig

__all__ = [
    "heartbeat_response",
    "ALIVE_MESSAGE",
]

ALIVE_MESSAGE = "alive"
"""I AM ALIVE"""


def heartbeat_response(
    config: DataServerConfig | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Return the reply payload for one heartbeat probe.

    The payload names this server and states that it is alive::

        {"server": "http://10.0.0.5:8080", "message": "alive"}

    ``"server"`` is this server's persisted ``own_address`` -- the ring node
    string a proxy already routes by -- and ``"message"`` is
    :data:`ALIVE_MESSAGE`.

    Pass ``config`` when the global parameters were already loaded at startup,
    which is what a request handler should do so a probe does not re-read the
    file.  With no argument they are loaded here instead; ``path`` overrides
    the default ``data_server.json``.
    """
    if config is None:
        config = DataServerConfig.load(path)
    return {"server": config.own_address, "message": ALIVE_MESSAGE}
