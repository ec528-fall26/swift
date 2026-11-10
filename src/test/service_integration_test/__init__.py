"""End-to-end tests: more than one swift_light service wired together.

Everything here drives the deployed topology rather than a single component:

* ``test_client_end_to_end.py``  -- client -> proxy -> data servers lifecycle
* ``test_write_fallback.py``     -- client quorum against real data servers
* ``test_read_fallback.py``      -- client read order against real data servers
* ``test_proxy_http.py``         -- proxy routing and heartbeats with real members
* ``test_ring_atomicity.py``     -- ring replacement seen through a live proxy
* ``test_remote_migration.py``   -- ring migration over HTTP against real servers

The servers, proxies and rings are started in-process on OS-assigned loopback
ports with temporary state, so the tests still need no deployed cluster.  Tests
for one component on its own live in ``client/``, ``data/`` and ``proxy/``.
"""
