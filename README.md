# RNS FileSync

Decentralized peer-to-peer file synchronization over the Reticulum Network Stack.

This package is designed as an embeddable library for host programs such as
MeshChatX, with a thin CLI for standalone use. There is no TUI.

For usability and easy usage this follows rngit permissions and configuration.

## Features

- Peer-to-peer sync with no central server
- Delta sync for changed blocks
- Path-jailed writes and rngit-style ACL (`permission:target`)
- Embed-safe: reuses an existing `RNS.Reticulum` instance when present
- Works over any Reticulum medium (radio, LoRa, WiFi, internet)

## Install

```bash
pip install -e ".[dev]"
```

Requires `rns>=1.3.9`. Current package version: **1.0.0**.

## Config

FileSync uses an rngit-like config directory (default `~/.rns_filesync/`):

```bash
rns-filesync -d ~/shared
# creates ~/.rns_filesync/config on first run
```

| Flag | Meaning |
|------|---------|
| `--config DIR` | FileSync config directory (default `~/.rns_filesync`) |
| `--rnsconfig DIR` | Reticulum config directory (default `~/.reticulum`) |
| `-d DIR` | Sync directory (overrides `[filesync] directory`) |

Example `~/.rns_filesync/config`:

```ini
[filesync]
announce_interval = 300
# directory = ~/shared
identity = rns_filesync
# peers = 9710b86ba12c42d1d8f30f74fe509286
# blocked_identities = d31aeea49873006f13b3415520666a4e

[aliases]
# alice = d09285e660cfe27cee6d9a0beb58b7e0

[access]
# sync = r:all, w:9710b86ba12c42d1d8f30f74fe509286, d:9710b86ba12c42d1d8f30f74fe509286

[logging]
loglevel = 4
```

Identities are stored under `~/.rns_filesync/identities/`.

## Permissions

Same rule form as rngit: `permission:target`.

| Perm | Meaning |
|------|---------|
| `r` | read |
| `w` | write |
| `d` | delete |
| `rw` | read/write |
| `rwd` | read/write/delete |
| `adm` | admin (all) |

Targets: identity hash, alias, `all`, or `none`.

When any ACL rule is loaded, access is **deny by default**. With no rules configured, the peer stays open (library/embed default).

Sidecar `.allowed` files (first found wins alongside config `[access]`):

- `<sync_dir>.allowed`
- `<parent>/<name>.allowed`
- `<sync_dir>/.allowed`

See `permissions.example`.

```bash
rns-filesync -d ~/shared --allow r:all --allow w:9710b86ba12c42d1d8f30f74fe509286
```

## CLI

```bash
rns-filesync -d ~/shared
rns-filesync -d ~/shared -p <peer_identity_hash>
rns-filesync --config ~/.rns_filesync --rnsconfig ~/.reticulum -d ~/shared
```

`-p` expects a peer **identity hash**. A destination hash is accepted as fallback
after the peer has announced.

Interactive commands: `status`, `peers`, `files`, `connect`, `disconnect`,
`browse`, `download`, `announce`, `quit`.

## Library embed (MeshChatX-style)

```python
from rns_filesync import FileSyncService
from rns_filesync.permissions import PermissionStore

perms = PermissionStore()
perms.add_rule("r:all")
perms.add_rule("w:9710b86ba12c42d1d8f30f74fe509286")

service = FileSyncService(
    identity=host_identity,
    sync_directory="/path/to/sync",
    reticulum=existing_reticulum,
    permissions=perms,
)
dest_hash = service.start(monitor=True)
service.connect_peer(peer_identity_hash)
service.stop()
```

Keep aspect `rns_filesync.filesync` for wire compatibility.

## Tests

```bash
pytest -m "not live and not exploratory"
pytest -m e2e
pytest -m live
```

Markers: `unit`, `acceptance`, `smoke`, `e2e`, `fuzz`, `live`, `exploratory`.

## License

BSD-2-Clause. See LICENSE.
