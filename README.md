# RNS FileSync

[Русская](README_RU.md)

> [!WARNING]  
> I still consider this experimental, use at your own risk.

**Decentralized peer-to-peer file synchronization using the Reticulum Network Stack**

RNS FileSync enables automatic file synchronization between devices without requiring central servers, internet connectivity, or cloud services. It works over any network medium supported by Reticulum, including radio, LoRa, WiFi, or the internet, making it ideal for off-grid, privacy-focused, and resilient file sharing.

## Features

- **Peer-to-Peer**: No central servers or coordination required
- **Bandwidth Efficient**: Delta sync transmits only changed blocks, not entire files
- **Works Anywhere**: Operates over radio, LoRa, mesh networks, or internet
- **Automatic Sync**: Monitors directories and syncs changes in real-time
- **Permission System**: Fine-grained access control (read/write/delete)
- **Terminal UI**: Built-in text interface for monitoring and management
- **Multi-Peer**: Connect to multiple peers simultaneously with mesh distribution
- **Cryptographically Secure**: End-to-end encrypted via Reticulum's transport layer

## Quick Start

### Prerequisites

Install Reticulum Network Stack:

```bash
pip install rns --break-system-packages
```

Or using pipx:

```bash
pipx install rns
```

### Installation

```bash
git clone https://github.com/Quad4-Software/RNS-Filesync.git
cd RNS-Filesync
pip install -r requirements.txt --break-system-packages
```

Or clone via SSH:

```bash
git clone git@github.com:Quad4-Software/RNS-Filesync.git
cd RNS-Filesync
pip install -r requirements.txt --break-system-packages
```

### Basic Usage

**1. Start your first peer:**

```bash
python rns_filesync.py -d ~/shared
```

This creates a sync directory and displays your peer's destination hash in the Terminal UI.

**2. Start a second peer and connect:**

On another device (or terminal), run:

```bash
python rns_filesync.py -d ~/shared -p <destination_hash_from_peer1>
```

Your files will automatically sync between both directories. Add, modify, or delete files in either directory and watch them sync.

### Interactive Commands

While running, you can type commands at the prompt:

- `status` - Show file count and connected peers
- `peers` - List all connected peers with their hashes
- `connect <hash>` - Connect to a peer by destination hash
- `disconnect <index>` - Disconnect from a peer by index (see `peers`)
- `browse <index|hash>` - Browse files on a connected peer
- `download <file>` - Download a file from the browsed peer
- `download_all` - Download all files from the browsed peer
- `announce` - Manually announce this peer on the network
- `logs` - Switch to the log view (TUI)
- `files` - Switch back to the file view (TUI)
- `quit` - Exit the application (`exit` or `q` also work)

## Command-Line Options

```
-d PATH              Directory to synchronize (required)
-i NAME              Identity name to use (default: rns_filesync)
-p HASH              Peer destination hash to connect to (can specify multiple times)
-n                   Disable file monitoring (sync on connect only)
-a SECONDS           Announce interval in seconds (default: 300)
--permissions-file FILE  Load permissions from file
--allow HASH         Allow specific peer identity hash
--perms LIST         Permissions for --allow (comma-separated: read,write,delete)
--no-tui             Disable Terminal UI, use plain logging
-v                   Verbose logging (-vv for more, -vvv for debug)
--config PATH        Path to Reticulum config directory
```

## Permissions and Access Control

Control who can read, write, or delete files with a permissions file.

See `permissions.example` for a template.

**Create `permissions.conf`:**

```
# Allow specific peer full access
a1b2c3d4e5f6g7h8 read,write,delete

# Read-only access for another peer
9876543210fedcba read

# Allow anyone to read (wildcard)
* read
```

**Use with:**

```bash
python rns_filesync.py -d ~/shared --permissions-file permissions.conf
```

**Or via command line:**

```bash
python rns_filesync.py -d ~/shared --allow a1b2c3d4e5f6g7h8 --perms read,write
```

### Permission Types

- `read` - Peer can list and download files
- `write` - Peer can upload and modify files
- `delete` - Peer can delete files

Without a permissions file, all peers have full access.

## How It Works

RNS FileSync watches a directory on your device. When you add or change a file, it automatically sends it to connected peers. When a peer adds or changes a file, you automatically receive it.

#### Architecture

**Transport Layer**: Built on Reticulum Network Stack (RNS) for cryptographic identity-based addressing and transport encryption.

**File Change Detection**: Monitors sync directory every 5 seconds using filesystem scans with SHA-256 hash comparison.

**Delta Synchronization Protocol**:
1. Files divided into 4KB blocks (`BLOCK_SIZE = 4096`)
2. Each block independently hashed with SHA-256
3. Block hashes exchanged with peers
4. Only blocks with differing hashes transmitted
5. Receiver applies delta patches to existing files

**Packet Fragmentation**: 
- Large file transfers use `RNS.Resource` with automatic chunking
- Delta blocks split into 256-byte sub-chunks (`MAX_PACKET_DATA_SIZE`)
- Sub-chunks sent as individual packets, reassembled on receive
- Verified with SHA-256 hash post-assembly

**Conflict Resolution**: Last-write-wins strategy. Most recent change propagates to all peers.

**Mesh Distribution**: Files propagate transitively through connected peers. If A connects to B, and B connects to C, files sync A↔B↔C.

#### Message Types

- `file_list` - Inventory of files with hashes
- `file_list_request` - Request peer's file list
- `file_request` - Request full file transfer
- `delta_request` - Request delta blocks for existing file
- `file_chunk` - Transfer file/delta data payload
- `file_complete` - Signal transfer completion with final hash
- `file_update` - Notify peers of local file change
- `file_deletion` - Notify peers of local file deletion

## License

RNS FileSync is licensed under [BSD-2-Clause](LICENSE).

## Dependencies

`rns` (Reticulum) - [Reticulum License](https://raw.githubusercontent.com/markqvist/Reticulum/refs/heads/master/LICENSE)