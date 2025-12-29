#!/usr/bin/env python3

"""RNS FileSync - Peer-to-Peer File Synchronization over Reticulum.

This module provides a file synchronization system that allows peers to
synchronize files over the Reticulum Network Stack (RNS). It includes
a terminal user interface (TUI) for monitoring and managing file sync
operations, permission-based access control, and delta synchronization
for efficient file transfers.
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
import threading
import time
from collections import deque

import RNS
from RNS.vendor import umsgpack

APP_NAME = "rns_filesync"
APP_TIMEOUT = 30.0
BLOCK_SIZE = 4096
CHUNK_SIZE = 7000
SCAN_INTERVAL = 5.0
MAX_PACKET_DATA_SIZE = 256

peer_identity = None
peer_destination = None
connected_peers = []
connected_peers_lock = threading.Lock()
file_monitor_active = False
sync_directory = None
file_hashes = {}
file_blocks = {}
file_hashes_lock = threading.Lock()
known_peers = set()
peer_permissions = {}
permissions_lock = threading.Lock()
whitelist_enabled = False

desired_peers = set()
desired_peers_lock = threading.Lock()
peer_reconnect_active = False

transfer_stats = {
    "last_transfer_bytes": 0,
    "last_transfer_time": 0,
    "last_transfer_start": 0,
    "current_speed": 0,
}
transfer_stats_lock = threading.Lock()

active_outgoing_transfers = {}
active_outgoing_transfers_lock = threading.Lock()

active_resources = {}
active_resources_lock = threading.Lock()


class Colors:
    """ANSI color codes for terminal output."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"

    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"


class SimpleTUI:
    """Terminal User Interface for RNS FileSync.

    Provides a text-based interface for monitoring file synchronization,
    viewing connected peers, browsing remote files, and managing sync operations.
    """

    def __init__(self):
        """Initialize the TUI with default settings."""
        self.enabled = True
        self.terminal_height = 0
        self.terminal_width = 0
        self.log_lines = deque(maxlen=1000)
        self.log_lock = threading.Lock()
        self.status_info = {
            "files": 0,
            "peers": 0,
            "identity": "",
            "destination": "",
            "directory": "",
            "permissions": False,
            "speed": 0,
        }
        self.status_lock = threading.Lock()
        self.update_terminal_size()
        self.refresh_timer = None
        self.current_input = ""
        self.input_lock = threading.Lock()
        self.view_mode = "files"
        self.view_lock = threading.Lock()
        self.file_list = []
        self.file_list_lock = threading.Lock()
        self.scroll_offset = 0
        self.browser_peer = None
        self.remote_files = []
        self.remote_files_lock = threading.Lock()

    def update_terminal_size(self):
        """Update terminal dimensions, falling back to defaults on error."""
        try:
            size = shutil.get_terminal_size()
            self.terminal_width = size.columns
            self.terminal_height = size.lines
        except Exception:
            self.terminal_width = 80
            self.terminal_height = 24

    def clear_screen(self):
        """Clear the terminal screen."""
        sys.stdout.write("\033[2J")
        sys.stdout.write("\033[H")
        sys.stdout.flush()

    def move_cursor(self, row, col):
        """Move cursor to specified position.

        Args:
            row: Row position (1-indexed).
            col: Column position (1-indexed).

        """
        sys.stdout.write(f"\033[{row};{col}H")

    def clear_line(self):
        """Clear the current line."""
        sys.stdout.write("\033[2K")

    def hide_cursor(self):
        """Hide the terminal cursor."""
        sys.stdout.write("\033[?25l")

    def show_cursor(self):
        """Show the terminal cursor."""
        sys.stdout.write("\033[?25h")

    def add_log(self, message, level="INFO"):
        """Add a log message to the TUI log buffer.

        Args:
            message: Log message text.
            level: Log level (string or RNS log constant).

        """
        with self.log_lock:
            timestamp = time.strftime("%H:%M:%S")

            if level in ("ERROR", RNS.LOG_ERROR):
                color = Colors.RED
                level_str = "ERR"
            elif level in ("WARNING", RNS.LOG_WARNING):
                color = Colors.YELLOW
                level_str = "WARN"
            elif level in ("NOTICE", RNS.LOG_NOTICE):
                color = Colors.BRIGHT_CYAN
                level_str = "NOTE"
            elif level in ("INFO", RNS.LOG_INFO):
                color = Colors.GREEN
                level_str = "INFO"
            elif level in ("VERBOSE", RNS.LOG_VERBOSE):
                color = Colors.BRIGHT_BLACK
                level_str = "VERB"
            elif level in ("DEBUG", RNS.LOG_DEBUG):
                color = Colors.BRIGHT_BLACK
                level_str = "DBG"
            else:
                color = Colors.WHITE
                level_str = "LOG"

            formatted = f"{Colors.BRIGHT_BLACK}[{timestamp}]{Colors.RESET} {color}{level_str:4}{Colors.RESET} {message}"
            self.log_lines.append(formatted)

    def update_status(self, **kwargs):
        """Update status information displayed in the TUI.

        Args:
            **kwargs: Status fields to update (files, peers, identity, etc.).

        """
        with self.status_lock:
            self.status_info.update(kwargs)

    def format_speed(self, bytes_per_sec):
        """Format transfer speed in human-readable units.

        Args:
            bytes_per_sec: Speed in bytes per second.

        Returns:
            Formatted speed string (Gbps, Mbps, Kbps, or bps), or None if zero.

        """
        if bytes_per_sec == 0:
            return None

        bits_per_sec = bytes_per_sec * 8

        if bits_per_sec >= 1_000_000_000:
            return f"{bits_per_sec / 1_000_000_000:.2f} Gbps"
        if bits_per_sec >= 1_000_000:
            return f"{bits_per_sec / 1_000_000:.2f} Mbps"
        if bits_per_sec >= 1_000:
            return f"{bits_per_sec / 1_000:.2f} Kbps"
        return f"{bits_per_sec:.0f} bps"

    def format_size(self, size):
        """Format file size in human-readable units.

        Args:
            size: Size in bytes.

        Returns:
            Formatted size string with appropriate unit.

        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f}{unit}"
            size /= 1024.0
        return f"{size:.1f}PB"

    def get_file_type(self, filepath):
        """Determine file type category based on extension.

        Args:
            filepath: Path to the file.

        Returns:
            File type category string (e.g., "[Audio]", "[Video]", "[File]").

        """
        ext = os.path.splitext(filepath)[1].lower()
        if ext in [".mp3", ".wav", ".flac", ".ogg", ".m4a", ".aac"]:
            return "[Audio]"
        if ext in [".mp4", ".mkv", ".avi", ".mov", ".webm"]:
            return "[Video]"
        if ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]:
            return "[Image]"
        if ext in [".txt", ".md", ".log", ".mu"]:
            return "[Text]"
        if ext in [".pdf"]:
            return "[PDF]"
        if ext in [".zip", ".tar", ".gz", ".7z", ".rar"]:
            return "[Archive]"
        if ext in [".py", ".js", ".c", ".cpp", ".java", ".go"]:
            return "[Code]"
        return "[File]"

    def update_file_list(self, directory):
        """Scan directory and update the file list for display.

        Args:
            directory: Directory path to scan.

        """
        if not directory or not os.path.exists(directory):
            return

        files = []
        try:
            for root, dirs, filenames in os.walk(directory):
                for filename in filenames:
                    if filename.startswith(".rns-filesync"):
                        continue
                    full_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(full_path, directory)
                    try:
                        size = os.path.getsize(full_path)
                        files.append(
                            {
                                "path": rel_path,
                                "size": size,
                                "type": self.get_file_type(filename),
                            },
                        )
                    except Exception as e:
                        RNS.log(f"Error reading file {rel_path}: {e}", RNS.LOG_DEBUG)
        except Exception as e:
            RNS.log(f"Error scanning directory {directory}: {e}", RNS.LOG_DEBUG)

        files.sort(key=lambda x: x["path"])

        with self.file_list_lock:
            self.file_list = files

    def set_view_mode(self, mode):
        """Set the current view mode (files, logs, browser).

        Args:
            mode: View mode string.

        """
        with self.view_lock:
            self.view_mode = mode
            self.scroll_offset = 0

    def draw_box(self, row, col, width, height, title=None):
        """Draw a box with optional title using box-drawing characters.

        Args:
            row: Top row position.
            col: Left column position.
            width: Box width.
            height: Box height.
            title: Optional title text for the box.

        """
        for i in range(height):
            self.move_cursor(row + i, col)
            if i == 0:
                if title:
                    title_text = f" {title} "
                    left_pad = (width - len(title_text) - 2) // 2
                    right_pad = width - len(title_text) - left_pad - 2
                    sys.stdout.write(f"┌{'─' * left_pad}{title_text}{'─' * right_pad}┐")
                else:
                    sys.stdout.write(f"┌{'─' * (width - 2)}┐")
            elif i == height - 1:
                sys.stdout.write(f"└{'─' * (width - 2)}┘")
            else:
                sys.stdout.write(f"│{' ' * (width - 2)}│")

    def draw_status_area(self):
        """Draw the status information area at the top of the screen.

        Returns:
            Height of the status area in rows.

        """
        with self.status_lock:
            info = self.status_info.copy()

        status_height = 8
        self.draw_box(
            1, 1, self.terminal_width - 2, status_height, "RNS FileSync Status",
        )

        self.move_cursor(2, 3)
        sys.stdout.write(
            f"{Colors.CYAN}Directory:{Colors.RESET} {info['directory'][: self.terminal_width - 20]}",
        )

        self.move_cursor(3, 3)
        sys.stdout.write(
            f"{Colors.CYAN}Identity:{Colors.RESET} {Colors.BRIGHT_YELLOW}{info['identity'][:40]}{Colors.RESET}",
        )

        self.move_cursor(4, 3)
        sys.stdout.write(
            f"{Colors.CYAN}Destination:{Colors.RESET} {Colors.BRIGHT_GREEN}{info['destination'][:40]}{Colors.RESET}",
        )

        self.move_cursor(5, 3)
        sys.stdout.write(
            f"{Colors.CYAN}Files:{Colors.RESET} {Colors.BRIGHT_WHITE}{info['files']}{Colors.RESET}  ",
        )
        sys.stdout.write(
            f"{Colors.CYAN}Peers:{Colors.RESET} {Colors.BRIGHT_WHITE}{info['peers']}{Colors.RESET}  ",
        )

        if info["permissions"]:
            sys.stdout.write(
                f"{Colors.CYAN}Permissions:{Colors.RESET} {Colors.GREEN}Enabled{Colors.RESET}  ",
            )

        speed_str = self.format_speed(info["speed"])
        if speed_str:
            sys.stdout.write(
                f"{Colors.CYAN}Speed:{Colors.RESET} {Colors.BRIGHT_MAGENTA}{speed_str}{Colors.RESET}",
            )

        self.move_cursor(6, 3)
        sys.stdout.write(
            f"{Colors.BRIGHT_BLACK}Commands: status | peers | connect <hash> | disconnect <#> | browse <peer> | logs | quit{Colors.RESET}",
        )

        return status_height

    def draw_files_area(self, start_row):
        """Draw the file list area.

        Args:
            start_row: Starting row position.

        Returns:
            Ending row position.

        """
        usable_height = self.terminal_height - 1
        view_height = max(1, usable_height - start_row - 2)

        with self.view_lock:
            mode = self.view_mode

        if mode == "browser" and self.browser_peer:
            title = f"Remote Files - Peer {RNS.prettyhexrep(self.browser_peer)[:16]}..."
            with self.remote_files_lock:
                files = self.remote_files[:]
        else:
            title = "Local Files"
            with self.file_list_lock:
                files = self.file_list[:]

        self.draw_box(start_row, 1, self.terminal_width - 2, view_height + 2, title)

        if not files:
            self.move_cursor(start_row + 1, 3)
            sys.stdout.write(f"{Colors.DIM}No files to display{Colors.RESET}")
        else:
            start_idx = self.scroll_offset
            end_idx = min(start_idx + view_height, len(files))

            for i, file_info in enumerate(files[start_idx:end_idx]):
                if i >= view_height:
                    break

                self.move_cursor(start_row + 1 + i, 3)
                self.clear_line()

                file_path = file_info["path"]
                file_size = self.format_size(file_info["size"])
                file_type = file_info["type"]

                max_path_len = self.terminal_width - 40
                if len(file_path) > max_path_len:
                    file_path = "..." + file_path[-(max_path_len - 3) :]

                line = f"{Colors.CYAN}{file_type:<12}{Colors.RESET} {Colors.WHITE}{file_size:>10}{Colors.RESET}  {file_path}"
                sys.stdout.write(line[: self.terminal_width - 6])

        for i in range(
            len(files[self.scroll_offset : self.scroll_offset + view_height])
            if files
            else 0,
            view_height,
        ):
            self.move_cursor(start_row + 1 + i, 3)
            self.clear_line()

        return start_row + view_height + 2

    def draw_log_area(self, start_row):
        """Draw the log area.

        Args:
            start_row: Starting row position.

        Returns:
            Ending row position.

        """
        usable_height = self.terminal_height - 1
        log_height = max(1, usable_height - start_row - 2)

        self.draw_box(start_row, 1, self.terminal_width - 2, log_height + 2, "Logs")

        with self.log_lock:
            recent_logs = list(self.log_lines)[-log_height:]

        for i, log_line in enumerate(recent_logs):
            self.move_cursor(start_row + 1 + i, 3)
            max_len = self.terminal_width - 6
            if len(log_line) > max_len:
                sys.stdout.write(log_line[:max_len])
            else:
                sys.stdout.write(log_line)
                self.clear_line()

        for i in range(len(recent_logs), log_height):
            self.move_cursor(start_row + 1 + i, 3)
            self.clear_line()

        return start_row + log_height + 2

    def draw_input_area(self, restore_input=True):
        """Draw the input area at the bottom of the screen.

        Args:
            restore_input: Whether to restore previously entered input.

        """
        self.update_terminal_size()
        input_row = self.terminal_height
        self.move_cursor(input_row, 1)
        self.clear_line()
        prompt = f"{Colors.BOLD}{Colors.BRIGHT_CYAN}>{Colors.RESET} "
        sys.stdout.write(prompt)
        sys.stdout.flush()

        if restore_input:
            with self.input_lock:
                current_input = self.current_input
                if current_input:
                    sys.stdout.write(current_input)
                    sys.stdout.flush()

    def save_current_input(self, text):
        """Save the current input text.

        Args:
            text: Input text to save.

        """
        with self.input_lock:
            self.current_input = text

    def clear_current_input(self):
        """Clear the saved input text."""
        with self.input_lock:
            self.current_input = ""

    def refresh_display(self, full_clear=False, refresh_input=False):
        """Refresh the entire TUI display.

        Args:
            full_clear: Whether to clear the screen before refreshing.
            refresh_input: Whether to refresh the input area (default: False to avoid interrupting typing).

        """
        if not self.enabled:
            return

        if full_clear:
            refresh_input = True

        saved_cursor = False
        if not refresh_input:
            sys.stdout.write("\033[s")  # save cursor position
            saved_cursor = True

        self.update_terminal_size()
        self.hide_cursor()

        if full_clear:
            self.clear_screen()

        status_end = self.draw_status_area()
        content_start = status_end + 1

        with self.view_lock:
            mode = self.view_mode

        if mode == "logs":
            self.draw_log_area(content_start)
        else:
            self.draw_files_area(content_start)

        if refresh_input:
            self.draw_input_area(restore_input=True)

        self.show_cursor()
        if saved_cursor:
            sys.stdout.write("\033[u")  # restore cursor position
        sys.stdout.flush()

    def start_refresh_timer(self):
        """Start the background thread that periodically refreshes the display."""
        def refresh_loop():
            while self.enabled:
                time.sleep(1)
                self.refresh_display(refresh_input=False)

        self.refresh_timer = threading.Thread(target=refresh_loop, daemon=True)
        self.refresh_timer.start()

    def stop(self):
        """Stop the TUI and restore original logging."""
        self.enabled = False
        self.show_cursor()
        self.clear_screen()

        global original_rns_log
        if original_rns_log:
            RNS.log = original_rns_log


tui = None


class TUILogHandler:
    """File-like object handler for redirecting stdout to TUI."""

    def __init__(self, tui_instance):
        """Initialize the log handler.

        Args:
            tui_instance: SimpleTUI instance to write logs to.

        """
        self.tui = tui_instance

    def write(self, data):
        """Write data to the TUI log.

        Args:
            data: Data string to write.

        """
        if self.tui and self.tui.enabled and data.strip():
            self.tui.add_log(data.strip(), "INFO")

    def flush(self):
        """Flush operation (no-op for this handler)."""


original_rns_log = None


def rns_log_hook(message, level, _override_destination=False):
    """Intercept RNS log messages and display in TUI.

    Args:
        message: Log message text.
        level: Log level constant.
        _override_destination: Unused parameter for compatibility.

    """
    if tui and tui.enabled:
        tui.add_log(message, level)
    if original_rns_log:
        original_rns_log(message, level, _override_destination)


def get_identity_path(identity_name):
    """Get the filesystem path for an RNS identity.

    Args:
        identity_name: Name of the identity.

    Returns:
        Full path to the identity file.

    """
    config_path = os.path.expanduser("~/.reticulum")
    identity_path = os.path.join(config_path, "identities", f"{identity_name}")
    return identity_path


def load_permissions(permissions_file):
    """Load peer permissions from a file.

    Args:
        permissions_file: Path to the permissions file.

    """
    global peer_permissions, whitelist_enabled

    if not os.path.exists(permissions_file):
        RNS.log(f"Permissions file not found: {permissions_file}", RNS.LOG_WARNING)
        return

    try:
        with open(permissions_file) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                if not line or line.startswith("#"):
                    continue

                parts = line.split()
                if len(parts) < 2:
                    RNS.log(
                        f"Invalid permissions line {line_num}: {line}", RNS.LOG_WARNING,
                    )
                    continue

                identity_hash = parts[0]
                perms = parts[1].split(",")

                valid_perms = []
                for perm in perms:
                    perm = perm.strip().lower()
                    if perm in ["read", "write", "delete"]:
                        valid_perms.append(perm)
                    else:
                        RNS.log(
                            f"Invalid permission '{perm}' on line {line_num}",
                            RNS.LOG_WARNING,
                        )

                with permissions_lock:
                    peer_permissions[identity_hash] = valid_perms

                if identity_hash == "*":
                    RNS.log(f"Wildcard permissions set: {valid_perms}", RNS.LOG_INFO)
                else:
                    RNS.log(
                        f"Loaded permissions for {identity_hash}: {valid_perms}",
                        RNS.LOG_VERBOSE,
                    )

        with permissions_lock:
            if peer_permissions:
                whitelist_enabled = True
                RNS.log(
                    f"Permissions loaded for {len(peer_permissions)} identities",
                    RNS.LOG_INFO,
                )

    except Exception as e:
        RNS.log(f"Error loading permissions file: {e}", RNS.LOG_ERROR)


def add_permission_from_args(identity_hash, perms_str):
    """Add permissions for an identity from command-line arguments.

    Args:
        identity_hash: Identity hash string.
        perms_str: Comma-separated permissions string (e.g., "read,write,delete").

    """
    global peer_permissions, whitelist_enabled

    perms = [p.strip().lower() for p in perms_str.split(",")]
    valid_perms = []

    for perm in perms:
        if perm in ["read", "write", "delete"]:
            valid_perms.append(perm)
        else:
            RNS.log(f"Invalid permission: {perm}", RNS.LOG_WARNING)

    if valid_perms:
        with permissions_lock:
            peer_permissions[identity_hash] = valid_perms
            whitelist_enabled = True

        if identity_hash == "*":
            RNS.log(f"Wildcard permissions set: {valid_perms}", RNS.LOG_INFO)
        else:
            RNS.log(f"Permissions for {identity_hash}: {valid_perms}", RNS.LOG_INFO)


def check_permission(identity_hash, permission):
    """Check if an identity has a specific permission.

    Args:
        identity_hash: Identity hash bytes or string.
        permission: Permission to check ("read", "write", or "delete").

    Returns:
        True if permission is granted, False otherwise.

    """
    if not whitelist_enabled:
        return True

    with permissions_lock:
        hash_str = RNS.hexrep(identity_hash, delimit=False)

        if hash_str in peer_permissions:
            return permission in peer_permissions[hash_str]

        if "*" in peer_permissions:
            return permission in peer_permissions["*"]

        return False


def can_connect(identity_hash):
    """Check if an identity is allowed to connect.

    Args:
        identity_hash: Identity hash bytes or string.

    Returns:
        True if connection is allowed, False otherwise.

    """
    if not whitelist_enabled:
        return True

    with permissions_lock:
        hash_str = RNS.hexrep(identity_hash, delimit=False)
        return hash_str in peer_permissions or "*" in peer_permissions


def get_peer_permissions(identity_hash):
    """Get all permissions for an identity.

    Args:
        identity_hash: Identity hash bytes or string.

    Returns:
        List of permission strings, or empty list if none found.

    """
    with permissions_lock:
        hash_str = RNS.hexrep(identity_hash, delimit=False)

        if hash_str in peer_permissions:
            return peer_permissions[hash_str]

        if "*" in peer_permissions:
            return peer_permissions["*"]

        return []


def load_or_create_identity(identity_name):
    """Load an existing RNS identity or create a new one.

    Args:
        identity_name: Name of the identity.

    Returns:
        RNS.Identity instance.

    """
    identity_path = get_identity_path(identity_name)
    identity_dir = os.path.dirname(identity_path)

    if not os.path.exists(identity_dir):
        os.makedirs(identity_dir)

    if os.path.exists(identity_path):
        identity = RNS.Identity.from_file(identity_path)
        RNS.log(f"Loaded identity {identity_name} from {identity_path}", RNS.LOG_INFO)
    else:
        identity = RNS.Identity()
        identity.to_file(identity_path)
        RNS.log(
            f"Created new identity {identity_name} at {identity_path}", RNS.LOG_INFO,
        )

    return identity


def hash_file(filepath):
    """Calculate SHA256 hash of a file.

    Args:
        filepath: Path to the file.

    Returns:
        Hex digest of the file hash, or None on error.

    """
    hasher = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        RNS.log(f"Error hashing file {filepath}: {e}", RNS.LOG_ERROR)
        return None


def hash_blocks(filepath):
    """Calculate SHA256 hash for each block of a file.

    Args:
        filepath: Path to the file.

    Returns:
        List of dictionaries with block number, hash, and size.

    """
    blocks = []
    try:
        with open(filepath, "rb") as f:
            block_num = 0
            while block := f.read(BLOCK_SIZE):
                block_hash = hashlib.sha256(block).hexdigest()
                blocks.append(
                    {
                        "num": block_num,
                        "hash": block_hash,
                        "size": len(block),
                    },
                )
                block_num += 1
        return blocks
    except Exception as e:
        RNS.log(f"Error hashing blocks for {filepath}: {e}", RNS.LOG_ERROR)
        return []


def scan_directory(directory):
    """Scan directory and return file information with hashes.

    Args:
        directory: Directory path to scan.

    Returns:
        Dictionary mapping relative file paths to file info (hash, size, mtime).

    """
    file_info = {}
    try:
        for root, dirs, files in os.walk(directory):
            for filename in files:
                if filename == ".rns-filesync.db":
                    continue

                filepath = os.path.join(root, filename)
                relative_path = os.path.relpath(filepath, directory)

                try:
                    stat = os.stat(filepath)
                    file_hash = hash_file(filepath)

                    if file_hash:
                        file_info[relative_path] = {
                            "hash": file_hash,
                            "size": stat.st_size,
                            "mtime": stat.st_mtime,
                        }
                except Exception as e:
                    RNS.log(f"Error scanning {relative_path}: {e}", RNS.LOG_DEBUG)
    except Exception as e:
        RNS.log(f"Error scanning directory {directory}: {e}", RNS.LOG_ERROR)

    return file_info


def save_hash_db(directory):
    """Save file hash database to disk.

    Args:
        directory: Directory where the database file should be saved.

    """
    db_path = os.path.join(directory, ".rns-filesync.db")
    try:
        with file_hashes_lock, open(db_path, "w") as f:
            json.dump(file_hashes, f, indent=2)
    except Exception as e:
        RNS.log(f"Error saving hash database: {e}", RNS.LOG_ERROR)


def load_hash_db(directory):
    """Load file hash database from disk.

    Args:
        directory: Directory containing the database file.

    """
    global file_hashes
    db_path = os.path.join(directory, ".rns-filesync.db")

    if os.path.exists(db_path):
        try:
            with open(db_path) as f:
                file_hashes = json.load(f)
            RNS.log(
                f"Loaded hash database with {len(file_hashes)} entries", RNS.LOG_INFO,
            )
        except Exception as e:
            RNS.log(f"Error loading hash database: {e}", RNS.LOG_WARNING)
            file_hashes = {}
    else:
        file_hashes = {}


def peer_connected(link):
    """Handle peer connection event.

    Args:
        link: RNS Link object for the connected peer.

    """
    try:
        remote_identity = link.get_remote_identity()
        identity_hash = None

        if remote_identity:
            identity_hash = remote_identity.hash
            RNS.log(
                f"Peer attempting to connect: {RNS.prettyhexrep(identity_hash)}",
                RNS.LOG_VERBOSE,
            )
        else:
            RNS.log(
                f"Peer attempting to connect (destination): {RNS.prettyhexrep(link.destination.hash)}",
                RNS.LOG_VERBOSE,
            )
    except Exception as e:
        RNS.log(
            f"Peer connection attempt (could not get identity: {e})", RNS.LOG_WARNING,
        )
        identity_hash = None

    if whitelist_enabled and identity_hash:
        if not can_connect(identity_hash):
            RNS.log(
                f"Connection rejected: {RNS.prettyhexrep(identity_hash)} not in whitelist",
                RNS.LOG_WARNING,
            )
            link.teardown()
            return

        perms = get_peer_permissions(identity_hash)
        RNS.log(
            f"Peer connected: {RNS.prettyhexrep(identity_hash)} with permissions: {perms}",
            RNS.LOG_INFO,
        )
    elif identity_hash:
        RNS.log(f"Peer connected: {RNS.prettyhexrep(identity_hash)}", RNS.LOG_INFO)
    else:
        RNS.log("Peer connected", RNS.LOG_INFO)

    with connected_peers_lock:
        if link not in connected_peers:
            connected_peers.append(link)

        if tui:
            tui.update_status(peers=len(connected_peers))
    
    if identity_hash:
        dest_hash = link.destination.hash
        with desired_peers_lock:
            desired_peers.add((RNS.hexrep(dest_hash, delimit=False), RNS.hexrep(identity_hash, delimit=False)))

    link.set_link_closed_callback(peer_disconnected)
    link.set_packet_callback(packet_received)
    link.set_resource_strategy(RNS.Link.ACCEPT_APP)
    link.set_resource_callback(resource_callback)
    link.set_resource_started_callback(resource_started)
    link.set_resource_concluded_callback(resource_concluded)
    link.upload_buffers = {}

    if (
        identity_hash and check_permission(identity_hash, "read")
    ) or not whitelist_enabled:
        send_file_list_to_peer(link)
        time.sleep(0.1)
        request_file_list_from_peer(link, browser_mode=False)
    else:
        RNS.log("Peer does not have read permission, skipping file list", RNS.LOG_INFO)


def peer_disconnected(link):
    """Handle peer disconnection event.

    Args:
        link: RNS Link object for the disconnected peer.

    """
    with connected_peers_lock:
        if link in connected_peers:
            connected_peers.remove(link)

        if tui:
            tui.update_status(peers=len(connected_peers))

    try:
        remote_identity = link.get_remote_identity()
        dest_hash = link.destination.hash
        
        if remote_identity:
            identity_hash = remote_identity.hash
            RNS.log(
                f"Peer disconnected: {RNS.prettyhexrep(identity_hash)} (will attempt reconnect)",
                RNS.LOG_INFO,
            )
            
            with desired_peers_lock:
                peer_tuple = (RNS.hexrep(dest_hash, delimit=False), RNS.hexrep(identity_hash, delimit=False))
                if peer_tuple in desired_peers:
                    RNS.log(f"Peer {RNS.prettyhexrep(identity_hash)} is in desired peers list", RNS.LOG_DEBUG)
        else:
            RNS.log(
                f"Peer disconnected: {RNS.prettyhexrep(dest_hash)}",
                RNS.LOG_INFO,
            )
    except Exception as e:
        RNS.log(f"Peer disconnected: {e}", RNS.LOG_DEBUG)


def send_file_list_to_peer(link, browser_mode=False):
    """Send file list to a connected peer.

    Args:
        link: RNS Link object for the peer.
        browser_mode: Whether this is for browser viewing (default: False).

    """
    try:
        if sync_directory:
            current_files = scan_directory(sync_directory)
            with file_hashes_lock:
                file_list = {}
                for filepath, file_info in current_files.items():
                    file_list[filepath] = {
                        "hash": file_info["hash"],
                        "size": file_info["size"],
                        "mtime": file_info["mtime"],
                    }
        else:
            with file_hashes_lock:
                file_list = {path: info for path, info in file_hashes.items()}

        data = umsgpack.packb(
            {
                "type": "file_list",
                "files": file_list,
                "browser": browser_mode,
            },
        )

        packet = RNS.Packet(link, data)
        packet.send()
        RNS.log(f"Sent file list ({len(file_list)} files, browser={browser_mode}) to peer", RNS.LOG_INFO)

    except Exception as e:
        RNS.log(f"Error sending file list: {e}", RNS.LOG_ERROR)


def request_file_list_from_peer(link, browser_mode=False):
    """Request file list from a connected peer.

    Args:
        link: RNS Link object for the peer.
        browser_mode: Whether the request is for browsing UI.

    """
    try:
        data = umsgpack.packb(
            {
                "type": "file_list_request",
                "browser": browser_mode,
            },
        )

        packet = RNS.Packet(link, data)
        packet.send()
        if browser_mode:
            RNS.log("Requested file list from peer for browsing", RNS.LOG_VERBOSE)
        else:
            RNS.log("Requested file list from peer", RNS.LOG_VERBOSE)

    except Exception as e:
        RNS.log(f"Error requesting file list: {e}", RNS.LOG_ERROR)


def download_file_from_peer(link, filepath):
    """Initiate file download from a peer.

    Args:
        link: RNS Link object for the peer.
        filepath: Relative path of the file to download.

    """
    RNS.log(f"Downloading file: {filepath}", RNS.LOG_INFO)
    request_file(link, filepath)


def packet_received(message, packet):
    """Handle incoming RNS packet and route to appropriate handler.

    Args:
        message: Packet message data.
        packet: RNS Packet object.

    """
    try:
        data = umsgpack.unpackb(message)
        msg_type = data.get("type")


        if msg_type == "file_list":
            handle_peer_file_list(data, packet.link)
        elif msg_type == "file_list_request":
            is_browser = data.get("browser", False)
            send_file_list_to_peer(packet.link, browser_mode=is_browser)
        elif msg_type == "file_request":
            handle_file_request(data, packet.link)
        elif msg_type == "block_hashes":
            handle_block_hashes_response(data, packet.link)
        elif msg_type == "delta_request":
            handle_delta_request(data, packet.link)
        elif msg_type == "empty_file":
            handle_empty_file(data, packet.link)
        elif msg_type == "file_update":
            handle_file_update_notification(data, packet.link)
        elif msg_type == "file_deletion":
            handle_file_deletion(data, packet.link)

    except Exception as e:
        RNS.log(f"Error processing packet: {e}", RNS.LOG_ERROR)


def handle_peer_file_list(data, link):
    """Handle file list received from a peer.

    Args:
        data: Packet data dictionary.
        link: RNS Link object for the peer.

    """
    peer_files = data.get("files", {})
    is_browser_response = data.get("browser", False)

    RNS.log(f"Received file list from peer ({len(peer_files)} files, browser={is_browser_response})", RNS.LOG_INFO)

    if is_browser_response and tui:
        remote_files = []
        for filepath, peer_info in peer_files.items():
            remote_files.append(
                {
                    "path": filepath,
                    "size": peer_info.get("size", 0),
                    "type": tui.get_file_type(filepath),
                },
            )

        with tui.remote_files_lock:
            tui.remote_files = remote_files

        return

    if not sync_directory:
        RNS.log("Sync directory not set, cannot process file list", RNS.LOG_WARNING)
        return

    current_files = scan_directory(sync_directory)
    RNS.log(f"Comparing {len(peer_files)} peer files with {len(current_files)} local files", RNS.LOG_INFO)

    if len(peer_files) == 0:
        RNS.log("Peer has no files to sync", RNS.LOG_INFO)
        return

    files_to_request = []
    files_to_sync = []

    with file_hashes_lock:
        for filepath, peer_info in peer_files.items():
            local_info = current_files.get(filepath)

            if not local_info:
                files_to_request.append(filepath)
                RNS.log(f"Requesting new file: {filepath}", RNS.LOG_INFO)
                request_file(link, filepath)
            elif local_info["hash"] != peer_info["hash"]:
                files_to_sync.append(filepath)
                RNS.log(f"File differs, requesting blocks: {filepath}", RNS.LOG_INFO)
                request_file_blocks(link, filepath)

    if files_to_request or files_to_sync:
        RNS.log(f"Sync initiated: {len(files_to_request)} new files, {len(files_to_sync)} modified files", RNS.LOG_INFO)
    else:
        RNS.log("No files need syncing (all files match)", RNS.LOG_INFO)


def request_file(link, filepath):
    """Request a file from a peer.

    Args:
        link: RNS Link object for the peer.
        filepath: Relative path of the file to request.

    """
    try:
        RNS.log(f"Requesting file from peer: {filepath}", RNS.LOG_INFO)
        data = umsgpack.packb(
            {
                "type": "file_request",
                "path": filepath,
            },
        )
        packet = RNS.Packet(link, data)
        packet.send()
    except Exception as e:
        RNS.log(f"Error requesting file: {e}", RNS.LOG_ERROR)


def request_file_blocks(link, filepath):
    """Request delta blocks for a file from a peer.

    Args:
        link: RNS Link object for the peer.
        filepath: Relative path of the file.

    """
    try:
        full_path = os.path.join(sync_directory, filepath)
        local_blocks = hash_blocks(full_path) if os.path.exists(full_path) else []

        data = umsgpack.packb(
            {
                "type": "delta_request",
                "path": filepath,
                "local_blocks": [b["hash"] for b in local_blocks],
            },
        )
        packet = RNS.Packet(link, data)
        packet.send()
    except Exception as e:
        RNS.log(f"Error requesting blocks: {e}", RNS.LOG_ERROR)


def handle_file_request(data, link):
    """Handle file request from a peer and send the file.

    Args:
        data: Packet data dictionary containing file path.
        link: RNS Link object for the peer.

    """
    filepath = data.get("path")
    if not filepath:
        return

    transfer_key = (id(link), filepath)

    with active_outgoing_transfers_lock:
        if transfer_key in active_outgoing_transfers:
            RNS.log(f"Transfer already in progress for {filepath}, ignoring duplicate request", RNS.LOG_DEBUG)
            return
        active_outgoing_transfers[transfer_key] = time.time()

    try:
        remote_identity = link.get_remote_identity()
        if remote_identity and whitelist_enabled:
            if not check_permission(remote_identity.hash, "read"):
                RNS.log(
                    f"Peer {RNS.prettyhexrep(remote_identity.hash)} does not have read permission for {filepath}",
                    RNS.LOG_WARNING,
                )
                with active_outgoing_transfers_lock:
                    active_outgoing_transfers.pop(transfer_key, None)
                return
    except Exception as e:
        RNS.log(f"Error checking permissions: {e}", RNS.LOG_DEBUG)

    full_path = os.path.join(sync_directory, filepath)

    if not os.path.exists(full_path):
        RNS.log(f"Peer requested non-existent file: {filepath}", RNS.LOG_WARNING)
        with active_outgoing_transfers_lock:
            active_outgoing_transfers.pop(transfer_key, None)
        return

    try:
        file_size = os.path.getsize(full_path)
        RNS.log(f"Sending file {filepath} ({file_size} bytes) to peer", RNS.LOG_INFO)

        with file_hashes_lock:
            file_hash = file_hashes.get(filepath, {}).get("hash")

        metadata = {
            "filepath": filepath.encode("utf-8"),
            "hash": file_hash.encode("utf-8") if file_hash else b"",
        }

        start_time = time.time()
        with transfer_stats_lock:
            transfer_stats["last_transfer_start"] = start_time
            transfer_stats["last_transfer_bytes"] = 0

        try:
            def send_concluded_callback(resource):
                with active_outgoing_transfers_lock:
                    active_outgoing_transfers.pop(transfer_key, None)
                with active_resources_lock:
                    active_resources.pop(transfer_key, None)

                if resource.status == RNS.Resource.COMPLETE:
                    RNS.log(f"File {filepath} sent successfully", RNS.LOG_INFO)
                elif resource.status == RNS.Resource.FAILED:
                    RNS.log(f"File {filepath} send failed", RNS.LOG_ERROR)
                else:
                    RNS.log(f"File {filepath} send concluded with status {resource.status}", RNS.LOG_WARNING)

            if file_size == 0:
                RNS.log(f"Sending empty file notification for {filepath} via packet", RNS.LOG_INFO)
                
                with file_hashes_lock:
                    file_hash = file_hashes.get(filepath, {}).get("hash")
                
                empty_file_data = umsgpack.packb({
                    "type": "empty_file",
                    "path": filepath,
                    "hash": file_hash,
                })
                
                packet = RNS.Packet(link, empty_file_data)
                packet.send()
                
                with active_outgoing_transfers_lock:
                    active_outgoing_transfers.pop(transfer_key, None)
                
                RNS.log(f"Empty file {filepath} notification sent", RNS.LOG_DEBUG)
                return
            else:
                file_handle = open(full_path, "rb")
                resource = RNS.Resource(
                    file_handle,
                    link,
                    metadata=metadata,
                    auto_compress=True,
                    callback=send_concluded_callback,
                )

            with active_resources_lock:
                active_resources[transfer_key] = resource

            if file_size > 0:
                def progress_callback(resource):
                    with transfer_stats_lock:
                        progress = resource.get_progress()
                        transfer_stats["last_transfer_bytes"] = int(progress * file_size)
                        if tui:
                            elapsed = time.time() - start_time
                            if elapsed > 0:
                                transfer_stats["current_speed"] = transfer_stats["last_transfer_bytes"] / elapsed
                                tui.update_status(speed=transfer_stats["current_speed"])

                resource.progress_callback(progress_callback)

            RNS.log(f"File {filepath} resource created and advertised", RNS.LOG_INFO)

        except Exception as e:
            RNS.log(f"Error creating resource for {filepath}: {e}", RNS.LOG_ERROR)
            import traceback
            RNS.log(traceback.format_exc(), RNS.LOG_ERROR)
            with active_outgoing_transfers_lock:
                active_outgoing_transfers.pop(transfer_key, None)
            with active_resources_lock:
                active_resources.pop(transfer_key, None)

    except Exception as e:
        RNS.log(f"Error sending file {filepath}: {e}", RNS.LOG_ERROR)
        import traceback
        RNS.log(traceback.format_exc(), RNS.LOG_ERROR)
        with active_outgoing_transfers_lock:
            active_outgoing_transfers.pop(transfer_key, None)
        with active_resources_lock:
            active_resources.pop(transfer_key, None)


def handle_delta_request(data, link):
    """Handle delta sync request and send only changed blocks.

    Args:
        data: Packet data dictionary containing file path and local block hashes.
        link: RNS Link object for the peer.

    """
    filepath = data.get("path")
    peer_block_hashes = data.get("local_blocks", [])

    if not filepath:
        return

    transfer_key = (id(link), filepath, "delta")

    with active_outgoing_transfers_lock:
        if transfer_key in active_outgoing_transfers:
            RNS.log(f"Delta transfer already in progress for {filepath}, ignoring duplicate request", RNS.LOG_DEBUG)
            return
        active_outgoing_transfers[transfer_key] = time.time()

    try:
        remote_identity = link.get_remote_identity()
        if remote_identity and whitelist_enabled:
            if not check_permission(remote_identity.hash, "read"):
                RNS.log(
                    f"Peer {RNS.prettyhexrep(remote_identity.hash)} does not have read permission for {filepath}",
                    RNS.LOG_WARNING,
                )
                with active_outgoing_transfers_lock:
                    active_outgoing_transfers.pop(transfer_key, None)
                return
    except Exception as e:
        RNS.log(f"Error checking permissions: {e}", RNS.LOG_DEBUG)

    full_path = os.path.join(sync_directory, filepath)

    if not os.path.exists(full_path):
        RNS.log(
            f"Peer requested delta for non-existent file: {filepath}", RNS.LOG_WARNING,
        )
        with active_outgoing_transfers_lock:
            active_outgoing_transfers.pop(transfer_key, None)
        return

    try:
        local_blocks = hash_blocks(full_path)
        file_size = os.path.getsize(full_path)

        blocks_to_send = [
            block_info["num"]
            for block_info in local_blocks
            if block_info["hash"] not in peer_block_hashes
        ]

        if len(blocks_to_send) == len(local_blocks):
            RNS.log(f"No common blocks, sending full file: {filepath}", RNS.LOG_VERBOSE)
            with active_outgoing_transfers_lock:
                active_outgoing_transfers.pop(transfer_key, None)
            handle_file_request({"path": filepath}, link)
            return

        RNS.log(
            f"Sending {len(blocks_to_send)}/{len(local_blocks)} blocks for {filepath} (delta sync)",
            RNS.LOG_INFO,
        )

        import io
        delta_buffer = io.BytesIO()
        
        with open(full_path, "rb") as f:
            for block_num in blocks_to_send:
                f.seek(block_num * BLOCK_SIZE)
                block_data = f.read(BLOCK_SIZE)
                delta_buffer.write(block_data)

        delta_buffer.seek(0)
        
        with file_hashes_lock:
            file_hash = file_hashes.get(filepath, {}).get("hash")

        metadata = {
            "filepath": filepath.encode("utf-8"),
            "hash": file_hash.encode("utf-8") if file_hash else b"",
            "mode": "delta",
            "blocks": blocks_to_send
        }

        def send_concluded_callback(resource):
            with active_outgoing_transfers_lock:
                active_outgoing_transfers.pop(transfer_key, None)
            if resource.status == RNS.Resource.COMPLETE:
                RNS.log(f"Delta for {filepath} sent successfully", RNS.LOG_INFO)
            else:
                RNS.log(f"Delta for {filepath} send failed", RNS.LOG_ERROR)

        RNS.Resource(
            delta_buffer,
            link,
            metadata=metadata,
            callback=send_concluded_callback
        )

    except Exception as e:
        RNS.log(f"Error sending delta for {filepath}: {e}", RNS.LOG_ERROR)
        with active_outgoing_transfers_lock:
            active_outgoing_transfers.pop(transfer_key, None)


def resource_callback(resource_advertisement):
    """Handle incoming resource advertisement and decide whether to accept it.

    Args:
        resource_advertisement: RNS ResourceAdvertisement object.

    Returns:
        True if resource should be accepted, False otherwise.

    """
    try:
        transfer_size = resource_advertisement.get_transfer_size()
        data_size = resource_advertisement.get_data_size()
        has_meta = resource_advertisement.has_metadata()

        RNS.log(
            f"Resource advertisement received: transfer_size={transfer_size}, data_size={data_size}, has_metadata={has_meta}",
            RNS.LOG_VERBOSE,
        )

        sender_identity = resource_advertisement.link.get_remote_identity()
        sender_hash = RNS.prettyhexrep(sender_identity.hash) if sender_identity else "unknown"

        if sender_identity:
            if whitelist_enabled:
                if not check_permission(sender_identity.hash, "write"):
                    RNS.log(
                        f"Rejecting resource from {sender_hash} - no write permission",
                        RNS.LOG_WARNING,
                    )
                    return False
            RNS.log(f"Accepting resource from {sender_hash} ({data_size} bytes)", RNS.LOG_INFO)
            return True

        if not whitelist_enabled:
            RNS.log(f"Accepting resource ({data_size} bytes, no whitelist)", RNS.LOG_INFO)
            return True

        RNS.log("Rejecting resource - no sender identity and whitelist enabled", RNS.LOG_WARNING)
        return False

    except Exception as e:
        RNS.log(f"Error in resource_callback: {e}", RNS.LOG_ERROR)
        import traceback
        RNS.log(traceback.format_exc(), RNS.LOG_ERROR)
        return False


def resource_started(resource):
    """Handle resource transfer started.

    Args:
        resource: RNS Resource object.

    """
    size = resource.size if hasattr(resource, "size") else 0
    total_size = resource.total_size if hasattr(resource, "total_size") else 0
    parts = len(resource.parts) if hasattr(resource, "parts") and resource.parts else 0
    has_meta = resource.has_metadata if hasattr(resource, "has_metadata") else False
    
    RNS.log(
        f"Resource transfer started: size={size}, total_size={total_size}, parts={parts}, has_metadata={has_meta}",
        RNS.LOG_INFO,
    )


def resource_concluded(resource):
    """Handle resource transfer completed (receiver side).

    Args:
        resource: RNS Resource object.

    """
    status_names = {
        0x00: "NONE/REJECTED",
        0x01: "QUEUED",
        0x02: "ADVERTISED",
        0x03: "TRANSFERRING",
        0x04: "AWAITING_PROOF",
        0x05: "ASSEMBLING",
        0x06: "COMPLETE",
        0x07: "FAILED",
        0x08: "CORRUPT",
    }
    
    try:
        status_name = status_names.get(resource.status, f"UNKNOWN({resource.status})")
        
        if resource.status != RNS.Resource.COMPLETE:
            size = resource.size if hasattr(resource, "size") else 0
            parts = len(resource.parts) if hasattr(resource, "parts") and resource.parts else 0
            
            filepath = None
            if resource.metadata:
                try:
                    if isinstance(resource.metadata, dict) and "filepath" in resource.metadata:
                        filepath = resource.metadata["filepath"].decode("utf-8") if isinstance(resource.metadata["filepath"], bytes) else resource.metadata["filepath"]
                except Exception as e:
                    RNS.log(f"Could not decode filepath from metadata: {e}", RNS.LOG_DEBUG)
            
            if filepath:
                RNS.log(
                    f"Resource transfer failed for {filepath}: status={status_name}, size={size}, parts={parts}",
                    RNS.LOG_ERROR,
                )
            else:
                RNS.log(
                    f"Resource transfer failed: status={status_name}, size={size}, parts={parts}",
                    RNS.LOG_ERROR,
                )
            return

        if not resource.metadata or "filepath" not in resource.metadata:
            RNS.log(f"Resource missing filepath metadata, type: {type(resource.metadata)}", RNS.LOG_WARNING)
            return

        filepath = resource.metadata["filepath"].decode("utf-8") if isinstance(resource.metadata["filepath"], bytes) else resource.metadata["filepath"]
        expected_hash_raw = resource.metadata.get("hash", b"")
        expected_hash = expected_hash_raw.decode("utf-8") if isinstance(expected_hash_raw, bytes) else expected_hash_raw

        RNS.log(f"Processing received resource for {filepath}", RNS.LOG_VERBOSE)
    except Exception as e:
        RNS.log(f"Error in resource_concluded metadata extraction: {e}", RNS.LOG_ERROR)
        import traceback
        RNS.log(traceback.format_exc(), RNS.LOG_ERROR)
        return

    try:
        remote_identity = resource.link.get_remote_identity()
        if remote_identity and whitelist_enabled:
            if not check_permission(remote_identity.hash, "write"):
                RNS.log(
                    f"Peer {RNS.prettyhexrep(remote_identity.hash)} does not have write permission for {filepath}",
                    RNS.LOG_WARNING,
                )
                return
    except Exception as e:
        RNS.log(f"Error checking permissions: {e}", RNS.LOG_DEBUG)

    full_path = os.path.join(sync_directory, filepath)
    dir_path = os.path.dirname(full_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    try:
        if resource.metadata.get("mode") == "delta":
            block_list = resource.metadata.get("blocks", [])
            RNS.log(f"Applying {len(block_list)} delta blocks to {filepath}", RNS.LOG_INFO)
            
            with open(full_path, "r+b") as f:
                # Reticulum Resource data is already a file-like object
                resource.data.seek(0)
                for block_num in block_list:
                    block_data = resource.data.read(BLOCK_SIZE)
                    if not block_data:
                        break
                    f.seek(block_num * BLOCK_SIZE)
                    f.write(block_data)
        else:
            shutil.move(resource.data.name, full_path)

        file_size = os.path.getsize(full_path)
        if resource.metadata.get("mode") == "delta":
            RNS.log(f"Delta applied to {filepath}", RNS.LOG_INFO)
        else:
            RNS.log(f"Received file {filepath} ({file_size} bytes)", RNS.LOG_INFO)

        actual_hash = hash_file(full_path)

        if expected_hash and actual_hash != expected_hash:
            RNS.log(
                f"Hash mismatch for {filepath}! Expected {expected_hash}, got {actual_hash}",
                RNS.LOG_ERROR,
            )
            os.remove(full_path)
            return

        if actual_hash:
            RNS.log(f"File {filepath} verified successfully", RNS.LOG_VERBOSE)

            with file_hashes_lock:
                file_hashes[filepath] = {
                    "hash": actual_hash,
                    "size": file_size,
                    "mtime": time.time(),
                }
            save_hash_db(sync_directory)

            broadcast_file_update(filepath, exclude_link=resource.link)

    except Exception as e:
        RNS.log(f"Error saving received file {filepath}: {e}", RNS.LOG_ERROR)


def handle_empty_file(data, link):
    """Handle empty file notification from a peer.

    Args:
        data: Packet data dictionary containing file path and hash.
        link: RNS Link object for the peer.

    """
    filepath = data.get("path")
    expected_hash = data.get("hash")

    if not filepath:
        return

    try:
        remote_identity = link.get_remote_identity()
        if remote_identity and whitelist_enabled:
            if not check_permission(remote_identity.hash, "write"):
                RNS.log(
                    f"Peer {RNS.prettyhexrep(remote_identity.hash)} does not have write permission for {filepath}",
                    RNS.LOG_WARNING,
                )
                return
    except Exception as e:
        RNS.log(f"Error checking permissions: {e}", RNS.LOG_DEBUG)

    full_path = os.path.join(sync_directory, filepath)
    dir_path = os.path.dirname(full_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    try:
        with open(full_path, "wb") as f:
            pass

        RNS.log(f"Created empty file {filepath}", RNS.LOG_INFO)

        actual_hash = hash_file(full_path)

        if expected_hash and actual_hash != expected_hash:
            RNS.log(
                f"Hash mismatch for empty file {filepath}! Expected {expected_hash}, got {actual_hash}",
                RNS.LOG_WARNING,
            )

        with file_hashes_lock:
            file_hashes[filepath] = {
                "hash": actual_hash,
                "size": 0,
                "mtime": time.time(),
            }
        save_hash_db(sync_directory)

        broadcast_file_update(filepath, exclude_link=link)

    except Exception as e:
        RNS.log(f"Error creating empty file {filepath}: {e}", RNS.LOG_ERROR)


def handle_block_hashes_response(data, link):
    """Handle block hashes response (currently unused).

    Args:
        data: Packet data dictionary.
        link: RNS Link object for the peer.

    """


def handle_file_update_notification(data, link):
    """Handle file update notification from a peer.

    Args:
        data: Packet data dictionary containing file path and info.
        link: RNS Link object for the peer.

    """
    filepath = data.get("path")
    peer_info = data.get("info")

    if not peer_info:
        RNS.log(f"Received file update without peer_info for {filepath}", RNS.LOG_WARNING)
        return

    with file_hashes_lock:
        local_info = file_hashes.get(filepath)

    if not local_info:
        RNS.log(f"New file available from peer: {filepath}", RNS.LOG_INFO)
        request_file(link, filepath)
    elif local_info["hash"] != peer_info["hash"]:
        RNS.log(f"File update available (hash differs): {filepath}", RNS.LOG_INFO)
        full_path = os.path.join(sync_directory, filepath)
        if os.path.exists(full_path):
            request_file_blocks(link, filepath)
        else:
            request_file(link, filepath)
    else:
        RNS.log(f"File {filepath} is already up to date", RNS.LOG_DEBUG)


def broadcast_file_update(filepath, exclude_link=None):
    """Broadcast file update notification to all connected peers.

    Args:
        filepath: Relative path of the updated file.
        exclude_link: Optional link to exclude from broadcast.

    """
    with connected_peers_lock:
        peers = list(connected_peers)

    if not peers:
        RNS.log(f"No peers to broadcast update for {filepath}", RNS.LOG_DEBUG)
        return

    for link in peers:
        if link != exclude_link and link.status == RNS.Link.ACTIVE:
            try:
                with file_hashes_lock:
                    file_info = file_hashes.get(filepath)

                if file_info:
                    data = umsgpack.packb(
                        {
                            "type": "file_update",
                            "path": filepath,
                            "info": file_info,
                        },
                    )

                    packet = RNS.Packet(link, data)
                    packet.send()
                    RNS.log(f"Sent file update notification for {filepath} to peer", RNS.LOG_VERBOSE)
                else:
                    RNS.log(f"No file info found for {filepath} in broadcast", RNS.LOG_WARNING)
            except Exception as e:
                RNS.log(f"Error broadcasting update: {e}", RNS.LOG_DEBUG)


def broadcast_file_deletion(filepath, exclude_link=None):
    """Broadcast file deletion notification to all connected peers.

    Args:
        filepath: Relative path of the deleted file.
        exclude_link: Optional link to exclude from broadcast.

    """
    with connected_peers_lock:
        peers = list(connected_peers)

    for link in peers:
        if link != exclude_link and link.status == RNS.Link.ACTIVE:
            try:
                data = umsgpack.packb(
                    {
                        "type": "file_deletion",
                        "path": filepath,
                    },
                )

                packet = RNS.Packet(link, data)
                packet.send()
                RNS.log(f"Notified peer of deletion: {filepath}", RNS.LOG_VERBOSE)
            except Exception as e:
                RNS.log(f"Error broadcasting deletion: {e}", RNS.LOG_DEBUG)


def handle_file_deletion(data, link):
    """Handle file deletion notification from a peer.

    Args:
        data: Packet data dictionary containing file path.
        link: RNS Link object for the peer.

    """
    filepath = data.get("path")

    if not filepath:
        return

    try:
        remote_identity = link.get_remote_identity()
        if remote_identity and whitelist_enabled:
            if not check_permission(remote_identity.hash, "delete"):
                RNS.log(
                    f"Peer {RNS.prettyhexrep(remote_identity.hash)} does not have delete permission for {filepath}",
                    RNS.LOG_WARNING,
                )
                return
    except Exception as e:
        RNS.log(f"Error checking permissions: {e}", RNS.LOG_DEBUG)

    full_path = os.path.join(sync_directory, filepath)

    if os.path.exists(full_path):
        try:
            os.remove(full_path)
            RNS.log(f"Deleted file from peer deletion: {filepath}", RNS.LOG_INFO)

            with file_hashes_lock:
                file_hashes.pop(filepath, None)

            save_hash_db(sync_directory)
            broadcast_file_deletion(filepath, exclude_link=link)
        except Exception as e:
            RNS.log(f"Error deleting file {filepath}: {e}", RNS.LOG_ERROR)
    else:
        with file_hashes_lock:
            file_hashes.pop(filepath, None)
        save_hash_db(sync_directory)


def peer_reconnection_loop():
    """Monitor desired peers and reconnect if disconnected."""
    global peer_reconnect_active
    
    reconnect_interval = 10
    
    while peer_reconnect_active:
        try:
            time.sleep(reconnect_interval)
            
            with desired_peers_lock:
                peers_to_check = list(desired_peers)
            
            if not peers_to_check:
                continue
            
            with connected_peers_lock:
                connected_hashes = set()
                for link in connected_peers:
                    if link.status == RNS.Link.ACTIVE:
                        try:
                            remote_id = link.get_remote_identity()
                            if remote_id:
                                connected_hashes.add(RNS.hexrep(remote_id.hash, delimit=False))
                        except Exception:
                            pass
            
            for dest_hash, identity_hash in peers_to_check:
                if identity_hash not in connected_hashes:
                    RNS.log(f"Peer {identity_hash[:16]}... disconnected, attempting reconnect", RNS.LOG_INFO)
                    threading.Thread(
                        target=connect_to_peer,
                        args=(dest_hash,),
                        daemon=True,
                    ).start()
                    time.sleep(1)
        
        except Exception as e:
            RNS.log(f"Error in peer reconnection loop: {e}", RNS.LOG_ERROR)
            time.sleep(5)


def file_monitor():
    """Monitor directory for file changes and sync with peers."""
    global file_monitor_active

    while file_monitor_active:
        try:
            current_files = scan_directory(sync_directory)

            with file_hashes_lock:
                old_files = set(file_hashes.keys())
                new_files = set(current_files.keys())

                added = new_files - old_files
                removed = old_files - new_files
                potentially_modified = old_files & new_files

                for filepath in added:
                    RNS.log(f"New file detected: {filepath}", RNS.LOG_INFO)
                    file_hashes[filepath] = current_files[filepath]

                for filepath in removed:
                    RNS.log(f"File removed: {filepath}", RNS.LOG_INFO)
                    del file_hashes[filepath]

                modified = []
                for filepath in potentially_modified:
                    if file_hashes[filepath]["hash"] != current_files[filepath]["hash"]:
                        RNS.log(f"File modified: {filepath}", RNS.LOG_INFO)
                        file_hashes[filepath] = current_files[filepath]
                        modified.append(filepath)

            if added or removed or modified:
                save_hash_db(sync_directory)

                if tui:
                    tui.update_status(files=len(file_hashes))
                    tui.update_file_list(sync_directory)

                for filepath in added:
                    broadcast_file_update(filepath)

                for filepath in removed:
                    broadcast_file_deletion(filepath)

                for filepath in modified:
                    broadcast_file_update(filepath)

            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            RNS.log(f"File monitor error: {e}", RNS.LOG_DEBUG)
            time.sleep(1)


def connect_to_peer(peer_hash_hex):
    """Connect to a peer by identity hash.

    Args:
        peer_hash_hex: Hex string representation of peer identity hash.

    Returns:
        RNS Link object if successful, None otherwise.

    """
    try:
        peer_hash = bytes.fromhex(peer_hash_hex)
    except Exception as e:
        RNS.log(f"Invalid peer hash: {e}", RNS.LOG_ERROR)
        return None

    if not RNS.Transport.has_path(peer_hash):
        RNS.log(f"Finding path to peer {RNS.prettyhexrep(peer_hash)}...", RNS.LOG_INFO)
        RNS.Transport.request_path(peer_hash)

        timeout = 30
        start_time = time.time()
        while not RNS.Transport.has_path(peer_hash):
            time.sleep(0.5)
            if time.time() - start_time > timeout:
                RNS.log("Path request timed out", RNS.LOG_ERROR)
                return None

        RNS.log("Path found", RNS.LOG_INFO)

    peer_identity = RNS.Identity.recall(peer_hash)

    if not peer_identity:
        RNS.log(
            f"Could not recall identity for {RNS.prettyhexrep(peer_hash)}",
            RNS.LOG_ERROR,
        )
        return None

    destination = RNS.Destination(
        peer_identity,
        RNS.Destination.OUT,
        RNS.Destination.SINGLE,
        APP_NAME,
        "filesync",
    )

    link = RNS.Link(
        destination,
        established_callback=peer_connected,
        closed_callback=peer_disconnected,
    )
    link.upload_buffers = {}

    timeout = 10
    start_time = time.time()
    while link.status not in (RNS.Link.ACTIVE, RNS.Link.CLOSED):
        time.sleep(0.1)
        if time.time() - start_time > timeout:
            RNS.log("Link establishment timed out", RNS.LOG_ERROR)
            return None

    if link.status != RNS.Link.ACTIVE:
        RNS.log("Failed to establish link", RNS.LOG_ERROR)
        return None

    link.set_packet_callback(packet_received)
    link.set_resource_strategy(RNS.Link.ACCEPT_APP)
    link.set_resource_callback(resource_callback)
    link.set_resource_started_callback(resource_started)
    link.set_resource_concluded_callback(resource_concluded)

    with connected_peers_lock:
        if link not in connected_peers:
            connected_peers.append(link)

    time.sleep(0.2)

    try:
        remote_identity = link.get_remote_identity()
        identity_hash = remote_identity.hash if remote_identity else None
        
        if identity_hash:
            dest_hash = link.destination.hash
            with desired_peers_lock:
                desired_peers.add((RNS.hexrep(dest_hash, delimit=False), RNS.hexrep(identity_hash, delimit=False)))
                RNS.log(f"Added peer to desired peers: {RNS.prettyhexrep(identity_hash)}", RNS.LOG_DEBUG)

        if (
            (identity_hash and check_permission(identity_hash, "read"))
            or not whitelist_enabled
        ):
            send_file_list_to_peer(link)
            time.sleep(0.1)
            request_file_list_from_peer(link, browser_mode=False)
        else:
            RNS.log("Peer does not have read permission, skipping file list", RNS.LOG_INFO)
    except Exception as e:
        RNS.log(f"Error setting up file sync on connect: {e}", RNS.LOG_DEBUG)
        send_file_list_to_peer(link)
        time.sleep(0.1)
        request_file_list_from_peer(link, browser_mode=False)

    RNS.log(f"Connected to peer {RNS.prettyhexrep(peer_hash)}", RNS.LOG_INFO)
    return link


def announce_loop(destination, interval):
    """Continuously announce destination at specified intervals.

    Args:
        destination: RNS Destination to announce.
        interval: Announcement interval in seconds.

    """
    last_announce = 0
    destination.announce()
    RNS.log(f"Announced: {RNS.prettyhexrep(destination.hash)}", RNS.LOG_INFO)

    while True:
        time.sleep(10)
        if time.time() - last_announce > interval:
            destination.announce()
            last_announce = time.time()
            RNS.log(
                f"Auto-announced: {RNS.prettyhexrep(destination.hash)}", RNS.LOG_DEBUG,
            )


def start_peer(
    configpath,
    directory,
    identity_name="rns_filesync",
    peers=None,
    monitor=True,
    announce_interval=300,
    use_tui=True,
):
    """Start the RNS FileSync peer.

    Args:
        configpath: Path to Reticulum config directory.
        directory: Directory to synchronize.
        identity_name: Name of the RNS identity to use.
        peers: List of peer identity hashes to connect to.
        monitor: Whether to enable file monitoring.
        announce_interval: Announcement interval in seconds.
        use_tui: Whether to enable terminal user interface.

    """
    global \
        peer_identity, \
        peer_destination, \
        sync_directory, \
        file_monitor_active, \
        peer_reconnect_active, \
        tui, \
        original_rns_log

    directory = os.path.abspath(os.path.expanduser(directory))

    if not os.path.exists(directory):
        os.makedirs(directory)
        if use_tui:
            print(f"Created sync directory: {directory}")
        else:
            RNS.log(f"Created sync directory: {directory}", RNS.LOG_INFO)

    sync_directory = directory

    if use_tui:
        tui = SimpleTUI()
        RNS.loglevel = RNS.LOG_VERBOSE
        original_rns_log = RNS.log
        RNS.log = rns_log_hook
        tui.add_log("Initializing RNS FileSync...", RNS.LOG_NOTICE)

    RNS.Reticulum(configpath)

    peer_identity = load_or_create_identity(identity_name)

    peer_destination = RNS.Destination(
        peer_identity,
        RNS.Destination.IN,
        RNS.Destination.SINGLE,
        APP_NAME,
        "filesync",
    )

    peer_destination.set_link_established_callback(peer_connected)

    RNS.log("RNS FileSync peer started", RNS.LOG_NOTICE)
    RNS.log(f"Sync directory: {directory}", RNS.LOG_NOTICE)
    RNS.log(f"Identity: {RNS.prettyhexrep(peer_identity.hash)}", RNS.LOG_NOTICE)
    RNS.log(f"Destination: {RNS.prettyhexrep(peer_destination.hash)}", RNS.LOG_NOTICE)

    if tui:
        tui.update_status(
            directory=directory,
            identity=RNS.prettyhexrep(peer_identity.hash),
            destination=RNS.prettyhexrep(peer_destination.hash),
            permissions=whitelist_enabled,
        )

    load_hash_db(directory)

    RNS.log("Performing initial directory scan...", RNS.LOG_INFO)
    current_files = scan_directory(directory)
    with file_hashes_lock:
        file_hashes.update(current_files)
    save_hash_db(directory)
    RNS.log(f"Tracking {len(file_hashes)} files", RNS.LOG_INFO)

    if tui:
        tui.update_status(files=len(file_hashes))
        tui.update_file_list(directory)

    if monitor:
        file_monitor_active = True
        monitor_thread = threading.Thread(target=file_monitor, daemon=True)
        monitor_thread.start()
        RNS.log("File monitoring enabled", RNS.LOG_INFO)

    announce_thread = threading.Thread(
        target=announce_loop, args=(peer_destination, announce_interval), daemon=True,
    )
    announce_thread.start()
    
    global peer_reconnect_active
    peer_reconnect_active = True
    reconnect_thread = threading.Thread(target=peer_reconnection_loop, daemon=True)
    reconnect_thread.start()
    RNS.log("Peer reconnection enabled (checking every 10 seconds)", RNS.LOG_INFO)

    if peers:
        for peer_hash in peers:
            RNS.log(f"Connecting to peer: {peer_hash}", RNS.LOG_INFO)
            connect_thread = threading.Thread(
                target=connect_to_peer, args=(peer_hash,), daemon=True,
            )
            connect_thread.start()
            time.sleep(0.5)

    RNS.log(
        "Commands: 'peers' - show peers, 'status' - show stats, 'connect <hash>' - connect, 'disconnect <#>' - disconnect peer, 'quit' - exit",
        RNS.LOG_INFO,
    )

    if tui:
        tui.start_refresh_timer()
        time.sleep(0.5)
        tui.refresh_display(full_clear=True)

    while True:
        try:
            if tui:
                tui.draw_input_area(restore_input=False)
                entered = input("").strip()
            else:
                entered = input().strip()

            if tui:
                tui.clear_current_input()

            if entered.lower() in ["quit", "exit", "q"]:
                RNS.log("Shutting down...", RNS.LOG_INFO)
                save_hash_db(sync_directory)
                if tui:
                    tui.stop()
                sys.exit(0)

            elif entered.lower() == "status":
                with file_hashes_lock:
                    RNS.log(f"Tracking {len(file_hashes)} files", RNS.LOG_INFO)
                with connected_peers_lock:
                    RNS.log(f"Connected peers: {len(connected_peers)}", RNS.LOG_INFO)
                    for link in connected_peers:
                        if link.status == RNS.Link.ACTIVE:
                            try:
                                remote_id = link.get_remote_identity()
                                if remote_id:
                                    RNS.log(
                                        f"  - {RNS.prettyhexrep(remote_id.hash)}",
                                        RNS.LOG_INFO,
                                    )
                                else:
                                    RNS.log(
                                        f"  - {RNS.prettyhexrep(link.destination.hash)}",
                                        RNS.LOG_INFO,
                                    )
                            except Exception:
                                RNS.log(
                                    f"  - {RNS.prettyhexrep(link.destination.hash)}",
                                    RNS.LOG_INFO,
                                )

            elif entered.lower() == "peers":
                with connected_peers_lock:
                    if not connected_peers:
                        RNS.log("No connected peers", RNS.LOG_INFO)
                    else:
                        RNS.log(
                            f"Connected to {len(connected_peers)} peer(s):",
                            RNS.LOG_INFO,
                        )
                        for link in connected_peers:
                            if link.status == RNS.Link.ACTIVE:
                                try:
                                    remote_id = link.get_remote_identity()
                                    if remote_id:
                                        RNS.log(
                                            f"  {RNS.prettyhexrep(remote_id.hash)}",
                                            RNS.LOG_INFO,
                                        )
                                    else:
                                        RNS.log(
                                            f"  {RNS.prettyhexrep(link.destination.hash)}",
                                            RNS.LOG_INFO,
                                        )
                                except Exception:
                                    RNS.log(
                                        f"  {RNS.prettyhexrep(link.destination.hash)}",
                                        RNS.LOG_INFO,
                                    )

            elif entered.lower().startswith("connect "):
                peer_hash = entered[8:].strip()
                RNS.log(f"Attempting to connect to {peer_hash}...", RNS.LOG_INFO)
                connect_thread = threading.Thread(
                    target=connect_to_peer, args=(peer_hash,), daemon=True,
                )
                connect_thread.start()

            elif entered.lower() == "announce":
                peer_destination.announce()
                RNS.log(
                    f"Announced: {RNS.prettyhexrep(peer_destination.hash)}",
                    RNS.LOG_INFO,
                )

            elif entered.lower() == "logs":
                if tui:
                    tui.set_view_mode("logs")
                    RNS.log(
                        "Switched to logs view (type 'files' to return)", RNS.LOG_INFO,
                    )
                else:
                    RNS.log("TUI is not enabled", RNS.LOG_INFO)

            elif entered.lower() == "files":
                if tui:
                    tui.set_view_mode("files")
                    tui.browser_peer = None
                    RNS.log("Switched to files view", RNS.LOG_INFO)
                else:
                    RNS.log("TUI is not enabled", RNS.LOG_INFO)

            elif entered.lower().startswith("browse "):
                peer_idx_str = entered[7:].strip()
                try:
                    with connected_peers_lock:
                        if not connected_peers:
                            RNS.log("No connected peers to browse", RNS.LOG_WARNING)
                        else:
                            try:
                                peer_idx = int(peer_idx_str)
                                if 0 <= peer_idx < len(connected_peers):
                                    link = connected_peers[peer_idx]
                                    if link.status == RNS.Link.ACTIVE:
                                        remote_id = link.get_remote_identity()
                                        peer_hash = (
                                            remote_id.hash
                                            if remote_id
                                            else link.destination.hash
                                        )

                                        if tui:
                                            tui.set_view_mode("browser")
                                            tui.browser_peer = peer_hash
                                            with tui.remote_files_lock:
                                                tui.remote_files = []

                                        request_file_list_from_peer(link, browser_mode=True)
                                        RNS.log(
                                            f"Browsing peer {peer_idx}: {RNS.prettyhexrep(peer_hash)}",
                                            RNS.LOG_INFO,
                                        )
                                    else:
                                        RNS.log(
                                            f"Peer {peer_idx} is not active",
                                            RNS.LOG_WARNING,
                                        )
                                else:
                                    RNS.log(
                                        f"Invalid peer index. Use 'peers' to see available peers (0-{len(connected_peers) - 1})",
                                        RNS.LOG_WARNING,
                                    )
                            except ValueError:
                                peer_hash = bytes.fromhex(peer_idx_str)
                                for link in connected_peers:
                                    remote_id = link.get_remote_identity()
                                    link_hash = (
                                        remote_id.hash
                                        if remote_id
                                        else link.destination.hash
                                    )
                                    if link_hash == peer_hash:
                                        if tui:
                                            tui.set_view_mode("browser")
                                            tui.browser_peer = peer_hash
                                            with tui.remote_files_lock:
                                                tui.remote_files = []
                                        request_file_list_from_peer(link, browser_mode=True)
                                        RNS.log(
                                            f"Browsing peer: {RNS.prettyhexrep(peer_hash)}",
                                            RNS.LOG_INFO,
                                        )
                                        break
                                else:
                                    RNS.log(
                                        "Peer not found in connected peers",
                                        RNS.LOG_WARNING,
                                    )
                except Exception as e:
                    RNS.log(f"Error browsing peer: {e}", RNS.LOG_ERROR)

            elif entered.lower().startswith("download "):
                filepath = entered[9:].strip()
                if tui and tui.browser_peer:
                    with connected_peers_lock:
                        for link in connected_peers:
                            remote_id = link.get_remote_identity()
                            link_hash = (
                                remote_id.hash if remote_id else link.destination.hash
                            )
                            if link_hash == tui.browser_peer:
                                download_file_from_peer(link, filepath)
                                break
                        else:
                            RNS.log(
                                "Browsed peer is no longer connected", RNS.LOG_WARNING,
                            )
                else:
                    RNS.log(
                        "Not in browser mode. Use 'browse <peer>' first", RNS.LOG_INFO,
                    )

            elif entered.lower() == "download_all":
                if tui and tui.browser_peer:
                    with tui.remote_files_lock:
                        files_to_download = [f["path"] for f in tui.remote_files]

                    if files_to_download:
                        with connected_peers_lock:
                            for link in connected_peers:
                                remote_id = link.get_remote_identity()
                                link_hash = (
                                    remote_id.hash
                                    if remote_id
                                    else link.destination.hash
                                )
                                if link_hash == tui.browser_peer:
                                    RNS.log(
                                        f"Downloading {len(files_to_download)} files...",
                                        RNS.LOG_INFO,
                                    )
                                    for filepath in files_to_download:
                                        download_file_from_peer(link, filepath)
                                    break
                            else:
                                RNS.log(
                                    "Browsed peer is no longer connected",
                                    RNS.LOG_WARNING,
                                )
                    else:
                        RNS.log("No files to download", RNS.LOG_INFO)
                else:
                    RNS.log(
                        "Not in browser mode. Use 'browse <peer>' first", RNS.LOG_INFO,
                    )

            elif entered.lower().startswith("disconnect "):
                peer_idx_str = entered[11:].strip()
                try:
                    with connected_peers_lock:
                        if not connected_peers:
                            RNS.log("No connected peers to disconnect", RNS.LOG_WARNING)
                        else:
                            try:
                                peer_idx = int(peer_idx_str)
                                if 0 <= peer_idx < len(connected_peers):
                                    link = connected_peers[peer_idx]
                                    remote_id = link.get_remote_identity()
                                    dest_hash = link.destination.hash
                                    identity_hash = remote_id.hash if remote_id else None
                                    
                                    if identity_hash:
                                        with desired_peers_lock:
                                            peer_tuple = (RNS.hexrep(dest_hash, delimit=False), RNS.hexrep(identity_hash, delimit=False))
                                            if peer_tuple in desired_peers:
                                                desired_peers.remove(peer_tuple)
                                                RNS.log(f"Removed peer {RNS.prettyhexrep(identity_hash)} from auto-reconnect", RNS.LOG_INFO)
                                    
                                    link.teardown()
                                    RNS.log(f"Disconnected from peer {peer_idx}", RNS.LOG_INFO)
                                else:
                                    RNS.log(f"Invalid peer index. Use 'peers' to see available peers (0-{len(connected_peers) - 1})", RNS.LOG_WARNING)
                            except ValueError:
                                RNS.log("Usage: disconnect <peer_index>", RNS.LOG_INFO)
                except Exception as e:
                    RNS.log(f"Error disconnecting peer: {e}", RNS.LOG_ERROR)

            elif entered:
                RNS.log(
                    "Unknown command. Available: peers, status, connect <hash>, disconnect <index>, browse <peer>, download <file>, download_all, logs, files, announce, quit",
                    RNS.LOG_INFO,
                )

        except KeyboardInterrupt:
            RNS.log("\nShutting down...", RNS.LOG_INFO)
            save_hash_db(sync_directory)
            if tui:
                tui.stop()
            sys.exit(0)
        except Exception as e:
            RNS.log(f"Input error: {e}", RNS.LOG_ERROR)


def main():
    """Run the RNS FileSync application."""
    parser = argparse.ArgumentParser(
        description="RNS FileSync - Peer-to-Peer File Synchronization over Reticulum",
    )

    parser.add_argument(
        "--config",
        action="store",
        default=None,
        help="path to Reticulum config directory",
        type=str,
    )

    parser.add_argument(
        "-d",
        "--directory",
        action="store",
        required=True,
        help="directory to synchronize",
        type=str,
    )

    parser.add_argument(
        "-i",
        "--identity",
        action="store",
        default=None,
        help="identity name to use (default: rns_filesync)",
        type=str,
    )

    parser.add_argument(
        "-p",
        "--peer",
        action="append",
        dest="peers",
        help="peer identity hash to connect to (can be specified multiple times)",
        type=str,
    )

    parser.add_argument(
        "-n",
        "--no-monitor",
        action="store_true",
        help="disable file monitoring (only sync on connect)",
        default=False,
    )

    parser.add_argument(
        "-a",
        "--announce-interval",
        action="store",
        default=300,
        help="announce interval in seconds (default: 300)",
        type=int,
    )

    parser.add_argument(
        "--permissions-file",
        action="store",
        default=None,
        help="path to permissions file (format: identity_hash permission1,permission2)",
        type=str,
    )

    parser.add_argument(
        "--allow",
        action="append",
        dest="allowed_peers",
        help="allow specific identity hash (use with --perms)",
        type=str,
    )

    parser.add_argument(
        "--perms",
        action="store",
        default="read,write,delete",
        help="permissions for --allow (comma-separated: read,write,delete)",
        type=str,
    )

    parser.add_argument(
        "--no-tui",
        action="store_true",
        help="disable TUI mode (use plain logging)",
        default=False,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
    )

    args = parser.parse_args()

    if args.verbose == 1:
        loglevel = RNS.LOG_INFO
    elif args.verbose == 2:
        loglevel = RNS.LOG_VERBOSE
    elif args.verbose >= 3:
        loglevel = RNS.LOG_DEBUG
    else:
        loglevel = RNS.LOG_NOTICE

    RNS.loglevel = loglevel

    identity_name = args.identity if args.identity else "rns_filesync"

    if args.permissions_file:
        load_permissions(args.permissions_file)

    if args.allowed_peers:
        for peer_hash in args.allowed_peers:
            add_permission_from_args(peer_hash, args.perms)

    start_peer(
        configpath=args.config,
        directory=args.directory,
        identity_name=identity_name,
        peers=args.peers,
        monitor=not args.no_monitor,
        announce_interval=args.announce_interval,
        use_tui=not args.no_tui,
    )


if __name__ == "__main__":
    main()
