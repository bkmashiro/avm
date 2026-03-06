"""
AVM Unified Daemon - Single process, multiple mount points

Usage:
    avm-daemon start [--config CONFIG]
    avm-daemon stop
    avm-daemon status
    avm-daemon add MOUNTPOINT --agent AGENT_ID
    avm-daemon remove MOUNTPOINT
"""

import os
import sys
import json
import signal
import threading
import argparse
from pathlib import Path
from typing import Dict, Optional
from dataclasses import dataclass, field, asdict

# Lazy imports to avoid circular dependencies
FUSE = None
AVMFuse = None
AVM = None


def _lazy_imports():
    global FUSE, AVMFuse, AVM
    if FUSE is None:
        from fuse import FUSE as _FUSE
        from .fuse_mount import AVMFuse as _AVMFuse
        from .core import AVM as _AVM
        FUSE = _FUSE
        AVMFuse = _AVMFuse
        AVM = _AVM


# ═══════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════

CONFIG_DIR = Path.home() / ".local" / "share" / "avm"
DAEMON_CONFIG = CONFIG_DIR / "daemon.json"
DAEMON_PID = CONFIG_DIR / "daemon.pid"


@dataclass
class MountConfig:
    """Configuration for a single mount point"""
    mountpoint: str
    agent_id: str
    enabled: bool = True


@dataclass
class DaemonConfig:
    """Daemon configuration"""
    mounts: Dict[str, MountConfig] = field(default_factory=dict)
    
    def save(self):
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "mounts": {k: asdict(v) for k, v in self.mounts.items()}
        }
        DAEMON_CONFIG.write_text(json.dumps(data, indent=2))
    
    @classmethod
    def load(cls) -> "DaemonConfig":
        if not DAEMON_CONFIG.exists():
            return cls()
        try:
            data = json.loads(DAEMON_CONFIG.read_text())
            mounts = {
                k: MountConfig(**v) 
                for k, v in data.get("mounts", {}).items()
            }
            return cls(mounts=mounts)
        except Exception:
            return cls()


# ═══════════════════════════════════════════════════════════════
# Mount Thread
# ═══════════════════════════════════════════════════════════════

class MountProcess:
    """Child process managing a single FUSE mount"""
    
    def __init__(self, mountpoint: str, agent_id: str):
        self.mountpoint = mountpoint
        self.agent_id = agent_id
        self.pid: Optional[int] = None
    
    def start(self):
        """Fork a child process to run the FUSE mount"""
        pid = os.fork()
        if pid == 0:
            # Child process
            self._run_fuse()
            os._exit(0)
        else:
            # Parent process
            self.pid = pid
    
    def _run_fuse(self):
        """Run FUSE in child process"""
        _lazy_imports()
        try:
            # Create agent-scoped AVM
            agent_avm = AVM(agent_id=self.agent_id)
            
            # Ensure mountpoint exists
            Path(self.mountpoint).mkdir(parents=True, exist_ok=True)
            
            # Run FUSE (blocks until unmounted)
            FUSE(
                AVMFuse(agent_avm, self.agent_id),
                self.mountpoint,
                nothreads=True,
                foreground=True,
                allow_other=False,
            )
        except Exception as e:
            print(f"FUSE error for {self.mountpoint}: {e}", file=sys.stderr)
    
    def stop(self):
        """Stop this mount"""
        # Unmount
        import subprocess
        try:
            subprocess.run(["/sbin/umount", self.mountpoint], 
                         capture_output=True, timeout=5)
        except Exception:
            pass
        
        # Kill child process if still running
        if self.pid:
            try:
                os.kill(self.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


# ═══════════════════════════════════════════════════════════════
# Daemon
# ═══════════════════════════════════════════════════════════════

class AVMDaemon:
    """Unified AVM daemon managing multiple mounts"""
    
    def __init__(self):
        _lazy_imports()
        self.config = DaemonConfig.load()
        self.mounts: Dict[str, MountProcess] = {}
        self._running = False
    
    def start(self):
        """Start the daemon and all configured mounts"""
        if DAEMON_PID.exists():
            pid = int(DAEMON_PID.read_text().strip())
            try:
                os.kill(pid, 0)
                print(f"Daemon already running (pid={pid})")
                return False
            except ProcessLookupError:
                pass  # Stale pid file
        
        # Write PID
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        DAEMON_PID.write_text(str(os.getpid()))
        
        self._running = True
        
        # Start all enabled mounts
        for name, mount_config in self.config.mounts.items():
            if mount_config.enabled:
                self._start_mount(mount_config)
        
        print(f"Daemon started (pid={os.getpid()})")
        print(f"Mounts: {len(self.mounts)}")
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        
        # Wait for stop
        try:
            while self._running:
                signal.pause()
        except Exception:
            pass
        
        return True
    
    def _start_mount(self, mount_config: MountConfig):
        """Start a single mount"""
        proc = MountProcess(
            mount_config.mountpoint,
            mount_config.agent_id,
        )
        proc.start()
        self.mounts[mount_config.mountpoint] = proc
        print(f"  Mounted: {mount_config.mountpoint} (agent={mount_config.agent_id}, pid={proc.pid})")
    
    def _handle_signal(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutting down...")
        self._running = False
        
        # Stop all mounts
        for mount in self.mounts.values():
            mount.stop()
        
        # Remove PID file
        if DAEMON_PID.exists():
            DAEMON_PID.unlink()
    
    def add_mount(self, mountpoint: str, agent_id: str):
        """Add a mount configuration"""
        mountpoint = str(Path(mountpoint).resolve())
        self.config.mounts[mountpoint] = MountConfig(
            mountpoint=mountpoint,
            agent_id=agent_id,
        )
        self.config.save()
        print(f"Added: {mountpoint} (agent={agent_id})")
    
    def remove_mount(self, mountpoint: str):
        """Remove a mount configuration"""
        mountpoint = str(Path(mountpoint).resolve())
        if mountpoint in self.config.mounts:
            del self.config.mounts[mountpoint]
            self.config.save()
            print(f"Removed: {mountpoint}")
        else:
            print(f"Not found: {mountpoint}")
    
    def list_mounts(self):
        """List configured mounts"""
        if not self.config.mounts:
            print("No mounts configured")
            return
        
        print("Configured mounts:")
        for mp, mc in self.config.mounts.items():
            status = "✓" if mc.enabled else "○"
            print(f"  {status} {mp} → {mc.agent_id}")


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def cmd_start(args):
    """Start the daemon"""
    daemon = AVMDaemon()
    
    if args.daemon:
        # Fork to background
        pid = os.fork()
        if pid > 0:
            print(f"Daemon started in background (pid={pid})")
            return 0
        
        # Child process
        os.setsid()
        
        # Redirect stdout/stderr
        log_file = CONFIG_DIR / "daemon.log"
        sys.stdout = open(log_file, "a")
        sys.stderr = sys.stdout
    
    daemon.start()
    return 0


def cmd_stop(args):
    """Stop the daemon"""
    if not DAEMON_PID.exists():
        print("Daemon not running")
        return 1
    
    pid = int(DAEMON_PID.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Stopped daemon (pid={pid})")
        return 0
    except ProcessLookupError:
        DAEMON_PID.unlink()
        print("Daemon not running (stale pid file removed)")
        return 1


def cmd_status(args):
    """Show daemon status"""
    print("╭─────────────────────────────────────────╮")
    print("│         🧠 AVM Daemon Status            │")
    print("╰─────────────────────────────────────────╯")
    
    if not DAEMON_PID.exists():
        print("  Status: ⭘ not running")
    else:
        pid = int(DAEMON_PID.read_text().strip())
        try:
            os.kill(pid, 0)
            print(f"  Status: ● running (pid={pid})")
        except ProcessLookupError:
            print("  Status: ⭘ not running (stale pid)")
    
    daemon = AVMDaemon()
    config = daemon.config
    
    if not config.mounts:
        print("\n  No mounts configured")
    else:
        print(f"\n  Mounts: {len(config.mounts)}")
        print("  ─────────────────────────────────────")
        for mp, mc in config.mounts.items():
            status = "●" if mc.enabled else "○"
            agent = mc.agent_id
            short_path = mp.replace(str(Path.home()), "~")
            print(f"  {status} {agent:<12} → {short_path}")
    
    print()
    return 0


def cmd_inspect(args):
    """Inspect daemon and mounts in detail"""
    _lazy_imports()
    
    print("╭─────────────────────────────────────────╮")
    print("│         🔍 AVM Daemon Inspect           │")
    print("╰─────────────────────────────────────────╯")
    
    # Daemon info
    print("\n📋 Daemon")
    print("  ─────────────────────────────────────")
    if DAEMON_PID.exists():
        pid = int(DAEMON_PID.read_text().strip())
        try:
            os.kill(pid, 0)
            print(f"  PID:     {pid}")
            print(f"  Status:  ● running")
        except ProcessLookupError:
            print(f"  Status:  ⭘ not running (stale pid={pid})")
    else:
        print("  Status:  ⭘ not running")
    
    print(f"  Config:  {DAEMON_CONFIG}")
    print(f"  PID file: {DAEMON_PID}")
    
    # Database info
    print("\n💾 Database")
    print("  ─────────────────────────────────────")
    avm = AVM()
    db_path = avm.store.db_path
    print(f"  Path:    {db_path}")
    if Path(db_path).exists():
        size = Path(db_path).stat().st_size
        if size > 1024 * 1024:
            print(f"  Size:    {size / 1024 / 1024:.1f} MB")
        else:
            print(f"  Size:    {size / 1024:.1f} KB")
        
        # Node count
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            conn.close()
            print(f"  Nodes:   {count}")
        except Exception:
            pass
    
    # Mount details
    daemon = AVMDaemon()
    config = daemon.config
    
    print("\n📂 Mounts")
    print("  ─────────────────────────────────────")
    
    if not config.mounts:
        print("  (none configured)")
    else:
        # Check actual mount status
        import subprocess
        result = subprocess.run(["/sbin/mount"], capture_output=True, text=True)
        mounted = result.stdout
        
        for mp, mc in config.mounts.items():
            short_path = mp.replace(str(Path.home()), "~")
            is_mounted = mp in mounted or mp.replace("/Users/", "/private/var/") in mounted
            
            status_icon = "●" if is_mounted else "○"
            status_text = "mounted" if is_mounted else "not mounted"
            
            print(f"\n  {status_icon} {mc.agent_id}")
            print(f"    Path:   {short_path}")
            print(f"    Status: {status_text}")
            
            # Check if accessible
            if is_mounted:
                try:
                    list_path = Path(mp) / ":stats"
                    if list_path.exists():
                        import json
                        stats = json.loads(list_path.read_text())
                        print(f"    Nodes:  {stats.get('nodes', '?')}")
                except Exception:
                    pass
    
    # Process tree
    print("\n🌳 Processes")
    print("  ─────────────────────────────────────")
    try:
        result = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True
        )
        procs = [l for l in result.stdout.split("\n") if "avm-daemon" in l and "grep" not in l]
        if procs:
            for p in procs:
                parts = p.split()
                pid = parts[1]
                mem = parts[3]
                print(f"  pid={pid} mem={mem}%")
        else:
            print("  (no daemon processes)")
    except Exception:
        print("  (unable to check)")
    
    print()
    return 0


def cmd_add(args):
    """Add a mount"""
    daemon = AVMDaemon()
    daemon.add_mount(args.mountpoint, args.agent)
    return 0


def cmd_remove(args):
    """Remove a mount"""
    daemon = AVMDaemon()
    daemon.remove_mount(args.mountpoint)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="AVM Unified Daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # start
    start_parser = subparsers.add_parser("start", help="Start daemon")
    start_parser.add_argument("--daemon", "-d", action="store_true",
                              help="Run in background")
    start_parser.set_defaults(func=cmd_start)
    
    # stop
    stop_parser = subparsers.add_parser("stop", help="Stop daemon")
    stop_parser.set_defaults(func=cmd_stop)
    
    # status
    status_parser = subparsers.add_parser("status", help="Show status")
    status_parser.set_defaults(func=cmd_status)
    
    # inspect
    inspect_parser = subparsers.add_parser("inspect", help="Detailed inspection")
    inspect_parser.set_defaults(func=cmd_inspect)
    
    # add
    add_parser = subparsers.add_parser("add", help="Add mount")
    add_parser.add_argument("mountpoint", help="Mount point path")
    add_parser.add_argument("--agent", "-a", required=True,
                           help="Agent ID")
    add_parser.set_defaults(func=cmd_add)
    
    # remove
    remove_parser = subparsers.add_parser("remove", help="Remove mount")
    remove_parser.add_argument("mountpoint", help="Mount point path")
    remove_parser.set_defaults(func=cmd_remove)
    
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
