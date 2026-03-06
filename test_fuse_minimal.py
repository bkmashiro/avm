#!/usr/bin/env python3
"""
Minimal FUSE test to isolate fusepy/macFUSE behavior.

Usage:
    python test_fuse_minimal.py /tmp/test-mount

Then in another terminal:
    echo "hello" > /tmp/test-mount/test.txt
    cat /tmp/test-mount/test.txt
"""

import os
import sys
import errno
import stat
from fuse import FUSE, FuseOSError, Operations


class MemoryFS(Operations):
    """Simple in-memory filesystem."""
    
    def __init__(self):
        self.files = {}  # path -> content
        self.attrs = {}  # path -> attributes
        print("[MemoryFS] Initialized", flush=True)
    
    def _log(self, op, path, *args):
        import sys
        sys.stderr.write(f"[{op}] {path} {args}\n")
        sys.stderr.flush()
    
    # ─── Metadata ─────────────────────────────────────────
    
    def getattr(self, path, fh=None):
        self._log("getattr", path)
        
        if path == '/':
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        
        if path in self.files:
            size = len(self.files[path])
            return dict(st_mode=(stat.S_IFREG | 0o644), st_nlink=1, st_size=size)
        
        # Check if it's a directory (has children)
        for p in self.files:
            if p.startswith(path + '/'):
                return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        
        raise FuseOSError(errno.ENOENT)
    
    def readdir(self, path, fh):
        self._log("readdir", path)
        
        entries = ['.', '..']
        prefix = path if path == '/' else path + '/'
        
        seen = set()
        for p in self.files:
            if p.startswith(prefix):
                rest = p[len(prefix):]
                name = rest.split('/')[0]
                if name and name not in seen:
                    entries.append(name)
                    seen.add(name)
        
        return entries
    
    # ─── File Operations ─────────────────────────────────
    
    def create(self, path, mode, fi=None):
        self._log("create", path, mode)
        self.files[path] = b''
        return 0
    
    def open(self, path, flags):
        self._log("open", path, flags)
        return 0
    
    def read(self, path, size, offset, fh):
        self._log("read", path, size, offset)
        data = self.files.get(path, b'')
        return data[offset:offset+size]
    
    def write(self, path, data, offset, fh):
        self._log("write", path, len(data), offset)
        
        if path not in self.files:
            self.files[path] = b''
        
        content = self.files[path]
        # Extend if needed
        if offset > len(content):
            content = content + b'\x00' * (offset - len(content))
        
        self.files[path] = content[:offset] + data + content[offset+len(data):]
        return len(data)
    
    def truncate(self, path, length, fh=None):
        self._log("truncate", path, length)
        if path in self.files:
            self.files[path] = self.files[path][:length]
        else:
            self.files[path] = b'\x00' * length
        return 0
    
    def release(self, path, fh):
        self._log("release", path)
        return 0
    
    def flush(self, path, fh):
        self._log("flush", path)
        return 0
    
    # ─── Directory Operations ─────────────────────────────
    
    def mkdir(self, path, mode):
        self._log("mkdir", path)
        return 0
    
    def rmdir(self, path):
        self._log("rmdir", path)
        return 0
    
    def unlink(self, path):
        self._log("unlink", path)
        self.files.pop(path, None)
        return 0


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <mountpoint>")
        sys.exit(1)
    
    mountpoint = sys.argv[1]
    os.makedirs(mountpoint, exist_ok=True)
    
    print(f"Mounting MemoryFS at {mountpoint}")
    print("Press Ctrl+C to unmount")
    print("-" * 40)
    
    FUSE(
        MemoryFS(),
        mountpoint,
        foreground=True,
        nothreads=True,
        debug=False,
    )


if __name__ == '__main__':
    main()
