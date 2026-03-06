"""
avm/index_handler.py - Index Handler for Structured Data

Index is a special handler for semi-structured/unstructured content
that tracks file states without storing full content.

Features:
- Agent-provided or hook-generated descriptions
- Status tracking (clean/dirty/missing)
- Scan hooks for auto-generation

Usage:
    providers:
      - pattern: "/index/project/{name}/**"
        handler: index
        config:
          type: project
          root: ~/projects
          scan_hook: builtin:project_scan
"""

import json
import time
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Callable
from abc import ABC, abstractmethod

from .handlers import BaseHandler, handler


# ─── Index Data Models ─────────────────────────────────────

@dataclass
class FileEntry:
    """A file in the index."""
    path: str
    description: str = ""
    mtime: float = 0.0
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass  
class IndexEntry:
    """An index entry (e.g., a project)."""
    name: str
    root: str
    description: str = ""
    files: List[FileEntry] = field(default_factory=list)
    indexed_at: float = 0.0
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> "IndexEntry":
        files = [FileEntry(**f) for f in data.pop("files", [])]
        return cls(files=files, **data)
    
    def to_readable(self) -> str:
        """Generate human-readable content for display."""
        lines = [f"# {self.name}", ""]
        if self.description:
            lines.append(self.description)
            lines.append("")
        
        if self.tags:
            lines.append(f"Tags: {', '.join(self.tags)}")
            lines.append("")
        
        if self.files:
            lines.append("## Files")
            for f in self.files:
                desc = f": {f.description}" if f.description else ""
                lines.append(f"- {f.path}{desc}")
        
        return "\n".join(lines)
    
    def check_status(self) -> Dict[str, str]:
        """Check status of all files."""
        root = Path(self.root).expanduser()
        status = {}
        
        for f in self.files:
            full_path = root / f.path
            if not full_path.exists():
                status[f.path] = "missing"
            elif full_path.stat().st_mtime > f.mtime:
                status[f.path] = "dirty"
            else:
                status[f.path] = "clean"
        
        return status
    
    def status_report(self) -> str:
        """Generate status report."""
        status = self.check_status()
        lines = []
        
        clean = dirty = missing = 0
        for path, state in status.items():
            if state == "clean":
                clean += 1
            elif state == "dirty":
                dirty += 1
                lines.append(f"{path}: DIRTY")
            else:
                missing += 1
                lines.append(f"{path}: MISSING")
        
        summary = f"[{clean} clean, {dirty} dirty, {missing} missing]"
        if lines:
            return summary + "\n" + "\n".join(lines)
        return summary + "\nAll files clean."


# ─── Scan Hooks ─────────────────────────────────────────────

class ScanHook(ABC):
    """Base class for scan hooks."""
    
    @abstractmethod
    def scan(self, root: str, **kwargs) -> IndexEntry:
        """Scan and generate index."""
        pass


class ProjectScanHook(ScanHook):
    """Scan a project directory."""
    
    # File patterns to ignore
    IGNORE_PATTERNS = {
        "__pycache__", ".git", ".venv", "node_modules",
        ".pyc", ".pyo", ".so", ".dylib", ".egg-info",
        ".DS_Store", "Thumbs.db"
    }
    
    # File extensions to index
    INDEX_EXTENSIONS = {
        ".py", ".js", ".ts", ".go", ".rs", ".java", ".c", ".cpp", ".h",
        ".md", ".txt", ".yaml", ".yml", ".json", ".toml",
        ".sh", ".bash", ".zsh"
    }
    
    def scan(self, root: str, name: str = None, **kwargs) -> IndexEntry:
        root_path = Path(root).expanduser().resolve()
        name = name or root_path.name
        
        files = []
        for f in root_path.rglob("*"):
            # Skip ignored
            if any(p in str(f) for p in self.IGNORE_PATTERNS):
                continue
            
            if not f.is_file():
                continue
            
            # Only index known extensions
            if f.suffix.lower() not in self.INDEX_EXTENSIONS:
                continue
            
            rel_path = str(f.relative_to(root_path))
            files.append(FileEntry(
                path=rel_path,
                mtime=f.stat().st_mtime,
                description="",  # Agent fills later
            ))
        
        return IndexEntry(
            name=name,
            root=str(root_path),
            files=files,
            indexed_at=time.time(),
        )


# Hook registry
SCAN_HOOKS: Dict[str, ScanHook] = {
    "project": ProjectScanHook(),
}


def register_scan_hook(name: str, hook: ScanHook):
    """Register a custom scan hook."""
    SCAN_HOOKS[name] = hook


# ─── Index Store ─────────────────────────────────────────────

class IndexStore:
    """Storage for index entries."""
    
    def __init__(self, db_path: str = None):
        self._indices: Dict[str, Dict[str, IndexEntry]] = {}  # type -> name -> entry
        self._db_path = db_path
        self._load()
    
    def _storage_path(self) -> Path:
        if self._db_path:
            return Path(self._db_path).parent / "indices.json"
        return Path.home() / ".local" / "share" / "avm" / "indices.json"
    
    def _load(self):
        path = self._storage_path()
        if path.exists():
            try:
                data = json.loads(path.read_text())
                for index_type, entries in data.items():
                    self._indices[index_type] = {}
                    for name, entry_data in entries.items():
                        self._indices[index_type][name] = IndexEntry.from_dict(entry_data)
            except (json.JSONDecodeError, KeyError):
                pass
    
    def _save(self):
        path = self._storage_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {}
        for index_type, entries in self._indices.items():
            data[index_type] = {
                name: entry.to_dict() 
                for name, entry in entries.items()
            }
        
        path.write_text(json.dumps(data, indent=2, default=str))
    
    def get(self, index_type: str, name: str) -> Optional[IndexEntry]:
        return self._indices.get(index_type, {}).get(name)
    
    def save(self, index_type: str, entry: IndexEntry):
        if index_type not in self._indices:
            self._indices[index_type] = {}
        self._indices[index_type][entry.name] = entry
        self._save()
    
    def delete(self, index_type: str, name: str) -> bool:
        if index_type in self._indices and name in self._indices[index_type]:
            del self._indices[index_type][name]
            self._save()
            return True
        return False
    
    def list(self, index_type: str) -> List[str]:
        return list(self._indices.get(index_type, {}).keys())
    
    def list_all(self) -> Dict[str, List[str]]:
        return {t: list(e.keys()) for t, e in self._indices.items()}


# ─── Index Handler ─────────────────────────────────────────────

@handler("index",
         description="Structured index with status tracking and scan hooks",
         usage="""pattern: "/index/{type}/{name}"
handler: index
config:
  root: ~/projects
  scan_hook: project""",
         examples=[
             "cat /index/project/myapp",
             "cat /index/project/myapp:status",
             "echo 'scan' > /index/project/myapp:scan",
         ])
class IndexHandler(BaseHandler):
    """
    Handler for structured indices.
    
    Virtual suffixes:
    - :status - Check file states
    - :scan - Trigger scan hook
    - :files - List files only
    - :json - Raw JSON output
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.store = IndexStore()
        self.root = config.get("root", "")
        self.scan_hook_name = config.get("scan_hook", "project")
    
    def _parse_path(self, path: str) -> tuple:
        """Parse /index/{type}/{name}[:suffix] -> (type, name, suffix)"""
        parts = path.strip("/").split("/")
        
        if len(parts) < 2:
            return (None, None, None)
        
        # Skip 'index' prefix if present
        if parts[0] == "index":
            parts = parts[1:]
        
        if len(parts) < 2:
            return (parts[0] if parts else None, None, None)
        
        index_type = parts[0]
        name_part = parts[1]
        
        # Check for suffix
        suffix = None
        for s in (":status", ":scan", ":files", ":json"):
            if name_part.endswith(s):
                name_part = name_part[:-len(s)]
                suffix = s
                break
        
        return (index_type, name_part, suffix)
    
    def read(self, path: str, context: Dict[str, Any]) -> Optional[str]:
        index_type, name, suffix = self._parse_path(path)
        
        # List all types
        if not index_type:
            all_indices = self.store.list_all()
            return json.dumps(all_indices, indent=2)
        
        # List entries of type
        if not name:
            entries = self.store.list(index_type)
            return "\n".join(entries) if entries else "(empty)"
        
        # Get entry
        entry = self.store.get(index_type, name)
        
        if suffix == ":scan":
            # Trigger scan
            hook = SCAN_HOOKS.get(self.scan_hook_name)
            if not hook:
                return f"Error: Unknown scan hook '{self.scan_hook_name}'"
            
            root = self.root or context.get("root", "")
            if not root:
                return "Error: No root path configured"
            
            project_root = str(Path(root).expanduser() / name)
            entry = hook.scan(project_root, name=name)
            self.store.save(index_type, entry)
            return f"Scanned: {len(entry.files)} files indexed"
        
        if not entry:
            return f"Index '{index_type}/{name}' not found. Use :scan to create."
        
        if suffix == ":status":
            return entry.status_report()
        elif suffix == ":files":
            return "\n".join(f.path for f in entry.files)
        elif suffix == ":json":
            return json.dumps(entry.to_dict(), indent=2, default=str)
        else:
            return entry.to_readable()
    
    def write(self, path: str, content: str, context: Dict[str, Any]) -> bool:
        index_type, name, suffix = self._parse_path(path)
        
        if not index_type or not name:
            return False
        
        if suffix == ":scan":
            # Trigger scan on write
            self.read(path, context)
            return True
        
        # Update entry
        entry = self.store.get(index_type, name)
        if not entry:
            # Create new entry
            entry = IndexEntry(
                name=name,
                root=str(Path(self.root).expanduser() / name) if self.root else "",
                indexed_at=time.time(),
            )
        
        # Update description
        entry.description = content.strip()
        self.store.save(index_type, entry)
        return True
    
    def delete(self, path: str, context: Dict[str, Any]) -> bool:
        index_type, name, _ = self._parse_path(path)
        if index_type and name:
            return self.store.delete(index_type, name)
        return False
    
    def list(self, prefix: str, context: Dict[str, Any]) -> List[str]:
        index_type, _, _ = self._parse_path(prefix)
        if index_type:
            return self.store.list(index_type)
        return list(self.store.list_all().keys())
