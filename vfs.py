"""
vfs.py — AI Virtual Filesystem Core Engine
VFSNode, VFSEdge, VFSProvider, VFSEngine
"""

import sqlite3, json, time, fnmatch
from dataclasses import dataclass, field
from typing import Optional, List
from pathlib import Path

DB_PATH = Path('~/.openclaw/workspace/vfs/vfs.db').expanduser()

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    path TEXT PRIMARY KEY,
    content TEXT NOT NULL DEFAULT '',
    raw_data TEXT DEFAULT '{}',
    sources TEXT DEFAULT '[]',
    confidence REAL DEFAULT 1.0,
    created_at REAL,
    updated_at REAL,
    expires_at REAL,
    tags TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_path TEXT NOT NULL,
    to_path TEXT NOT NULL,
    relation TEXT NOT NULL,
    weight REAL DEFAULT 1.0,
    metadata TEXT DEFAULT '{}',
    created_at REAL,
    UNIQUE(from_path, to_path, relation)
);

CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
    path, content, tags,
    content='nodes',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS nodes_ai AFTER INSERT ON nodes BEGIN
    INSERT INTO nodes_fts(rowid, path, content, tags)
    VALUES (new.rowid, new.path, new.content, new.tags);
END;

CREATE TRIGGER IF NOT EXISTS nodes_au AFTER UPDATE ON nodes BEGIN
    INSERT INTO nodes_fts(nodes_fts, rowid, path, content, tags)
    VALUES('delete', old.rowid, old.path, old.content, old.tags);
    INSERT INTO nodes_fts(rowid, path, content, tags)
    VALUES (new.rowid, new.path, new.content, new.tags);
END;

CREATE TRIGGER IF NOT EXISTS nodes_ad AFTER DELETE ON nodes BEGIN
    INSERT INTO nodes_fts(nodes_fts, rowid, path, content, tags)
    VALUES('delete', old.rowid, old.path, old.content, old.tags);
END;
"""


@dataclass
class VFSNode:
    path: str
    content: str                  # Markdown/text — AI reads this
    raw_data: dict = field(default_factory=dict)   # structured data
    sources: list = field(default_factory=list)    # ["edgar", "alpaca"]
    confidence: float = 1.0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None             # None = never expires
    tags: list = field(default_factory=list)


@dataclass
class VFSEdge:
    from_path: str
    to_path: str
    relation: str    # "COMPETES_WITH", "CEO_OF", "RELATED_TO", etc.
    weight: float = 1.0
    metadata: dict = field(default_factory=dict)


class VFSProvider:
    """Base class for mountable data providers."""
    pattern: str = "*"   # glob path pattern
    ttl: int = 3600      # default TTL in seconds; 0 = never expires

    def match(self, path: str) -> bool:
        return fnmatch.fnmatch(path, self.pattern)

    def fetch(self, path: str, **kwargs) -> Optional[VFSNode]:
        """Fetch data, return VFSNode or None."""
        raise NotImplementedError

    def can_write(self) -> bool:
        return False

    def write(self, path: str, content: str, **kwargs) -> bool:
        return False


class VFSEngine:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = Path(db_path)
        self.providers: List[VFSProvider] = []
        self._init_db()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def mount(self, provider: VFSProvider):
        """Mount a data provider."""
        self.providers.append(provider)

    def _init_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(SCHEMA)

    # ── Internal helpers ───────────────────────────────────────────────────

    def _get_from_db(self, path: str) -> Optional[VFSNode]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT path,content,raw_data,sources,confidence,created_at,updated_at,expires_at,tags "
                "FROM nodes WHERE path=?", (path,)
            ).fetchone()
        return self._row_to_node(row) if row else None

    def _is_stale(self, node: VFSNode) -> bool:
        if node.expires_at is None:
            return False
        return time.time() > node.expires_at

    def _upsert(self, node: VFSNode):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO nodes(path,content,raw_data,sources,confidence,
                                     created_at,updated_at,expires_at,tags)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(path) DO UPDATE SET
                       content=excluded.content,
                       raw_data=excluded.raw_data,
                       sources=excluded.sources,
                       confidence=excluded.confidence,
                       updated_at=excluded.updated_at,
                       expires_at=excluded.expires_at,
                       tags=excluded.tags""",
                (
                    node.path, node.content,
                    json.dumps(node.raw_data), json.dumps(node.sources),
                    node.confidence, node.created_at, node.updated_at,
                    node.expires_at, json.dumps(node.tags),
                ),
            )

    def _row_to_node(self, row) -> VFSNode:
        return VFSNode(
            path=row[0], content=row[1],
            raw_data=json.loads(row[2] or '{}'),
            sources=json.loads(row[3] or '[]'),
            confidence=row[4],
            created_at=row[5], updated_at=row[6], expires_at=row[7],
            tags=json.loads(row[8] or '[]'),
        )

    # ── Public API ─────────────────────────────────────────────────────────

    def read(self, path: str, force_refresh: bool = False) -> Optional[VFSNode]:
        """Read a node; auto-refresh via provider when TTL expires."""
        node = self._get_from_db(path)

        if node and not force_refresh and not self._is_stale(node):
            return node

        # Try providers
        for provider in self.providers:
            if provider.match(path):
                fresh = provider.fetch(path)
                if fresh:
                    if provider.ttl > 0:
                        fresh.expires_at = time.time() + provider.ttl
                    self._upsert(fresh)
                    return fresh

        return node  # Return stale data rather than None

    def write(
        self, path: str, content: str,
        sources=None, tags=None, raw_data=None,
        confidence: float = 1.0, ttl: Optional[int] = None,
    ) -> VFSNode:
        """Write a node manually."""
        now = time.time()
        node = VFSNode(
            path=path, content=content,
            sources=sources or ['manual'],
            tags=tags or [],
            raw_data=raw_data or {},
            confidence=confidence,
            created_at=now, updated_at=now,
            expires_at=now + ttl if ttl else None,
        )
        self._upsert(node)
        return node

    def ls(self, prefix: str) -> List[str]:
        """List all nodes under a path prefix."""
        prefix = prefix.rstrip('/') + '/'
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT path FROM nodes WHERE path LIKE ? ORDER BY path",
                (prefix + '%',),
            ).fetchall()
        return [r[0] for r in rows]

    def link(
        self, from_path: str, to_path: str, relation: str,
        weight: float = 1.0, metadata: Optional[dict] = None,
    ):
        """Add a directed relationship edge."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO edges
                       (from_path, to_path, relation, weight, metadata, created_at)
                   VALUES(?,?,?,?,?,?)""",
                (from_path, to_path, relation, weight,
                 json.dumps(metadata or {}), time.time()),
            )

    def links(self, path: str) -> List[VFSEdge]:
        """Return all edges connected to a node (in or out)."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT from_path, to_path, relation, weight, metadata
                   FROM edges WHERE from_path=? OR to_path=?""",
                (path, path),
            ).fetchall()
        return [VFSEdge(r[0], r[1], r[2], r[3], json.loads(r[4])) for r in rows]

    def search(self, query: str, limit: int = 10) -> List[VFSNode]:
        """Full-text search across path, content, and tags (FTS5)."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT n.path,n.content,n.raw_data,n.sources,n.confidence,
                          n.created_at,n.updated_at,n.expires_at,n.tags
                   FROM nodes n
                   JOIN nodes_fts f ON n.rowid = f.rowid
                   WHERE nodes_fts MATCH ?
                   ORDER BY rank
                   LIMIT ?""",
                (query, limit),
            ).fetchall()
        return [self._row_to_node(r) for r in rows]

    def delete(self, path: str):
        """Delete a node by path."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM nodes WHERE path=?", (path,))

    def touch(self, path: str) -> VFSNode:
        """Create an empty node if it doesn't exist."""
        node = self._get_from_db(path)
        if node:
            return node
        return self.write(path, '')

    def stats(self) -> dict:
        """Return basic database statistics."""
        with sqlite3.connect(self.db_path) as conn:
            n_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            n_edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        return {'nodes': n_nodes, 'edges': n_edges, 'db': str(self.db_path)}
