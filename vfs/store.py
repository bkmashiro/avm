"""
vfs/store.py - SQLite存储层（含FTS5全文搜索）
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
import difflib

from .node import VFSNode, NodeDiff, NodeType
from .graph import KVGraph, Edge, EdgeType


# SQLite schema
SCHEMA = """
-- 节点表
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT UNIQUE NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    meta TEXT NOT NULL DEFAULT '{}',
    node_type TEXT NOT NULL DEFAULT 'file',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    content_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_nodes_path ON nodes(path);

-- FTS5全文搜索索引（独立表，不使用external content避免同步问题）
CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
    path,
    content
);

-- 边表（关系图）
CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    edge_type TEXT NOT NULL DEFAULT 'related',
    weight REAL NOT NULL DEFAULT 1.0,
    meta TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(source, target, edge_type)
);

CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target);

-- 变更历史表
CREATE TABLE IF NOT EXISTS diffs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_path TEXT NOT NULL,
    version INTEGER NOT NULL,
    old_hash TEXT,
    new_hash TEXT NOT NULL,
    diff_content TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    change_type TEXT NOT NULL DEFAULT 'update'
);

CREATE INDEX IF NOT EXISTS idx_diffs_path ON diffs(node_path);
CREATE INDEX IF NOT EXISTS idx_diffs_version ON diffs(node_path, version);

-- 向量表（sqlite-vec预留，暂用普通表存embedding）
CREATE TABLE IF NOT EXISTS embeddings (
    path TEXT PRIMARY KEY,
    vector BLOB,  -- 序列化的float数组
    model TEXT,
    updated_at TEXT
);
"""


class VFSStore:
    """
    VFS SQLite存储
    
    功能：
    - 节点CRUD
    - FTS5全文搜索
    - 关系图存储
    - 变更历史
    """
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = str(Path.home() / ".openclaw" / "vfs" / "vfs.db")
        
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self._init_db()
    
    def _init_db(self):
        """初始化数据库"""
        with self._conn() as conn:
            conn.executescript(SCHEMA)
    
    @contextmanager
    def _conn(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
    
    # ─── 节点操作 ─────────────────────────────────────────
    
    def get_node(self, path: str) -> Optional[VFSNode]:
        """读取节点"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM nodes WHERE path = ?", (path,)
            ).fetchone()
            
            if row is None:
                return None
            
            return VFSNode(
                path=row["path"],
                content=row["content"],
                meta=json.loads(row["meta"]),
                node_type=NodeType(row["node_type"]),
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                version=row["version"],
            )
    
    def put_node(self, node: VFSNode, save_diff: bool = True) -> VFSNode:
        """
        写入节点
        
        - 检查写权限
        - 自动递增版本
        - 保存diff
        """
        if not node.is_writable:
            # 只读路径：仅允许内部provider写入（通过 _put_node_internal）
            raise PermissionError(f"Path {node.path} is read-only")
        
        return self._put_node_internal(node, save_diff)
    
    def _put_node_internal(self, node: VFSNode, save_diff: bool = True) -> VFSNode:
        """
        内部写入（绕过权限检查，供provider使用）
        """
        with self._conn() as conn:
            existing = self.get_node(node.path)
            
            now = datetime.utcnow()
            new_hash = node.content_hash
            
            if existing:
                # 更新
                old_hash = existing.content_hash
                new_version = existing.version + 1
                
                if save_diff and old_hash != new_hash:
                    # 保存diff
                    diff = self._compute_diff(existing.content, node.content)
                    self._save_diff(conn, NodeDiff(
                        node_path=node.path,
                        version=new_version,
                        old_hash=old_hash,
                        new_hash=new_hash,
                        diff_content=diff,
                        change_type="update",
                    ))
                
                conn.execute("""
                    UPDATE nodes SET 
                        content = ?, meta = ?, node_type = ?,
                        updated_at = ?, version = ?, content_hash = ?
                    WHERE path = ?
                """, (
                    node.content,
                    json.dumps(node.meta),
                    node.node_type.value,
                    now.isoformat(),
                    new_version,
                    new_hash,
                    node.path,
                ))
                
                # 更新FTS索引
                conn.execute("DELETE FROM nodes_fts WHERE path = ?", (node.path,))
                conn.execute(
                    "INSERT INTO nodes_fts (path, content) VALUES (?, ?)",
                    (node.path, node.content)
                )
                
                node.version = new_version
                node.updated_at = now
            else:
                # 新建
                if save_diff:
                    self._save_diff(conn, NodeDiff(
                        node_path=node.path,
                        version=1,
                        old_hash=None,
                        new_hash=new_hash,
                        diff_content=node.content,
                        change_type="create",
                    ))
                
                conn.execute("""
                    INSERT INTO nodes 
                        (path, content, meta, node_type, created_at, updated_at, version, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    node.path,
                    node.content,
                    json.dumps(node.meta),
                    node.node_type.value,
                    now.isoformat(),
                    now.isoformat(),
                    1,
                    new_hash,
                ))
                
                # 插入FTS索引
                conn.execute(
                    "INSERT INTO nodes_fts (path, content) VALUES (?, ?)",
                    (node.path, node.content)
                )
                
                node.version = 1
                node.created_at = now
                node.updated_at = now
        
        return node
    
    def delete_node(self, path: str) -> bool:
        """删除节点"""
        node = self.get_node(path)
        if node is None:
            return False
        
        if not node.is_writable:
            raise PermissionError(f"Path {path} is read-only")
        
        with self._conn() as conn:
            # 记录删除
            self._save_diff(conn, NodeDiff(
                node_path=path,
                version=node.version + 1,
                old_hash=node.content_hash,
                new_hash="",
                diff_content="",
                change_type="delete",
            ))
            
            conn.execute("DELETE FROM nodes WHERE path = ?", (path,))
            conn.execute("DELETE FROM nodes_fts WHERE path = ?", (path,))
            conn.execute("DELETE FROM edges WHERE source = ? OR target = ?", (path, path))
        
        return True
    
    def list_nodes(self, prefix: str = "/", limit: int = 100) -> List[VFSNode]:
        """列出节点"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM nodes WHERE path LIKE ? ORDER BY path LIMIT ?",
                (prefix + "%", limit)
            ).fetchall()
            
            return [
                VFSNode(
                    path=row["path"],
                    content=row["content"],
                    meta=json.loads(row["meta"]),
                    node_type=NodeType(row["node_type"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                    version=row["version"],
                )
                for row in rows
            ]
    
    # ─── 搜索 ─────────────────────────────────────────────
    
    def search(self, query: str, limit: int = 10) -> List[Tuple[VFSNode, float]]:
        """
        FTS5全文搜索
        返回 [(node, score), ...]
        
        自动添加前缀匹配（*）以支持中英混合文本
        """
        # 对每个词添加前缀匹配
        terms = query.split()
        fts_query = " ".join(f"{term}*" for term in terms)
        
        with self._conn() as conn:
            # FTS5 BM25 ranking
            rows = conn.execute("""
                SELECT nodes.*, bm25(nodes_fts) as score
                FROM nodes_fts
                JOIN nodes ON nodes_fts.path = nodes.path
                WHERE nodes_fts MATCH ?
                ORDER BY score
                LIMIT ?
            """, (fts_query, limit)).fetchall()
            
            results = []
            for row in rows:
                node = VFSNode(
                    path=row["path"],
                    content=row["content"],
                    meta=json.loads(row["meta"]),
                    node_type=NodeType(row["node_type"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                    version=row["version"],
                )
                results.append((node, abs(row["score"])))
            
            return results
    
    # ─── 关系图 ─────────────────────────────────────────────
    
    def add_edge(self, source: str, target: str,
                 edge_type: EdgeType = EdgeType.RELATED,
                 weight: float = 1.0,
                 meta: Dict = None) -> Edge:
        """添加边"""
        edge = Edge(
            source=source,
            target=target,
            edge_type=edge_type,
            weight=weight,
            meta=meta or {},
        )
        
        with self._conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO edges 
                    (source, target, edge_type, weight, meta, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                source, target, edge_type.value, weight,
                json.dumps(meta or {}),
                datetime.utcnow().isoformat(),
            ))
        
        return edge
    
    def get_links(self, path: str, 
                  direction: str = "both",
                  edge_type: EdgeType = None) -> List[Edge]:
        """获取节点的关联边"""
        with self._conn() as conn:
            edges = []
            
            if direction in ("out", "both"):
                sql = "SELECT * FROM edges WHERE source = ?"
                params = [path]
                if edge_type:
                    sql += " AND edge_type = ?"
                    params.append(edge_type.value)
                
                for row in conn.execute(sql, params):
                    edges.append(Edge(
                        source=row["source"],
                        target=row["target"],
                        edge_type=EdgeType(row["edge_type"]),
                        weight=row["weight"],
                        meta=json.loads(row["meta"]),
                        created_at=datetime.fromisoformat(row["created_at"]),
                    ))
            
            if direction in ("in", "both"):
                sql = "SELECT * FROM edges WHERE target = ?"
                params = [path]
                if edge_type:
                    sql += " AND edge_type = ?"
                    params.append(edge_type.value)
                
                for row in conn.execute(sql, params):
                    edges.append(Edge(
                        source=row["source"],
                        target=row["target"],
                        edge_type=EdgeType(row["edge_type"]),
                        weight=row["weight"],
                        meta=json.loads(row["meta"]),
                        created_at=datetime.fromisoformat(row["created_at"]),
                    ))
            
            return edges
    
    def load_graph(self) -> KVGraph:
        """加载完整图到内存"""
        graph = KVGraph()
        
        with self._conn() as conn:
            for row in conn.execute("SELECT * FROM edges"):
                graph.add_edge(
                    row["source"],
                    row["target"],
                    EdgeType(row["edge_type"]),
                    row["weight"],
                    json.loads(row["meta"]),
                )
        
        return graph
    
    # ─── Diff ─────────────────────────────────────────────
    
    def _compute_diff(self, old: str, new: str) -> str:
        """计算unified diff"""
        diff = difflib.unified_diff(
            old.splitlines(keepends=True),
            new.splitlines(keepends=True),
            lineterm="",
        )
        return "".join(diff)
    
    def _save_diff(self, conn, diff: NodeDiff):
        """保存diff记录"""
        conn.execute("""
            INSERT INTO diffs 
                (node_path, version, old_hash, new_hash, diff_content, changed_at, change_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            diff.node_path,
            diff.version,
            diff.old_hash,
            diff.new_hash,
            diff.diff_content,
            diff.changed_at.isoformat(),
            diff.change_type,
        ))
    
    def get_history(self, path: str, limit: int = 10) -> List[NodeDiff]:
        """获取变更历史"""
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT * FROM diffs 
                WHERE node_path = ? 
                ORDER BY version DESC 
                LIMIT ?
            """, (path, limit)).fetchall()
            
            return [
                NodeDiff(
                    node_path=row["node_path"],
                    version=row["version"],
                    old_hash=row["old_hash"],
                    new_hash=row["new_hash"],
                    diff_content=row["diff_content"],
                    changed_at=datetime.fromisoformat(row["changed_at"]),
                    change_type=row["change_type"],
                )
                for row in rows
            ]
    
    # ─── 统计 ─────────────────────────────────────────────
    
    def stats(self) -> Dict[str, Any]:
        """获取存储统计"""
        with self._conn() as conn:
            node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
            diff_count = conn.execute("SELECT COUNT(*) FROM diffs").fetchone()[0]
            
            # 按路径前缀统计
            prefix_stats = {}
            for row in conn.execute("""
                SELECT 
                    CASE 
                        WHEN path LIKE '/live%' THEN '/live'
                        WHEN path LIKE '/research%' THEN '/research'
                        WHEN path LIKE '/memory%' THEN '/memory'
                        ELSE '/other'
                    END as prefix,
                    COUNT(*) as cnt
                FROM nodes GROUP BY prefix
            """):
                prefix_stats[row["prefix"]] = row["cnt"]
            
            return {
                "nodes": node_count,
                "edges": edge_count,
                "diffs": diff_count,
                "by_prefix": prefix_stats,
                "db_path": self.db_path,
            }
