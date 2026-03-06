"""Telemetry and observability for AVM operations."""

import json
import time
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from pathlib import Path
from contextlib import contextmanager
from dataclasses import dataclass, asdict


@dataclass
class OpLog:
    """Single operation log entry."""
    ts: str
    op: str
    agent: str
    path: Optional[str] = None
    query: Optional[str] = None
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    results: Optional[int] = None
    latency_ms: Optional[float] = None
    success: bool = True
    error: Optional[str] = None
    meta: Optional[Dict] = None


class Telemetry:
    """
    AVM telemetry collector.
    
    Logs operations to SQLite for analysis and benchmarking.
    """
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            data_dir = Path.home() / ".local" / "share" / "avm"
            data_dir.mkdir(parents=True, exist_ok=True)
            db_path = str(data_dir / "telemetry.db")
        
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize telemetry table."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS op_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    op TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    path TEXT,
                    query TEXT,
                    tokens_in INTEGER,
                    tokens_out INTEGER,
                    results INTEGER,
                    latency_ms REAL,
                    success INTEGER DEFAULT 1,
                    error TEXT,
                    meta TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_op_ts ON op_logs(ts)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_op_agent ON op_logs(agent)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_op_op ON op_logs(op)")
    
    def log(self, entry: OpLog):
        """Log an operation."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO op_logs 
                (ts, op, agent, path, query, tokens_in, tokens_out, 
                 results, latency_ms, success, error, meta)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry.ts,
                entry.op,
                entry.agent,
                entry.path,
                entry.query,
                entry.tokens_in,
                entry.tokens_out,
                entry.results,
                entry.latency_ms,
                1 if entry.success else 0,
                entry.error,
                json.dumps(entry.meta) if entry.meta else None
            ))
    
    @contextmanager
    def track(self, op: str, agent: str, **kwargs):
        """
        Context manager to track an operation.
        
        Usage:
            with telemetry.track("recall", "akashi", query="test") as t:
                result = do_recall()
                t["results"] = len(result)
                t["tokens_out"] = count_tokens(result)
        """
        start = time.perf_counter()
        ctx = {
            "op": op,
            "agent": agent,
            "success": True,
            "error": None,
            **kwargs
        }
        
        try:
            yield ctx
        except Exception as e:
            ctx["success"] = False
            ctx["error"] = str(e)
            raise
        finally:
            latency_ms = (time.perf_counter() - start) * 1000
            
            entry = OpLog(
                ts=datetime.now(timezone.utc).isoformat(),
                op=ctx["op"],
                agent=ctx["agent"],
                path=ctx.get("path"),
                query=ctx.get("query"),
                tokens_in=ctx.get("tokens_in"),
                tokens_out=ctx.get("tokens_out"),
                results=ctx.get("results"),
                latency_ms=latency_ms,
                success=ctx["success"],
                error=ctx.get("error"),
                meta=ctx.get("meta")
            )
            self.log(entry)
    
    def query(
        self,
        agent: str = None,
        op: str = None,
        since: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Query operation logs."""
        sql = "SELECT * FROM op_logs WHERE 1=1"
        params = []
        
        if agent:
            sql += " AND agent = ?"
            params.append(agent)
        if op:
            sql += " AND op = ?"
            params.append(op)
        if since:
            sql += " AND ts >= ?"
            params.append(since)
        
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
    
    def stats(self, agent: str = None, since: str = None) -> Dict:
        """Get aggregated statistics."""
        where = "WHERE 1=1"
        params = []
        
        if agent:
            where += " AND agent = ?"
            params.append(agent)
        if since:
            where += " AND ts >= ?"
            params.append(since)
        
        with sqlite3.connect(self.db_path) as conn:
            # Op counts
            rows = conn.execute(f"""
                SELECT op, COUNT(*) as count,
                       AVG(latency_ms) as avg_latency,
                       SUM(tokens_in) as total_tokens_in,
                       SUM(tokens_out) as total_tokens_out
                FROM op_logs {where}
                GROUP BY op
            """, params).fetchall()
            
            by_op = {}
            for row in rows:
                by_op[row[0]] = {
                    "count": row[1],
                    "avg_latency_ms": round(row[2], 2) if row[2] else None,
                    "total_tokens_in": row[3],
                    "total_tokens_out": row[4]
                }
            
            # Error rate
            total = conn.execute(
                f"SELECT COUNT(*) FROM op_logs {where}", params
            ).fetchone()[0]
            
            errors = conn.execute(
                f"SELECT COUNT(*) FROM op_logs {where} AND success = 0", params
            ).fetchone()[0]
            
            return {
                "total_ops": total,
                "error_rate": round(errors / total, 4) if total else 0,
                "by_op": by_op
            }
    
    def token_savings(self, agent: str = None, since: str = None) -> Dict:
        """Calculate token savings from recall operations."""
        where = "WHERE op = 'recall'"
        params = []
        
        if agent:
            where += " AND agent = ?"
            params.append(agent)
        if since:
            where += " AND ts >= ?"
            params.append(since)
        
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(f"""
                SELECT 
                    COUNT(*) as recalls,
                    SUM(tokens_in) as tokens_returned,
                    SUM(tokens_out) as tokens_available
                FROM op_logs {where}
            """, params).fetchone()
            
            recalls = row[0] or 0
            tokens_returned = row[1] or 0
            tokens_available = row[2] or 0
            
            if tokens_available > 0:
                savings_pct = round((1 - tokens_returned / tokens_available) * 100, 1)
            else:
                savings_pct = 0
            
            return {
                "recalls": recalls,
                "tokens_returned": tokens_returned,
                "tokens_available": tokens_available,
                "tokens_saved": tokens_available - tokens_returned,
                "savings_pct": savings_pct
            }


# Global instance
_telemetry: Optional[Telemetry] = None


def get_telemetry() -> Telemetry:
    """Get global telemetry instance."""
    global _telemetry
    if _telemetry is None:
        _telemetry = Telemetry()
    return _telemetry
