"""
vfs/provider.py - 数据提供者

Provider 负责从外部数据源获取数据，转换为 VFSNode。
支持：
- LiveProvider: 实时数据（带TTL缓存）
- StaticProvider: 静态数据（手动更新）
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

from .node import VFSNode, NodeType
from .store import VFSStore


class VFSProvider(ABC):
    """
    数据提供者基类
    """
    
    def __init__(self, store: VFSStore, prefix: str):
        """
        Args:
            store: VFS存储
            prefix: 路径前缀（如 /live/positions）
        """
        self.store = store
        self.prefix = prefix
    
    @abstractmethod
    def fetch(self, path: str) -> Optional[VFSNode]:
        """
        从数据源获取数据
        
        子类实现具体的数据获取逻辑
        """
        pass
    
    def get(self, path: str, force_refresh: bool = False) -> Optional[VFSNode]:
        """
        获取节点（带缓存）
        
        1. 检查缓存
        2. 如果未过期，返回缓存
        3. 否则调用 fetch 刷新
        """
        if not path.startswith(self.prefix):
            return None
        
        cached = self.store.get_node(path)
        
        if cached and not force_refresh:
            if not cached.is_expired:
                return cached
        
        # 刷新
        node = self.fetch(path)
        if node:
            self.store._put_node_internal(node, save_diff=True)
        
        return node
    
    def refresh_all(self) -> int:
        """刷新所有节点，返回刷新数量"""
        count = 0
        for node in self.store.list_nodes(self.prefix):
            refreshed = self.get(node.path, force_refresh=True)
            if refreshed:
                count += 1
        return count


class LiveProvider(VFSProvider):
    """
    实时数据提供者
    
    特点：
    - 数据有 TTL
    - 读取时自动刷新过期数据
    """
    
    def __init__(self, store: VFSStore, prefix: str, ttl_seconds: int = 300):
        super().__init__(store, prefix)
        self.ttl_seconds = ttl_seconds
    
    def _make_node(self, path: str, content: str, 
                   meta: Dict = None) -> VFSNode:
        """创建带TTL的节点"""
        node_meta = meta or {}
        node_meta["ttl_seconds"] = self.ttl_seconds
        node_meta["provider"] = self.__class__.__name__
        
        return VFSNode(
            path=path,
            content=content,
            meta=node_meta,
            node_type=NodeType.FILE,
        )


class StaticProvider(VFSProvider):
    """
    静态数据提供者
    
    特点：
    - 数据长期有效
    - 需要手动触发更新
    """
    
    def _make_node(self, path: str, content: str,
                   meta: Dict = None) -> VFSNode:
        """创建静态节点"""
        node_meta = meta or {}
        node_meta["provider"] = self.__class__.__name__
        
        return VFSNode(
            path=path,
            content=content,
            meta=node_meta,
            node_type=NodeType.FILE,
        )


# ─── 具体实现 ─────────────────────────────────────────────


class AlpacaPositionsProvider(LiveProvider):
    """
    Alpaca 持仓数据提供者
    
    路径: /live/positions.md
    """
    
    def __init__(self, store: VFSStore, 
                 api_key: str, secret_key: str,
                 base_url: str = "https://paper-api.alpaca.markets",
                 ttl_seconds: int = 60):
        super().__init__(store, "/live/positions", ttl_seconds)
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url
    
    def _api_request(self, endpoint: str) -> Any:
        """调用Alpaca API"""
        import urllib.request
        
        req = urllib.request.Request(
            f"{self.base_url}{endpoint}",
            headers={
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.secret_key,
            }
        )
        
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    
    def fetch(self, path: str) -> Optional[VFSNode]:
        """获取持仓数据"""
        try:
            if path == "/live/positions.md":
                return self._fetch_positions()
            elif path == "/live/positions/account.md":
                return self._fetch_account()
            elif path.startswith("/live/positions/"):
                # /live/positions/AAPL.md
                symbol = path.split("/")[-1].replace(".md", "")
                return self._fetch_position(symbol)
        except Exception as e:
            # 返回错误节点
            return self._make_node(
                path,
                f"# Error\n\nFailed to fetch: {e}",
                {"error": str(e)}
            )
        
        return None
    
    def _fetch_positions(self) -> VFSNode:
        """获取所有持仓"""
        positions = self._api_request("/v2/positions")
        account = self._api_request("/v2/account")
        
        lines = [
            "# Portfolio Positions",
            "",
            f"**Equity:** ${float(account.get('equity', 0)):,.2f}",
            f"**Cash:** ${float(account.get('cash', 0)):,.2f}",
            f"**Buying Power:** ${float(account.get('buying_power', 0)):,.2f}",
            "",
            "## Positions",
            "",
            "| Symbol | Qty | Avg Cost | Current | P/L | P/L % |",
            "|--------|-----|----------|---------|-----|-------|",
        ]
        
        total_pl = 0
        for pos in positions:
            symbol = pos["symbol"]
            qty = int(pos["qty"])
            avg_cost = float(pos["avg_entry_price"])
            current = float(pos["current_price"])
            pl = float(pos["unrealized_pl"])
            pl_pct = float(pos["unrealized_plpc"]) * 100
            total_pl += pl
            
            lines.append(
                f"| {symbol} | {qty} | ${avg_cost:.2f} | ${current:.2f} | "
                f"${pl:+,.2f} | {pl_pct:+.2f}% |"
            )
        
        lines.extend([
            "",
            f"**Total Unrealized P/L:** ${total_pl:+,.2f}",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
        ])
        
        return self._make_node(
            "/live/positions.md",
            "\n".join(lines),
            {
                "position_count": len(positions),
                "total_pl": total_pl,
            }
        )
    
    def _fetch_account(self) -> VFSNode:
        """获取账户信息"""
        account = self._api_request("/v2/account")
        
        lines = [
            "# Account Summary",
            "",
            f"- **Account ID:** {account.get('id', 'N/A')}",
            f"- **Status:** {account.get('status', 'N/A')}",
            f"- **Equity:** ${float(account.get('equity', 0)):,.2f}",
            f"- **Cash:** ${float(account.get('cash', 0)):,.2f}",
            f"- **Buying Power:** ${float(account.get('buying_power', 0)):,.2f}",
            f"- **Portfolio Value:** ${float(account.get('portfolio_value', 0)):,.2f}",
            f"- **Day Trade Count:** {account.get('daytrade_count', 0)}",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
        ]
        
        return self._make_node(
            "/live/positions/account.md",
            "\n".join(lines),
            {"account_id": account.get("id")}
        )
    
    def _fetch_position(self, symbol: str) -> VFSNode:
        """获取单个持仓"""
        try:
            pos = self._api_request(f"/v2/positions/{symbol}")
        except Exception:
            return self._make_node(
                f"/live/positions/{symbol}.md",
                f"# {symbol}\n\nNo position found.",
                {"symbol": symbol, "has_position": False}
            )
        
        lines = [
            f"# {symbol} Position",
            "",
            f"- **Quantity:** {pos['qty']}",
            f"- **Avg Entry Price:** ${float(pos['avg_entry_price']):.2f}",
            f"- **Current Price:** ${float(pos['current_price']):.2f}",
            f"- **Market Value:** ${float(pos['market_value']):,.2f}",
            f"- **Unrealized P/L:** ${float(pos['unrealized_pl']):+,.2f}",
            f"- **Unrealized P/L %:** {float(pos['unrealized_plpc'])*100:+.2f}%",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
        ]
        
        return self._make_node(
            f"/live/positions/{symbol}.md",
            "\n".join(lines),
            {
                "symbol": symbol,
                "has_position": True,
                "qty": int(pos["qty"]),
                "market_value": float(pos["market_value"]),
            }
        )


class MemoryProvider(VFSProvider):
    """
    Bot 记忆区提供者
    
    路径: /memory/*
    可读写
    """
    
    def __init__(self, store: VFSStore):
        super().__init__(store, "/memory")
    
    def fetch(self, path: str) -> Optional[VFSNode]:
        """Memory 区不需要外部 fetch，直接从 store 读取"""
        return self.store.get_node(path)
    
    def write(self, path: str, content: str, meta: Dict = None) -> VFSNode:
        """写入记忆"""
        if not path.startswith("/memory"):
            raise PermissionError(f"Cannot write to {path}")
        
        node = VFSNode(
            path=path,
            content=content,
            meta=meta or {},
            node_type=NodeType.FILE,
        )
        
        return self.store.put_node(node)
