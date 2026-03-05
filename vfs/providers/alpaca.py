"""
vfs/providers/alpaca.py - Alpaca Trading API Provider
"""

import json
from datetime import datetime
from typing import Optional, Any

from .base import LiveProvider
from ..node import VFSNode
from ..store import VFSStore


class AlpacaPositionsProvider(LiveProvider):
    """
    Alpaca positions data
    
    path:
        /live/positions.md         - positions总览
        /live/positions/account.md - accountinfo
        /live/positions/AAPL.md    - singlepositions
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
        try:
            if path == "/live/positions.md":
                return self._fetch_positions()
            elif path == "/live/positions/account.md":
                return self._fetch_account()
            elif path.startswith("/live/positions/"):
                symbol = path.split("/")[-1].replace(".md", "")
                return self._fetch_position(symbol)
        except Exception as e:
            return self._make_node(
                path,
                f"# Error\n\nFailed to fetch: {e}",
                {"error": str(e)}
            )
        return None
    
    def _fetch_positions(self) -> VFSNode:
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
            {"position_count": len(positions), "total_pl": total_pl}
        )
    
    def _fetch_account(self) -> VFSNode:
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


class AlpacaOrdersProvider(LiveProvider):
    """
    Alpaca orders data
    
    path:
        /live/orders.md           - allorders
        /live/orders/open.md      - notfilledorders
        /live/orders/filled.md    - alreadyfilledorders
    """
    
    def __init__(self, store: VFSStore,
                 api_key: str, secret_key: str,
                 base_url: str = "https://paper-api.alpaca.markets",
                 ttl_seconds: int = 30):
        super().__init__(store, "/live/orders", ttl_seconds)
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url
    
    def _api_request(self, endpoint: str) -> Any:
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
        try:
            if path == "/live/orders.md":
                return self._fetch_orders("all")
            elif path == "/live/orders/open.md":
                return self._fetch_orders("open")
            elif path == "/live/orders/filled.md":
                return self._fetch_orders("filled")
        except Exception as e:
            return self._make_node(path, f"# Error\n\n{e}", {"error": str(e)})
        return None
    
    def _fetch_orders(self, status: str) -> VFSNode:
        endpoint = f"/v2/orders?status={status}&limit=50"
        orders = self._api_request(endpoint)
        
        lines = [
            f"# Orders ({status.title()})",
            "",
            "| Symbol | Side | Qty | Type | Status | Created |",
            "|--------|------|-----|------|--------|---------|",
        ]
        
        for o in orders:
            created = o.get("created_at", "")[:10]
            lines.append(
                f"| {o['symbol']} | {o['side']} | {o['qty']} | "
                f"{o['type']} | {o['status']} | {created} |"
            )
        
        lines.extend([
            "",
            f"**Total:** {len(orders)} orders",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
        ])
        
        path = "/live/orders.md" if status == "all" else f"/live/orders/{status}.md"
        return self._make_node(path, "\n".join(lines), {"order_count": len(orders)})
