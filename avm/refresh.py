"""
avm/refresh.py - Auto-refresh mechanism

Scheduled refresh of expired live nodes
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Callable, Optional, Dict, List
from pathlib import Path

from .store import AVMStore
from .node import AVMNode


class RefreshScheduler:
    """
    Refresh scheduler
    
    Scheduled refresh of expired live nodes
    """
    
    def __init__(self, store: AVMStore, interval_seconds: int = 60):
        """
        Args:
            store: VFS storage
            interval_seconds: Check interval (seconds)
        """
        self.store = store
        self.interval = interval_seconds
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._callbacks: List[Callable[[AVMNode], None]] = []
    
    def add_callback(self, callback: Callable[[AVMNode], None]):
        """addrefreshcallback"""
        self._callbacks.append(callback)
    
    def _refresh_expired(self):
        """refreshallexpirednode"""
        nodes = self.store.list_nodes("/live", limit=1000)
        refreshed = []
        
        for node in nodes:
            if node.is_expired:
                # via provider refresh（requiresexternalconfiguration）
                refreshed.append(node)
                
                for callback in self._callbacks:
                    try:
                        callback(node)
                    except Exception as e:
                        print(f"Callback error for {node.path}: {e}")
        
        return refreshed
    
    def _run_loop(self):
        """Background refresh loop"""
        while not self._stop_event.is_set():
            try:
                self._refresh_expired()
            except Exception as e:
                print(f"Refresh error: {e}")
            
            self._stop_event.wait(self.interval)
    
    def start(self):
        """Start background refresh"""
        if self._thread and self._thread.is_alive():
            return
        
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
    
    def stop(self):
        """stopbackgroundrefresh"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)


class RefreshManager:
    """
    Refresh manager
    
    Manual refresh specified path or all live nodes
    """
    
    def __init__(self, store: AVMStore):
        self.store = store
        self._providers = {}
    
    def register_provider(self, prefix: str, provider):
        """register provider"""
        self._providers[prefix] = provider
    
    def refresh_path(self, path: str, force: bool = True) -> Optional[AVMNode]:
        """refreshspecifiedpath"""
        for prefix, provider in self._providers.items():
            if path.startswith(prefix):
                return provider.get(path, force_refresh=force)
        
        return None
    
    def refresh_prefix(self, prefix: str) -> List[AVMNode]:
        """refreshspecifiedprefixunderallnode"""
        nodes = self.store.list_nodes(prefix, limit=1000)
        refreshed = []
        
        for node in nodes:
            result = self.refresh_path(node.path, force=True)
            if result:
                refreshed.append(result)
        
        return refreshed
    
    def refresh_all(self) -> Dict[str, int]:
        """refreshall live node"""
        stats = {}
        
        for prefix in self._providers.keys():
            nodes = self.refresh_prefix(prefix)
            stats[prefix] = len(nodes)
        
        return stats
    
    def get_expired(self) -> List[AVMNode]:
        """getallexpirednode"""
        nodes = self.store.list_nodes("/live", limit=1000)
        return [n for n in nodes if n.is_expired]


def refresh_all_providers(store: AVMStore) -> Dict[str, int]:
    """
    refreshall provider
    
    Convenience function, auto-load all known providers
    """
    from .providers import (
        TechnicalIndicatorsProvider,
        NewsProvider,
        WatchlistProvider,
    )
    
    manager = RefreshManager(store)
    
    # Register providers that don't need auth
    manager.register_provider("/live/indicators", TechnicalIndicatorsProvider(store))
    manager.register_provider("/live/news", NewsProvider(store))
    manager.register_provider("/live/watchlist", WatchlistProvider(store))
    
    # tryregister Alpaca（requirescredentials）
    env_path = Path.home() / ".openclaw" / "workspace" / "trading" / ".env"
    if env_path.exists():
        from .providers import AlpacaPositionsProvider, AlpacaOrdersProvider
        
        env = dict(
            line.split("=", 1) 
            for line in env_path.read_text().splitlines() 
            if "=" in line
        )
        
        if env.get("ALPACA_API_KEY"):
            manager.register_provider(
                "/live/positions",
                AlpacaPositionsProvider(
                    store,
                    api_key=env["ALPACA_API_KEY"],
                    secret_key=env["ALPACA_SECRET_KEY"],
                    base_url=env.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
                )
            )
            manager.register_provider(
                "/live/orders",
                AlpacaOrdersProvider(
                    store,
                    api_key=env["ALPACA_API_KEY"],
                    secret_key=env["ALPACA_SECRET_KEY"],
                    base_url=env.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
                )
            )
    
    return manager.refresh_all()
