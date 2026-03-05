"""
vfs/providers/base.py - Provider base class
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..node import VFSNode, NodeType
from ..store import VFSStore


class VFSProvider(ABC):
    """
    Data provider base class
    """
    
    def __init__(self, store: VFSStore, prefix: str):
        self.store = store
        self.prefix = prefix
    
    @abstractmethod
    def fetch(self, path: str) -> Optional[VFSNode]:
        """Fetch data from source"""
        pass
    
    def get(self, path: str, force_refresh: bool = False) -> Optional[VFSNode]:
        """Get node (with cache)"""
        if not path.startswith(self.prefix):
            return None
        
        cached = self.store.get_node(path)
        
        if cached and not force_refresh:
            if not cached.is_expired:
                return cached
        
        node = self.fetch(path)
        if node:
            self.store._put_node_internal(node, save_diff=True)
        
        return node
    
    def refresh_all(self) -> int:
        """Refresh all nodes"""
        count = 0
        for node in self.store.list_nodes(self.prefix):
            refreshed = self.get(node.path, force_refresh=True)
            if refreshed:
                count += 1
        return count


class LiveProvider(VFSProvider):
    """Live data provider (with TTL)"""
    
    def __init__(self, store: VFSStore, prefix: str, ttl_seconds: int = 300):
        super().__init__(store, prefix)
        self.ttl_seconds = ttl_seconds
    
    def _make_node(self, path: str, content: str, 
                   meta: Dict = None) -> VFSNode:
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
    """Static data provider"""
    
    def _make_node(self, path: str, content: str,
                   meta: Dict = None) -> VFSNode:
        node_meta = meta or {}
        node_meta["provider"] = self.__class__.__name__
        
        return VFSNode(
            path=path,
            content=content,
            meta=node_meta,
            node_type=NodeType.FILE,
        )
