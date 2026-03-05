"""
vfs/providers/memory.py - Bot memory区 Provider
"""

from typing import Dict, Optional

from .base import VFSProvider
from ..node import VFSNode, NodeType
from ..store import VFSStore


class MemoryProvider(VFSProvider):
    """
    Bot memory区
    
    path: /memory/*
    可读写
    
    usage:
        - Bot 自己的observation和学习
        - 交易experiencelesson
        - 用户preferencerecord
    """
    
    def __init__(self, store: VFSStore):
        super().__init__(store, "/memory")
    
    def fetch(self, path: str) -> Optional[VFSNode]:
        """Memory 区directlyfrom store read，不requiresexternal fetch"""
        return self.store.get_node(path)
    
    def write(self, path: str, content: str, meta: Dict = None) -> VFSNode:
        """writememory"""
        if not path.startswith("/memory"):
            raise PermissionError(f"Cannot write to {path}")
        
        node = VFSNode(
            path=path,
            content=content,
            meta=meta or {},
            node_type=NodeType.FILE,
        )
        
        return self.store.put_node(node)
    
    def append(self, path: str, content: str) -> VFSNode:
        """appendcontentto现hasnode"""
        existing = self.store.get_node(path)
        
        if existing:
            new_content = existing.content + "\n" + content
        else:
            new_content = content
        
        return self.write(path, new_content, existing.meta if existing else None)
    
    def create_lesson(self, title: str, content: str, 
                      tags: list = None) -> VFSNode:
        """create一条experiencelesson"""
        from datetime import datetime
        
        # generatepath
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        slug = title.lower().replace(" ", "_")[:30]
        path = f"/memory/lessons/{timestamp}_{slug}.md"
        
        # format化content
        full_content = f"# {title}\n\n"
        full_content += f"*Created: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*\n\n"
        
        if tags:
            full_content += f"**Tags:** {', '.join(tags)}\n\n"
        
        full_content += "---\n\n"
        full_content += content
        
        return self.write(path, full_content, {"tags": tags or [], "title": title})
    
    def create_observation(self, symbol: str, observation: str,
                           category: str = "general") -> VFSNode:
        """createmarketobservationrecord"""
        from datetime import datetime
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = f"/memory/observations/{symbol}/{timestamp}.md"
        
        content = f"# {symbol} Observation\n\n"
        content += f"*Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*\n"
        content += f"*Category: {category}*\n\n"
        content += "---\n\n"
        content += observation
        
        return self.write(path, content, {
            "symbol": symbol, 
            "category": category,
            "timestamp": timestamp,
        })
