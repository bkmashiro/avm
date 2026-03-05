"""
avm/providers/memory.py - Bot memory provider
"""

from typing import Dict, Optional

from .base import AVMProvider
from ..node import AVMNode, NodeType
from ..store import AVMStore


class MemoryProvider(AVMProvider):
    """
    Bot memory zone
    
    path: /memory/*
    Read/write enabled
    
    usage:
        - Bot's own observations and learnings
        - Trading experience lessons
        - userpreferencerecord
    """
    
    def __init__(self, store: AVMStore):
        super().__init__(store, "/memory")
    
    def fetch(self, path: str) -> Optional[AVMNode]:
        """Memory zone reads directly from store, no external fetch required"""
        return self.store.get_node(path)
    
    def write(self, path: str, content: str, meta: Dict = None) -> AVMNode:
        """writememory"""
        if not path.startswith("/memory"):
            raise PermissionError(f"Cannot write to {path}")
        
        node = AVMNode(
            path=path,
            content=content,
            meta=meta or {},
            node_type=NodeType.FILE,
        )
        
        return self.store.put_node(node)
    
    def append(self, path: str, content: str) -> AVMNode:
        """Append content to existing node"""
        existing = self.store.get_node(path)
        
        if existing:
            new_content = existing.content + "\n" + content
        else:
            new_content = content
        
        return self.write(path, new_content, existing.meta if existing else None)
    
    def create_lesson(self, title: str, content: str, 
                      tags: list = None) -> AVMNode:
        """Create an experience lesson"""
        from datetime import datetime
        
        # generatepath
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        slug = title.lower().replace(" ", "_")[:30]
        path = f"/memory/lessons/{timestamp}_{slug}.md"
        
        # Format content
        full_content = f"# {title}\n\n"
        full_content += f"*Created: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*\n\n"
        
        if tags:
            full_content += f"**Tags:** {', '.join(tags)}\n\n"
        
        full_content += "---\n\n"
        full_content += content
        
        return self.write(path, full_content, {"tags": tags or [], "title": title})
    
    def create_observation(self, symbol: str, observation: str,
                           category: str = "general") -> AVMNode:
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
