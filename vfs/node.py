"""
vfs/node.py - VFSnodedatastructure
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
import hashlib
import json


class NodeType(Enum):
    """node type"""
    FILE = "file"
    DIRECTORY = "dir"
    LINK = "link"  # 软链接


class Permission(Enum):
    """permission"""
    READ_ONLY = "ro"
    READ_WRITE = "rw"


@dataclass
class VFSNode:
    """
    VFSnode
    
    eachnodehas：
    - path: 虚拟path (e.g., /research/MSFT.md)
    - content: filecontent
    - meta: metadata（TTL、source、updatetime等）
    - node_type: file/directory/链接
    """
    path: str
    content: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    node_type: NodeType = NodeType.FILE
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    version: int = 1
    
    # permission由pathprefix决定
    WRITABLE_PREFIXES = ("/memory",)
    READONLY_PREFIXES = ("/research", "/live", "/links")
    
    @property
    def is_writable(self) -> bool:
        """checknodewhetherwritable"""
        for prefix in self.WRITABLE_PREFIXES:
            if self.path.startswith(prefix):
                return True
        return False
    
    @property
    def is_live(self) -> bool:
        """checkwhetherlive datanode"""
        return self.path.startswith("/live")
    
    @property
    def ttl_seconds(self) -> Optional[int]:
        """getTTL（onlylivenode）"""
        return self.meta.get("ttl_seconds") if self.is_live else None
    
    @property
    def is_expired(self) -> bool:
        """checklivenodewhetherexpired"""
        if not self.is_live:
            return False
        ttl = self.ttl_seconds
        if ttl is None:
            return False
        age = (datetime.utcnow() - self.updated_at).total_seconds()
        return age > ttl
    
    @property
    def content_hash(self) -> str:
        """contenthash（fordiff检测）"""
        return hashlib.sha256(self.content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        """转dict"""
        return {
            "path": self.path,
            "content": self.content,
            "meta": self.meta,
            "node_type": self.node_type.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "version": self.version,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VFSNode":
        """fromdictcreate"""
        return cls(
            path=data["path"],
            content=data.get("content", ""),
            meta=data.get("meta", {}),
            node_type=NodeType(data.get("node_type", "file")),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.utcnow(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.utcnow(),
            version=data.get("version", 1),
        )
    
    def __repr__(self) -> str:
        return f"VFSNode({self.path}, v{self.version}, {len(self.content)} bytes)"


@dataclass
class NodeDiff:
    """
    nodechangerecord
    """
    node_path: str
    version: int
    old_hash: Optional[str]
    new_hash: str
    diff_content: str  # unified diff 或complete新content
    changed_at: datetime = field(default_factory=datetime.utcnow)
    change_type: str = "update"  # create/update/delete
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_path": self.node_path,
            "version": self.version,
            "old_hash": self.old_hash,
            "new_hash": self.new_hash,
            "diff_content": self.diff_content,
            "changed_at": self.changed_at.isoformat(),
            "change_type": self.change_type,
        }
