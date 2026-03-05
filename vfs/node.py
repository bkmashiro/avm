"""
vfs/node.py - VFS节点数据结构
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
import hashlib
import json


class NodeType(Enum):
    """节点类型"""
    FILE = "file"
    DIRECTORY = "dir"
    LINK = "link"  # 软链接


class Permission(Enum):
    """权限"""
    READ_ONLY = "ro"
    READ_WRITE = "rw"


@dataclass
class VFSNode:
    """
    VFS节点
    
    每个节点有：
    - path: 虚拟路径 (e.g., /research/MSFT.md)
    - content: 文件内容
    - meta: 元数据（TTL、来源、更新时间等）
    - node_type: 文件/目录/链接
    """
    path: str
    content: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    node_type: NodeType = NodeType.FILE
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    version: int = 1
    
    # 权限由路径前缀决定
    WRITABLE_PREFIXES = ("/memory",)
    READONLY_PREFIXES = ("/research", "/live", "/links")
    
    @property
    def is_writable(self) -> bool:
        """检查节点是否可写"""
        for prefix in self.WRITABLE_PREFIXES:
            if self.path.startswith(prefix):
                return True
        return False
    
    @property
    def is_live(self) -> bool:
        """检查是否为实时数据节点"""
        return self.path.startswith("/live")
    
    @property
    def ttl_seconds(self) -> Optional[int]:
        """获取TTL（仅live节点）"""
        return self.meta.get("ttl_seconds") if self.is_live else None
    
    @property
    def is_expired(self) -> bool:
        """检查live节点是否过期"""
        if not self.is_live:
            return False
        ttl = self.ttl_seconds
        if ttl is None:
            return False
        age = (datetime.utcnow() - self.updated_at).total_seconds()
        return age > ttl
    
    @property
    def content_hash(self) -> str:
        """内容哈希（用于diff检测）"""
        return hashlib.sha256(self.content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        """转为字典"""
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
        """从字典创建"""
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
    节点变更记录
    """
    node_path: str
    version: int
    old_hash: Optional[str]
    new_hash: str
    diff_content: str  # unified diff 或完整新内容
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
