"""
AI Virtual Filesystem (VFS)

让 AI Bot 通过文件路径读写结构化知识。
"""

__version__ = "0.1.0"

from .node import VFSNode
from .graph import KVGraph
from .provider import VFSProvider, LiveProvider, StaticProvider
from .store import VFSStore

__all__ = [
    "VFSNode",
    "KVGraph", 
    "VFSProvider",
    "LiveProvider",
    "StaticProvider",
    "VFSStore",
]
