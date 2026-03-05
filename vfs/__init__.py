"""
AI Virtual Filesystem (VFS)

让 AI Bot 通过文件路径读写结构化知识。
配置驱动，支持自定义 provider 和权限规则。
"""

__version__ = "0.5.0"

from .node import VFSNode
from .graph import KVGraph
from .store import VFSStore
from .config import VFSConfig, ProviderSpec, PermissionRule, load_config
from .core import VFS, register_provider_type
from .retrieval import Retriever, DocumentSynthesizer, RetrievalResult
from .multi_agent import (
    AgentConfig, AgentRegistry, AgentRole, AgentQuota,
    NamespacePermissions, AuditLog, VersionedMemory, QuotaEnforcer
)

__all__ = [
    # Core
    "VFS",
    "VFSConfig",
    "VFSStore",
    "VFSNode",
    "KVGraph",
    # Config
    "ProviderSpec",
    "PermissionRule",
    "load_config",
    "register_provider_type",
    # Retrieval
    "Retriever",
    "DocumentSynthesizer",
    "RetrievalResult",
    # Multi-Agent
    "AgentConfig",
    "AgentRegistry",
    "AgentRole",
    "AgentQuota",
    "NamespacePermissions",
    "AuditLog",
    "VersionedMemory",
    "QuotaEnforcer",
]
