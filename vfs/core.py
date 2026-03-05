"""
vfs/core.py - VFS 核心类

Config-driven virtual filesystem
"""

from typing import Dict, List, Optional, Type, Callable, Any, Tuple
from pathlib import Path

from .config import VFSConfig, ProviderSpec, load_config
from .store import VFSStore
from .node import VFSNode, NodeType
from .graph import EdgeType


class ProviderRegistry:
    """
    Provider registertable
    
    管理 provider type name -> provider class 的mapping
    """
    
    def __init__(self):
        self._types: Dict[str, Type] = {}
        self._factories: Dict[str, Callable] = {}
    
    def register(self, name: str, provider_class: Type = None, 
                 factory: Callable = None):
        """
        register provider type
        
        Args:
            name: type名称
            provider_class: Provider 类
            factory: 工厂function (store, spec) -> Provider
        """
        if provider_class:
            self._types[name] = provider_class
        if factory:
            self._factories[name] = factory
    
    def create(self, name: str, store: VFSStore, 
               spec: ProviderSpec) -> Optional[Any]:
        """create provider instance"""
        if name in self._factories:
            return self._factories[name](store, spec)
        
        if name in self._types:
            cls = self._types[name]
            return cls(store, spec.pattern, spec.ttl, **spec.config)
        
        return None
    
    def list_types(self) -> List[str]:
        """listallalreadyregister的type"""
        return list(set(self._types.keys()) | set(self._factories.keys()))


# 全局registertable
_registry = ProviderRegistry()


def register_provider_type(name: str, provider_class: Type = None,
                           factory: Callable = None):
    """register provider type（全局）"""
    _registry.register(name, provider_class, factory)


class VFS:
    """
    虚拟file系统
    
    Config-driven, supports:
    - 动态 provider register
    - Configurable permission rules
    - TTL cache
    - relationgraph
    """
    
    def __init__(self, config: VFSConfig = None, config_path: str = None):
        """
        Args:
            config: VFSConfig instance
            config_path: Configuration file path
        """
        if config:
            self.config = config
        else:
            self.config = load_config(config_path)
        
        # initializestorage
        db_path = self.config.db_path or None
        self.store = VFSStore(db_path)
        
        # Provider instancecache
        self._providers: Dict[str, Any] = {}
        
        # use全局registertable
        self._registry = _registry
        
        # registerbuilt-in provider type
        self._register_builtin_providers()
    
    def _register_builtin_providers(self):
        """registerbuilt-in provider"""
        from .providers import (
            AlpacaPositionsProvider, AlpacaOrdersProvider,
            TechnicalIndicatorsProvider, NewsProvider,
            WatchlistProvider, MemoryProvider,
        )
        
        # Alpaca (requires config)
        def create_alpaca_positions(store, spec):
            config = spec.config
            if not config.get("api_key"):
                # tryfrom env_file load
                env_file = config.get("env_file", "")
                if env_file:
                    env_path = Path(env_file).expanduser()
                    if env_path.exists():
                        env = dict(
                            line.split("=", 1)
                            for line in env_path.read_text().splitlines()
                            if "=" in line and not line.startswith("#")
                        )
                        config = {**config, **{
                            "api_key": env.get("ALPACA_API_KEY", ""),
                            "secret_key": env.get("ALPACA_SECRET_KEY", ""),
                            "base_url": env.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
                        }}
            
            return AlpacaPositionsProvider(
                store,
                api_key=config.get("api_key", ""),
                secret_key=config.get("secret_key", ""),
                base_url=config.get("base_url", "https://paper-api.alpaca.markets"),
                ttl_seconds=spec.ttl or 60,
            )
        
        def create_alpaca_orders(store, spec):
            config = spec.config
            env_file = config.get("env_file", "")
            if env_file and not config.get("api_key"):
                env_path = Path(env_file).expanduser()
                if env_path.exists():
                    env = dict(
                        line.split("=", 1)
                        for line in env_path.read_text().splitlines()
                        if "=" in line and not line.startswith("#")
                    )
                    config = {**config, **{
                        "api_key": env.get("ALPACA_API_KEY", ""),
                        "secret_key": env.get("ALPACA_SECRET_KEY", ""),
                        "base_url": env.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
                    }}
            
            return AlpacaOrdersProvider(
                store,
                api_key=config.get("api_key", ""),
                secret_key=config.get("secret_key", ""),
                base_url=config.get("base_url", "https://paper-api.alpaca.markets"),
                ttl_seconds=spec.ttl or 30,
            )
        
        self._registry.register("alpaca_positions", factory=create_alpaca_positions)
        self._registry.register("alpaca_orders", factory=create_alpaca_orders)
        
        # Providers that need no config
        self._registry.register("technical_indicators", TechnicalIndicatorsProvider)
        self._registry.register("news", NewsProvider)
        self._registry.register("watchlist", WatchlistProvider)
        self._registry.register("memory", MemoryProvider)
    
    def register_provider_type(self, name: str, provider_class: Type = None,
                               factory: Callable = None):
        """registercustom provider type"""
        self._registry.register(name, provider_class, factory)
    
    def _get_provider(self, path: str) -> Optional[Any]:
        """Get or create provider for path"""
        spec = self.config.get_provider_spec(path)
        if not spec:
            return None
        
        # cache key
        cache_key = f"{spec.type}:{spec.pattern}"
        
        if cache_key not in self._providers:
            provider = self._registry.create(spec.type, self.store, spec)
            if provider:
                self._providers[cache_key] = provider
        
        return self._providers.get(cache_key)
    
    # ─── 读写interface ─────────────────────────────────────────
    
    def read(self, path: str, force_refresh: bool = False) -> Optional[VFSNode]:
        """
        readnode
        
        1. check读permission
        2. 查找 provider
        3. Fetch via provider (with TTL cache)
        4. 或directlyfrom store read
        """
        if not self.config.check_permission(path, "read"):
            raise PermissionError(f"No read permission for {path}")
        
        provider = self._get_provider(path)
        if provider:
            return provider.get(path, force_refresh=force_refresh)
        
        return self.store.get_node(path)
    
    def write(self, path: str, content: str, 
              meta: Dict = None) -> VFSNode:
        """
        writenode
        
        1. check写permission
        2. create或updatenode
        """
        if not self.config.check_permission(path, "write"):
            raise PermissionError(f"No write permission for {path}")
        
        node = VFSNode(
            path=path,
            content=content,
            meta=meta or {},
            node_type=NodeType.FILE,
        )
        
        return self.store.put_node(node)
    
    def delete(self, path: str) -> bool:
        """deletenode"""
        if not self.config.check_permission(path, "write"):
            raise PermissionError(f"No write permission for {path}")
        
        return self.store.delete_node(path)
    
    def list(self, prefix: str = "/", limit: int = 100) -> List[VFSNode]:
        """listnode"""
        return self.store.list_nodes(prefix, limit)
    
    # ─── search ─────────────────────────────────────────────
    
    def search(self, query: str, limit: int = 10) -> List[Tuple[VFSNode, float]]:
        """full-textsearch"""
        return self.store.search(query, limit)
    
    # ─── relationgraph ─────────────────────────────────────────────
    
    def link(self, source: str, target: str,
             edge_type: EdgeType = EdgeType.RELATED,
             weight: float = 1.0):
        """addrelation"""
        return self.store.add_edge(source, target, edge_type, weight)
    
    def links(self, path: str, direction: str = "both") -> List:
        """Get relations"""
        return self.store.get_links(path, direction)
    
    # ─── history ─────────────────────────────────────────────
    
    def history(self, path: str, limit: int = 10):
        """Get change history"""
        return self.store.get_history(path, limit)
    
    # ─── statistics ─────────────────────────────────────────────
    
    def stats(self) -> Dict:
        """storagestatistics"""
        return self.store.stats()
    
    # ─── 联动retrieve ─────────────────────────────────────────
    
    def retrieve(self, query: str, k: int = 5,
                 expand_graph: bool = True,
                 graph_depth: int = 1) -> "RetrievalResult":
        """
        联动retrieve
        
        1. semanticsearch (ifhas embedding)
        2. FTS5 full-textsearch
        3. graphextend
        """
        from .retrieval import Retriever, RetrievalResult
        
        # Get or create embedding store
        embedding_store = getattr(self, '_embedding_store', None)
        
        retriever = Retriever(self.store, embedding_store)
        return retriever.retrieve(
            query, k=k,
            expand_graph=expand_graph,
            graph_depth=graph_depth
        )
    
    def synthesize(self, query: str, k: int = 5,
                   title: str = None) -> str:
        """
        动态generate综合document
        
        一linecall:
            vfs.synthesize("NVDA风险analysis")
        
        Returns: Markdown format的综合document
        """
        from .retrieval import Retriever, DocumentSynthesizer
        
        embedding_store = getattr(self, '_embedding_store', None)
        retriever = Retriever(self.store, embedding_store)
        synthesizer = DocumentSynthesizer(self.store)
        
        result = retriever.retrieve(query, k=k, expand_graph=True)
        doc = synthesizer.synthesize(result, title=title)
        
        return doc.to_markdown()
    
    def enable_embedding(self, backend: "EmbeddingBackend" = None,
                         model: str = "text-embedding-3-small"):
        """
        enablesemanticsearch
        
        Args:
            backend: custom embedding 后端
            model: OpenAI 模型名称（if不提供 backend）
        """
        from .embedding import EmbeddingStore, OpenAIEmbedding
        
        if backend is None:
            backend = OpenAIEmbedding(model=model)
        
        self._embedding_store = EmbeddingStore(self.store, backend)
        return self._embedding_store
    
    def embed_all(self, prefix: str = "/") -> int:
        """allnodegenerate embedding"""
        if not hasattr(self, '_embedding_store'):
            raise RuntimeError("Call enable_embedding() first")
        
        return self._embedding_store.embed_all(prefix)
    
    # ─── Agent Memory ─────────────────────────────────────
    
    def agent_memory(self, agent_id: str, 
                     config: Dict = None) -> "AgentMemory":
        """
        Get Agent Memory instance
        
        Args:
            agent_id: Agent 标识
            config: Optional configuration
        
        Returns:
            AgentMemory instance
        """
        from .agent_memory import AgentMemory, MemoryConfig
        
        mem_config = None
        if config:
            mem_config = MemoryConfig.from_dict(config)
        
        return AgentMemory(self, agent_id, mem_config)
    
    # ─── Multi-Agent ─────────────────────────────────────
    
    def load_agents(self, config_path: str = None, config_dict: Dict = None):
        """
        Load multi-agent configuration
        
        Args:
            config_path: YAML Configuration file path
            config_dict: Configuration dictionary
        """
        from .multi_agent import AgentRegistry, AuditLog, VersionedMemory
        
        self._agent_registry = AgentRegistry()
        self._audit_log = AuditLog(self.store)
        self._versioned_memory = VersionedMemory(self.store)
        
        if config_path:
            import yaml
            with open(config_path) as f:
                config_dict = yaml.safe_load(f)
        
        if config_dict:
            self._agent_registry.load_from_dict(config_dict)
    
    def get_agent_config(self, agent_id: str):
        """Get agent configuration"""
        if not hasattr(self, '_agent_registry'):
            from .multi_agent import AgentRegistry
            self._agent_registry = AgentRegistry()
        
        return self._agent_registry.get(agent_id)
    
    def audit_log(self, agent_id: str = None, path_prefix: str = None,
                  limit: int = 100) -> List[Dict]:
        """queryauditlog"""
        if not hasattr(self, '_audit_log'):
            from .multi_agent import AuditLog
            self._audit_log = AuditLog(self.store)
        
        return self._audit_log.query(agent_id, path_prefix, limit=limit)
    
    # ─── 高级features ─────────────────────────────────────────
    
    def subscribe(self, pattern: str, callback) -> str:
        """subscribepath变化"""
        from .advanced import SubscriptionManager
        
        if not hasattr(self, '_subscription_manager'):
            self._subscription_manager = SubscriptionManager()
        
        return self._subscription_manager.subscribe(pattern, callback)
    
    def _notify_subscribers(self, path: str, event_type: str, agent_id: str = None):
        """notifysubscribe者（internalmethod）"""
        if hasattr(self, '_subscription_manager'):
            from .advanced import MemoryEvent, EventType
            
            event = MemoryEvent(
                event_type=EventType(event_type),
                path=path,
                agent_id=agent_id or "system",
            )
            self._subscription_manager.notify(event)
    
    def query_time(self, prefix: str = "/memory",
                   time_range: str = None,
                   after: str = None,
                   before: str = None,
                   limit: int = 100) -> List[VFSNode]:
        """timerangequery"""
        from .advanced import TimeQuery
        from datetime import datetime
        
        query = TimeQuery(self.store)
        
        after_dt = datetime.fromisoformat(after) if after else None
        before_dt = datetime.fromisoformat(before) if before else None
        
        return query.query(
            prefix=prefix,
            after=after_dt,
            before=before_dt,
            time_range=time_range,
            limit=limit
        )
    
    def sync(self, target: str, prefix: str = "/memory") -> Dict[str, int]:
        """
        sync to远程
        
        Args:
            target: directorypath或 s3://bucket/prefix
            prefix: 要sync的pathprefix
        """
        from .advanced import SyncManager
        
        sync_mgr = SyncManager(self.store)
        
        if target.startswith("s3://"):
            # S3 sync
            parts = target[5:].split("/", 1)
            bucket = parts[0]
            s3_prefix = parts[1] if len(parts) > 1 else "vfs/"
            return sync_mgr.sync_to_s3(bucket, s3_prefix, prefix)
        else:
            # Directory sync
            return sync_mgr.sync_to_directory(target, prefix)
    
    def snapshot(self, name: str = None) -> str:
        """createsnapshot"""
        from .advanced import ExportManager
        
        export_mgr = ExportManager(self.store)
        return export_mgr.snapshot(name)
    
    def list_snapshots(self) -> List[Dict]:
        """listsnapshot"""
        from .advanced import ExportManager
        
        export_mgr = ExportManager(self.store)
        return export_mgr.list_snapshots()
    
    def restore_snapshot(self, name: str) -> int:
        """restoresnapshot"""
        from .advanced import ExportManager
        
        export_mgr = ExportManager(self.store)
        return export_mgr.restore_snapshot(name)
    
    # ─── Linux-Style Permissions ──────────────────────────
    
    def init_permissions(self, config_dict: Dict = None):
        """
        initialize Linux 风格permission系统
        
        Args:
            config_dict: User/group configuration
        """
        from .permissions import UserRegistry, PermissionManager, APIKeyManager
        
        self._user_registry = UserRegistry()
        self._perm_manager = PermissionManager(self._user_registry)
        self._api_key_manager = APIKeyManager(self._user_registry)
        
        if config_dict:
            self._user_registry.load_from_dict(config_dict)
    
    def authenticate(self, api_key: str) -> Optional["User"]:
        """
        via API Key auth
        
        Returns:
            User object，或 None
        """
        if not hasattr(self, '_user_registry'):
            self.init_permissions()
        
        return self._user_registry.authenticate(api_key)
    
    def create_user(self, name: str, groups: List[str] = None,
                    capabilities: List[str] = None) -> "User":
        """create用户"""
        if not hasattr(self, '_user_registry'):
            self.init_permissions()
        
        from .permissions import Capability
        caps = [Capability(c) for c in (capabilities or [])]
        
        return self._user_registry.create_user(name, groups, caps)
    
    def get_user(self, name: str) -> Optional["User"]:
        """Get user"""
        if not hasattr(self, '_user_registry'):
            return None
        return self._user_registry.get_user(name)
    
    def check_permission(self, user: "User", path: str, 
                         action: str = "read") -> bool:
        """
        check用户permission
        
        Args:
            user: 用户object
            path: path
            action: read/write/delete/search
        """
        if not hasattr(self, '_perm_manager'):
            return True  # 没hasinitializepermission系统则allow
        
        from .permissions import NodeOwnership
        
        # Get node ownership info
        node = self.store.get_node(path)
        if node:
            ownership = NodeOwnership.from_meta(node.meta)
        else:
            # defaultpermission
            ownership = NodeOwnership(owner="root", group="root", mode=0o644)
        
        if action == "read":
            return self._perm_manager.check_read(user, ownership)
        elif action == "write":
            return self._perm_manager.check_write(user, ownership)
        elif action == "delete":
            return self._perm_manager.check_delete(user, ownership)
        elif action == "search":
            return self._perm_manager.check_search(user, path)
        
        return False
    
    def sudo(self, user: "User", duration_minutes: int = 5) -> bool:
        """temporaryelevate privileges"""
        if not hasattr(self, '_perm_manager'):
            return False
        return self._perm_manager.sudo(user, duration_minutes)
    
    def create_api_key(self, user: "User", 
                       paths: List[str] = None,
                       actions: List[str] = None,
                       expires_days: int = None) -> str:
        """
        create API Key（for skill authentication）
        
        Args:
            user: 用户
            paths: allow的path（supportswildcard）
            actions: allow的操作
            expires_days: expired天数
        """
        if not hasattr(self, '_api_key_manager'):
            self.init_permissions()
        
        from .permissions import APIKeyScope
        
        scope = APIKeyScope(
            paths=paths or ["*"],
            actions=actions or ["read"],
        )
        
        return self._api_key_manager.create_key(user, scope, expires_days)
