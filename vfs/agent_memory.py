"""
vfs/agent_memory.py - Agent Memory System

Token-aware memory retrieval with:
- Agent isolation (private/shared namespaces)
- Importance/recency/relevance scoring
- Token budget control
- Compact synthesis
- Multi-agent support with permissions
- Append-only versioning
"""

import re
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum

from .store import VFSStore
from .node import VFSNode
from .core import VFS
from .retrieval import Retriever
from .embedding import EmbeddingStore


class ScoringStrategy(Enum):
    IMPORTANCE = "importance"
    RECENCY = "recency"
    RELEVANCE = "relevance"
    BALANCED = "balanced"


@dataclass
class MemoryConfig:
    """Agent Memory 配置"""
    default_max_tokens: int = 4000
    default_strategy: ScoringStrategy = ScoringStrategy.BALANCED
    
    # 评分权重 (balanced 策略)
    importance_weight: float = 0.3
    recency_weight: float = 0.2
    relevance_weight: float = 0.5
    
    # 压缩设置
    max_chars_per_node: int = 300
    include_path: bool = True
    include_metadata: bool = False
    
    # Token 估算
    chars_per_token: float = 4.0  # 粗略估算
    
    @classmethod
    def from_dict(cls, data: Dict) -> "MemoryConfig":
        return cls(
            default_max_tokens=data.get("default_max_tokens", 4000),
            default_strategy=ScoringStrategy(data.get("default_strategy", "balanced")),
            importance_weight=data.get("scoring_weights", {}).get("importance", 0.3),
            recency_weight=data.get("scoring_weights", {}).get("recency", 0.2),
            relevance_weight=data.get("scoring_weights", {}).get("relevance", 0.5),
            max_chars_per_node=data.get("compression", {}).get("max_chars_per_node", 300),
        )


@dataclass
class ScoredNode:
    """带评分的节点"""
    node: VFSNode
    relevance_score: float = 0.0
    importance_score: float = 0.5
    recency_score: float = 0.5
    final_score: float = 0.0
    estimated_tokens: int = 0
    summary: str = ""


class AgentMemory:
    """
    Agent Memory System
    
    提供 token-aware 的记忆检索和管理
    支持多 agent 权限控制和 append-only 版本
    """
    
    def __init__(self, vfs: VFS, agent_id: str, 
                 config: MemoryConfig = None):
        """
        Args:
            vfs: VFS 实例
            agent_id: Agent 标识
            config: 配置
        """
        self.vfs = vfs
        self.agent_id = agent_id
        self.config = config or MemoryConfig()
        
        # 路径前缀
        self.private_prefix = f"/memory/private/{agent_id}"
        self.shared_prefix = "/memory/shared"
        
        # 获取 agent 配置（权限、配额）
        self._agent_config = vfs.get_agent_config(agent_id)
    
    # ─── 检索 ─────────────────────────────────────────────
    
    def recall(self, query: str,
               max_tokens: int = None,
               strategy: ScoringStrategy = None,
               include_shared: bool = True,
               namespaces: List[str] = None,
               merge_versions: bool = True) -> str:
        """
        检索相关记忆，返回 token 可控的上下文
        
        Args:
            query: 查询文本
            max_tokens: 最大 token 数
            strategy: 评分策略
            include_shared: 是否包含共享记忆
            namespaces: 限定的共享命名空间
            merge_versions: 是否合并同一路径的多版本
        
        Returns:
            紧凑的 Markdown 格式上下文
        """
        max_tokens = max_tokens or self.config.default_max_tokens
        strategy = strategy or self.config.default_strategy
        
        # 1. 确定搜索范围
        prefixes = [self.private_prefix]
        if include_shared:
            if namespaces:
                prefixes.extend([f"{self.shared_prefix}/{ns}" for ns in namespaces])
            else:
                prefixes.append(self.shared_prefix)
        
        # 2. 检索候选节点
        candidates = self._retrieve_candidates(query, prefixes, k=50)
        
        # 3. 权限过滤
        candidates = [(n, s) for n, s in candidates if self._can_read(n.path)]
        
        # 4. 评分
        scored = self._score_nodes(candidates, query, strategy)
        
        # 5. 在 token 预算内选择
        selected = self._select_within_budget(scored, max_tokens)
        
        # 6. 版本合并（如果启用）
        if merge_versions and hasattr(self.vfs, '_versioned_memory'):
            selected = self._merge_versions_in_results(selected)
        
        # 7. 生成紧凑输出
        return self._compact_synthesis(selected, query, max_tokens, strategy)
    
    def _merge_versions_in_results(self, scored: List[ScoredNode]) -> List[ScoredNode]:
        """合并同一 base_path 的多版本"""
        # 按 base_path 分组
        by_base: Dict[str, List[ScoredNode]] = {}
        no_base: List[ScoredNode] = []
        
        for sn in scored:
            base = sn.node.meta.get("base_path")
            if base:
                if base not in by_base:
                    by_base[base] = []
                by_base[base].append(sn)
            else:
                no_base.append(sn)
        
        # 合并每组
        merged = []
        for base_path, versions in by_base.items():
            if len(versions) == 1:
                merged.append(versions[0])
            else:
                # 合并多版本
                merged_content = self.vfs._versioned_memory.merge_versions(
                    [sn.node for sn in versions]
                )
                # 使用最高分的节点作为代表
                best = max(versions, key=lambda x: x.final_score)
                best.summary = self._extract_summary(
                    VFSNode(path=base_path, content=merged_content)
                )
                merged.append(best)
        
        return merged + no_base
    
    def _retrieve_candidates(self, query: str, 
                            prefixes: List[str],
                            k: int = 50) -> List[Tuple[VFSNode, float]]:
        """检索候选节点"""
        candidates = []
        seen = set()
        
        # 使用 VFS 的检索功能（一次检索）
        result = self.vfs.retrieve(query, k=k)
        
        for node in result.nodes:
            # 检查是否在允许的前缀下
            if any(node.path.startswith(p) for p in prefixes):
                if node.path not in seen:
                    seen.add(node.path)
                    score = result.scores.get(node.path, 0.0)
                    candidates.append((node, score))
        
        return candidates
    
    def _score_nodes(self, candidates: List[Tuple[VFSNode, float]],
                     query: str,
                     strategy: ScoringStrategy) -> List[ScoredNode]:
        """为节点评分"""
        scored = []
        now = datetime.utcnow()
        
        for node, relevance in candidates:
            sn = ScoredNode(node=node, relevance_score=relevance)
            
            # Importance score (from metadata)
            sn.importance_score = node.meta.get("importance", 0.5)
            
            # Recency score (exponential decay)
            age_hours = (now - node.updated_at).total_seconds() / 3600
            sn.recency_score = math.exp(-age_hours / 168)  # 半衰期 1 周
            
            # 计算最终分数
            if strategy == ScoringStrategy.IMPORTANCE:
                sn.final_score = sn.importance_score
            elif strategy == ScoringStrategy.RECENCY:
                sn.final_score = sn.recency_score
            elif strategy == ScoringStrategy.RELEVANCE:
                sn.final_score = sn.relevance_score
            else:  # BALANCED
                sn.final_score = (
                    self.config.importance_weight * sn.importance_score +
                    self.config.recency_weight * sn.recency_score +
                    self.config.relevance_weight * sn.relevance_score
                )
            
            # 生成摘要并估算 token
            sn.summary = self._extract_summary(node)
            sn.estimated_tokens = self._estimate_tokens(sn.summary)
            
            scored.append(sn)
        
        # 按分数排序
        scored.sort(key=lambda x: x.final_score, reverse=True)
        return scored
    
    def _select_within_budget(self, scored: List[ScoredNode],
                              max_tokens: int) -> List[ScoredNode]:
        """在 token 预算内选择节点"""
        selected = []
        used_tokens = 100  # 预留 header
        
        for sn in scored:
            if used_tokens + sn.estimated_tokens <= max_tokens:
                selected.append(sn)
                used_tokens += sn.estimated_tokens
            
            # 至少保留一个
            if not selected and sn == scored[0]:
                selected.append(sn)
                break
        
        return selected
    
    def _extract_summary(self, node: VFSNode) -> str:
        """提取节点摘要"""
        content = node.content
        max_chars = self.config.max_chars_per_node
        
        # 移除 Markdown 格式
        # 移除标题
        content = re.sub(r'^#+\s+.*$', '', content, flags=re.MULTILINE)
        # 移除更新时间
        content = re.sub(r'\*Updated:.*\*', '', content)
        # 移除空行
        content = re.sub(r'\n{2,}', '\n', content)
        
        # 提取关键行
        lines = [l.strip() for l in content.split('\n') if l.strip()]
        
        # 优先保留带数字的行（可能是关键数据）
        key_lines = [l for l in lines if re.search(r'\d', l)]
        other_lines = [l for l in lines if l not in key_lines]
        
        # 组合
        result_lines = key_lines[:3] + other_lines
        result = ' '.join(result_lines)
        
        if len(result) > max_chars:
            result = result[:max_chars-3] + "..."
        
        return result
    
    def _estimate_tokens(self, text: str) -> int:
        """估算 token 数"""
        return int(len(text) / self.config.chars_per_token) + 10  # +10 for formatting
    
    def _compact_synthesis(self, selected: List[ScoredNode],
                          query: str,
                          max_tokens: int,
                          strategy: ScoringStrategy) -> str:
        """生成紧凑的 Markdown 输出"""
        if not selected:
            return f"## Memory Recall\n\nNo relevant memories found for: \"{query}\""
        
        total_tokens = sum(sn.estimated_tokens for sn in selected)
        
        lines = [
            f"## Relevant Memory ({len(selected)} items, ~{total_tokens} tokens)",
            "",
        ]
        
        for sn in selected:
            # 格式: [path] summary
            score_str = f"{sn.final_score:.2f}"
            lines.append(f"[{sn.node.path}] ({score_str}) {sn.summary}")
            lines.append("")
        
        lines.append("---")
        lines.append(f"*Tokens: ~{total_tokens}/{max_tokens} | Strategy: {strategy.value} | Query: \"{query}\"*")
        
        return "\n".join(lines)
    
    # ─── 写入 ─────────────────────────────────────────────
    
    def remember(self, content: str,
                 title: str = None,
                 importance: float = 0.5,
                 tags: List[str] = None,
                 source: str = "agent",
                 namespace: str = None,
                 path: str = None) -> VFSNode:
        """
        写入记忆（支持 append-only 版本）
        
        Args:
            content: 记忆内容
            title: 标题（用于生成路径）
            importance: 重要性 (0-1)
            tags: 标签
            source: 来源
            namespace: 共享命名空间（如 "market", "projects"）
            path: 指定路径（用于 append-only 更新）
        """
        # 确定目标路径
        if path:
            target_path = path
        elif namespace:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            slug = self._make_slug(title) if title else timestamp
            target_path = f"{self.shared_prefix}/{namespace}/{slug}.md"
        else:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            slug = self._make_slug(title) if title else ""
            filename = f"{timestamp}_{slug}.md" if slug else f"{timestamp}.md"
            target_path = f"{self.private_prefix}/{filename}"
        
        # 检查写权限
        if not self._can_write(target_path):
            raise PermissionError(f"Agent {self.agent_id} cannot write to {target_path}")
        
        # 检查配额
        self._check_quota()
        
        # 格式化内容
        full_content = self._format_content(content, title, tags)
        
        meta = {
            "importance": importance,
            "tags": tags or [],
            "source": source,
            "author": self.agent_id,
        }
        
        # 使用版本化写入（如果是更新现有路径）
        if path and hasattr(self.vfs, '_versioned_memory'):
            node = self.vfs._versioned_memory.write_version(
                path, full_content, self.agent_id, meta
            )
        else:
            node = self.vfs.write(target_path, full_content, meta)
        
        # 记录审计日志
        self._log_operation("write", node.path)
        
        return node
    
    def _make_slug(self, title: str) -> str:
        """生成 URL-safe slug"""
        if not title:
            return ""
        slug = re.sub(r'[^\w\s-]', '', title.lower())
        slug = re.sub(r'[\s_]+', '_', slug)
        return slug[:30]
    
    def _format_content(self, content: str, title: str = None, 
                        tags: List[str] = None) -> str:
        """格式化记忆内容"""
        lines = []
        if title:
            lines.append(f"# {title}")
            lines.append("")
        lines.append(f"*Created: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*")
        if tags:
            lines.append(f"*Tags: {', '.join(tags)}*")
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append(content)
        return "\n".join(lines)
    
    def _can_write(self, path: str) -> bool:
        """检查写权限"""
        if self._agent_config:
            return self._agent_config.namespaces.can_write(path)
        # 默认：只能写私有空间
        return path.startswith(self.private_prefix)
    
    def _can_read(self, path: str) -> bool:
        """检查读权限"""
        if self._agent_config:
            return self._agent_config.namespaces.can_read(path)
        # 默认：能读私有和共享
        return path.startswith(self.private_prefix) or path.startswith(self.shared_prefix)
    
    def _check_quota(self):
        """检查配额"""
        if hasattr(self.vfs, '_agent_registry') and self._agent_config:
            from .multi_agent import QuotaEnforcer
            enforcer = QuotaEnforcer(self.vfs.store)
            result = enforcer.check_quota(self.agent_id, self._agent_config.quota)
            if not result["ok"]:
                raise RuntimeError(f"Quota exceeded: {result['message']}")
    
    def _log_operation(self, operation: str, path: str, details: Dict = None):
        """记录审计日志"""
        if hasattr(self.vfs, '_audit_log'):
            self.vfs._audit_log.log(self.agent_id, operation, path, details)
    
    def share(self, path: str, namespace: str,
              new_name: str = None) -> VFSNode:
        """
        分享记忆到共享空间
        
        Args:
            path: 原始路径（私有记忆）
            namespace: 目标命名空间
            new_name: 新文件名（可选）
        """
        # 读取原始节点
        node = self.vfs.read(path)
        if not node:
            raise ValueError(f"Node not found: {path}")
        
        # 生成新路径
        if new_name:
            new_path = f"{self.shared_prefix}/{namespace}/{new_name}"
        else:
            filename = path.split("/")[-1]
            new_path = f"{self.shared_prefix}/{namespace}/{filename}"
        
        # 更新元数据
        meta = node.meta.copy()
        meta["shared_from"] = path
        meta["shared_by"] = self.agent_id
        meta["shared_at"] = datetime.utcnow().isoformat()
        
        return self.vfs.write(new_path, node.content, meta)
    
    # ─── 更新 ─────────────────────────────────────────────
    
    def update_importance(self, path: str, importance: float):
        """更新记忆的重要性"""
        node = self.vfs.read(path)
        if not node:
            raise ValueError(f"Node not found: {path}")
        
        # 检查权限
        if not path.startswith(self.private_prefix):
            if node.meta.get("agent") != self.agent_id:
                raise PermissionError(f"Cannot modify: {path}")
        
        meta = node.meta.copy()
        meta["importance"] = max(0.0, min(1.0, importance))
        
        return self.vfs.write(path, node.content, meta)
    
    def mark_accessed(self, path: str):
        """标记记忆被访问（用于 recency 计算）"""
        node = self.vfs.read(path)
        if node:
            meta = node.meta.copy()
            meta["last_accessed"] = datetime.utcnow().isoformat()
            # 不更新内容，只更新 meta
            self.vfs.store._put_node_internal(
                VFSNode(path=path, content=node.content, meta=meta),
                save_diff=False
            )
    
    # ─── 列表 ─────────────────────────────────────────────
    
    def list_private(self, limit: int = 100) -> List[VFSNode]:
        """列出私有记忆"""
        return self.vfs.list(self.private_prefix, limit)
    
    def list_shared(self, namespace: str = None, 
                    limit: int = 100) -> List[VFSNode]:
        """列出共享记忆"""
        prefix = f"{self.shared_prefix}/{namespace}" if namespace else self.shared_prefix
        return self.vfs.list(prefix, limit)
    
    def stats(self) -> Dict[str, Any]:
        """统计信息"""
        private = self.list_private()
        shared = self.list_shared()
        
        return {
            "agent_id": self.agent_id,
            "private_count": len(private),
            "shared_accessible": len(shared),
            "private_prefix": self.private_prefix,
            "config": {
                "max_tokens": self.config.default_max_tokens,
                "strategy": self.config.default_strategy.value,
            }
        }
