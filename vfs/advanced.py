"""
vfs/advanced.py - Advanced Memory Features

Features:
- Subscription/Notification system
- Memory decay (weight reduction over time)
- Memory compaction (summarize old versions)
- Semantic deduplication
- Derived links (reasoning chains)
- Time-based queries
"""

import fnmatch
import math
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable, Set, Tuple
from enum import Enum

from .store import VFSStore
from .node import VFSNode
from .graph import EdgeType


# ═══════════════════════════════════════════════════════════════
# Subscription System
# ═══════════════════════════════════════════════════════════════

class EventType(Enum):
    WRITE = "write"
    DELETE = "delete"
    LINK = "link"


@dataclass
class MemoryEvent:
    """Memory change event"""
    event_type: EventType
    path: str
    agent_id: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    data: Dict = field(default_factory=dict)


Callback = Callable[[MemoryEvent], None]


class SubscriptionManager:
    """
    Subscription system for memory changes
    
    Usage:
        sub_mgr = SubscriptionManager()
        
        def on_market_update(event):
            print(f"Market update: {event.path}")
        
        sub_mgr.subscribe("/memory/shared/market/*", on_market_update)
        
        # Later, when write happens:
        sub_mgr.notify(MemoryEvent(
            event_type=EventType.WRITE,
            path="/memory/shared/market/BTC.md",
            agent_id="akashi"
        ))
    """
    
    def __init__(self):
        self._subscriptions: Dict[str, List[Tuple[str, Callback]]] = {}
        # pattern -> [(subscriber_id, callback), ...]
        self._subscriber_count = 0
    
    def subscribe(self, pattern: str, callback: Callback, 
                  subscriber_id: str = None) -> str:
        """
        Subscribe to path pattern
        
        Args:
            pattern: Glob pattern (e.g., "/memory/shared/market/*")
            callback: Function to call on match
            subscriber_id: Optional ID (auto-generated if not provided)
        
        Returns:
            Subscriber ID for unsubscribe
        """
        if subscriber_id is None:
            self._subscriber_count += 1
            subscriber_id = f"sub_{self._subscriber_count}"
        
        if pattern not in self._subscriptions:
            self._subscriptions[pattern] = []
        
        self._subscriptions[pattern].append((subscriber_id, callback))
        return subscriber_id
    
    def unsubscribe(self, subscriber_id: str, pattern: str = None):
        """Unsubscribe by ID"""
        patterns = [pattern] if pattern else list(self._subscriptions.keys())
        
        for p in patterns:
            if p in self._subscriptions:
                self._subscriptions[p] = [
                    (sid, cb) for sid, cb in self._subscriptions[p]
                    if sid != subscriber_id
                ]
    
    def notify(self, event: MemoryEvent):
        """Notify all matching subscribers"""
        for pattern, subscribers in self._subscriptions.items():
            if fnmatch.fnmatch(event.path, pattern):
                for subscriber_id, callback in subscribers:
                    try:
                        callback(event)
                    except Exception as e:
                        # Log but don't crash
                        print(f"Subscription callback error: {e}")
    
    def list_subscriptions(self) -> Dict[str, int]:
        """List patterns and subscriber counts"""
        return {p: len(subs) for p, subs in self._subscriptions.items()}


# ═══════════════════════════════════════════════════════════════
# Memory Decay
# ═══════════════════════════════════════════════════════════════

class MemoryDecay:
    """
    Memory decay system
    
    Reduces effective weight of unaccessed memories over time.
    Does NOT delete - just affects recall ranking.
    """
    
    def __init__(self, store: VFSStore, half_life_days: float = 7.0):
        """
        Args:
            store: VFS store
            half_life_days: Time for weight to halve (default 7 days)
        """
        self.store = store
        self.half_life_days = half_life_days
        self._decay_constant = math.log(2) / (half_life_days * 24 * 3600)
    
    def calculate_decay(self, node: VFSNode, 
                        reference_time: datetime = None) -> float:
        """
        Calculate decay factor for a node
        
        Returns: Factor between 0 and 1 (1 = no decay)
        """
        if reference_time is None:
            reference_time = datetime.utcnow()
        
        # Use last_accessed if available, else updated_at
        last_access = node.meta.get("last_accessed")
        if last_access:
            try:
                last_time = datetime.fromisoformat(last_access)
            except:
                last_time = node.updated_at
        else:
            last_time = node.updated_at
        
        # Calculate time since last access
        delta_seconds = (reference_time - last_time).total_seconds()
        
        # Exponential decay
        decay_factor = math.exp(-self._decay_constant * delta_seconds)
        
        return decay_factor
    
    def apply_decay(self, nodes: List[VFSNode]) -> List[Tuple[VFSNode, float]]:
        """
        Apply decay to list of nodes
        
        Returns: [(node, decayed_weight), ...] sorted by decayed weight
        """
        now = datetime.utcnow()
        
        decayed = []
        for node in nodes:
            base_importance = node.meta.get("importance", 0.5)
            decay = self.calculate_decay(node, now)
            decayed_weight = base_importance * decay
            decayed.append((node, decayed_weight))
        
        # Sort by decayed weight descending
        decayed.sort(key=lambda x: x[1], reverse=True)
        return decayed
    
    def get_cold_memories(self, prefix: str = "/memory",
                          threshold: float = 0.1,
                          limit: int = 100) -> List[VFSNode]:
        """Get memories that have decayed below threshold"""
        nodes = self.store.list_nodes(prefix, limit=1000)
        
        cold = []
        for node in nodes:
            decay = self.calculate_decay(node)
            importance = node.meta.get("importance", 0.5)
            if importance * decay < threshold:
                cold.append(node)
        
        return cold[:limit]


# ═══════════════════════════════════════════════════════════════
# Memory Compaction
# ═══════════════════════════════════════════════════════════════

@dataclass
class CompactionResult:
    """Result of memory compaction"""
    base_path: str
    versions_before: int
    versions_after: int
    summary_path: str
    removed_paths: List[str]


class MemoryCompactor:
    """
    Compacts old versions into summaries
    
    Keeps recent N versions, summarizes older ones.
    """
    
    def __init__(self, store: VFSStore, summarizer: Callable = None):
        """
        Args:
            store: VFS store
            summarizer: Optional function (List[str]) -> str for custom summarization
        """
        self.store = store
        self.summarizer = summarizer or self._default_summarizer
    
    def _default_summarizer(self, contents: List[str]) -> str:
        """Default summarizer: concatenate with markers"""
        summary_parts = []
        for i, content in enumerate(contents):
            # Extract key lines (non-empty, non-header)
            lines = [l.strip() for l in content.split("\n") 
                    if l.strip() and not l.startswith("#") and not l.startswith("*")]
            if lines:
                summary_parts.append(" | ".join(lines[:3]))
        
        return "**Compacted summary:**\n\n" + "\n\n".join(summary_parts)
    
    def compact(self, base_path: str, keep_recent: int = 3) -> CompactionResult:
        """
        Compact versions of a path
        
        Args:
            base_path: Base path to compact
            keep_recent: Number of recent versions to keep
        
        Returns:
            CompactionResult
        """
        # Get all versions
        versions = self._get_versions(base_path)
        
        if len(versions) <= keep_recent:
            return CompactionResult(
                base_path=base_path,
                versions_before=len(versions),
                versions_after=len(versions),
                summary_path="",
                removed_paths=[],
            )
        
        # Sort by date (newest first)
        versions.sort(key=lambda n: n.meta.get("created_at", ""), reverse=True)
        
        # Keep recent, compact old
        to_keep = versions[:keep_recent]
        to_compact = versions[keep_recent:]
        
        # Generate summary
        contents = [n.content for n in to_compact]
        summary = self.summarizer(contents)
        
        # Create summary node
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        base_name = base_path.rsplit(".", 1)[0]
        summary_path = f"{base_name}.summary_{timestamp}.md"
        
        summary_node = VFSNode(
            path=summary_path,
            content=summary,
            meta={
                "type": "compaction_summary",
                "base_path": base_path,
                "compacted_versions": len(to_compact),
                "created_at": datetime.utcnow().isoformat(),
            }
        )
        self.store.put_node(summary_node)
        
        # Remove old versions (optional - could also just mark as compacted)
        removed = []
        for node in to_compact:
            self.store.delete_node(node.path)
            removed.append(node.path)
        
        return CompactionResult(
            base_path=base_path,
            versions_before=len(versions),
            versions_after=len(to_keep) + 1,  # kept + summary
            summary_path=summary_path,
            removed_paths=removed,
        )
    
    def _get_versions(self, base_path: str) -> List[VFSNode]:
        """Get all versions of a path"""
        # Get base
        base = self.store.get_node(base_path)
        versions = [base] if base else []
        
        # Get version links
        edges = self.store.get_links(base_path, direction="in")
        for edge in edges:
            if edge.edge_type.value == "version_of":
                node = self.store.get_node(edge.source)
                if node:
                    versions.append(node)
        
        return versions


# ═══════════════════════════════════════════════════════════════
# Semantic Deduplication
# ═══════════════════════════════════════════════════════════════

@dataclass
class DedupeResult:
    """Result of deduplication check"""
    is_duplicate: bool
    similar_path: Optional[str] = None
    similarity: float = 0.0
    action: str = "write"  # "write" | "skip" | "merge"


class SemanticDeduplicator:
    """
    Checks for semantically similar memories before writing
    
    Uses either:
    - Embedding similarity (if available)
    - Text fingerprinting (fallback)
    """
    
    def __init__(self, store: VFSStore, embedding_store = None):
        self.store = store
        self.embedding_store = embedding_store
    
    def check_duplicate(self, content: str, 
                        prefix: str = "/memory",
                        threshold: float = 0.85) -> DedupeResult:
        """
        Check if content is similar to existing memories
        
        Args:
            content: New content to check
            prefix: Path prefix to search
            threshold: Similarity threshold (0-1)
        
        Returns:
            DedupeResult
        """
        # Try embedding-based similarity first
        if self.embedding_store:
            return self._check_embedding(content, prefix, threshold)
        
        # Fallback to text fingerprinting
        return self._check_fingerprint(content, prefix, threshold)
    
    def _check_embedding(self, content: str, prefix: str, 
                         threshold: float) -> DedupeResult:
        """Check using embedding similarity"""
        results = self.embedding_store.search(content, k=3, prefix=prefix)
        
        for node, similarity in results:
            if similarity >= threshold:
                return DedupeResult(
                    is_duplicate=True,
                    similar_path=node.path,
                    similarity=similarity,
                    action="skip",
                )
        
        return DedupeResult(is_duplicate=False, action="write")
    
    def _check_fingerprint(self, content: str, prefix: str,
                           threshold: float) -> DedupeResult:
        """Check using text fingerprinting (simhash-like)"""
        new_shingles = self._get_shingles(content)
        
        nodes = self.store.list_nodes(prefix, limit=500)
        
        for node in nodes:
            existing_shingles = self._get_shingles(node.content)
            similarity = self._jaccard_similarity(new_shingles, existing_shingles)
            
            if similarity >= threshold:
                return DedupeResult(
                    is_duplicate=True,
                    similar_path=node.path,
                    similarity=similarity,
                    action="skip",
                )
        
        return DedupeResult(is_duplicate=False, action="write")
    
    def _get_shingles(self, text: str, k: int = 3) -> Set[str]:
        """Get k-shingles from text"""
        # Normalize
        text = text.lower()
        words = text.split()
        
        shingles = set()
        for i in range(len(words) - k + 1):
            shingle = " ".join(words[i:i+k])
            shingles.add(shingle)
        
        return shingles
    
    def _jaccard_similarity(self, a: Set[str], b: Set[str]) -> float:
        """Calculate Jaccard similarity"""
        if not a or not b:
            return 0.0
        
        intersection = len(a & b)
        union = len(a | b)
        
        return intersection / union if union > 0 else 0.0


# ═══════════════════════════════════════════════════════════════
# Derived Links (Reasoning Chains)
# ═══════════════════════════════════════════════════════════════

class DerivedLinkManager:
    """
    Manages derived/reasoning chain links
    
    When an agent writes a conclusion, link it to source memories.
    """
    
    def __init__(self, store: VFSStore):
        self.store = store
    
    def link_derived(self, derived_path: str, 
                     source_paths: List[str],
                     reasoning: str = None):
        """
        Link a derived memory to its sources
        
        Args:
            derived_path: Path of the derived/conclusion memory
            source_paths: Paths of source memories
            reasoning: Optional reasoning description
        """
        for source_path in source_paths:
            self.store.add_edge(
                derived_path,
                source_path,
                EdgeType.DERIVED,
                weight=1.0,
                meta={"reasoning": reasoning} if reasoning else {},
            )
    
    def get_derivation_chain(self, path: str, 
                             max_depth: int = 5) -> List[List[str]]:
        """
        Get the derivation chain for a memory
        
        Returns: List of paths from conclusion back to sources
        """
        chains = []
        self._trace_chain(path, [], chains, max_depth)
        return chains
    
    def _trace_chain(self, path: str, current_chain: List[str],
                     all_chains: List[List[str]], max_depth: int):
        """Recursively trace derivation chain"""
        current_chain = current_chain + [path]
        
        if len(current_chain) > max_depth:
            all_chains.append(current_chain)
            return
        
        # Get sources
        edges = self.store.get_links(path, direction="out")
        derived_edges = [e for e in edges if e.edge_type == EdgeType.DERIVED]
        
        if not derived_edges:
            # End of chain
            all_chains.append(current_chain)
            return
        
        for edge in derived_edges:
            self._trace_chain(edge.target, current_chain, all_chains, max_depth)
    
    def get_derived_from(self, source_path: str) -> List[VFSNode]:
        """Get all memories derived from a source"""
        edges = self.store.get_links(source_path, direction="in")
        derived_edges = [e for e in edges if e.edge_type == EdgeType.DERIVED]
        
        nodes = []
        for edge in derived_edges:
            node = self.store.get_node(edge.source)
            if node:
                nodes.append(node)
        
        return nodes


# ═══════════════════════════════════════════════════════════════
# Time-Based Queries
# ═══════════════════════════════════════════════════════════════

class TimeQuery:
    """
    Time-based memory queries
    """
    
    def __init__(self, store: VFSStore):
        self.store = store
    
    def query(self, prefix: str = "/memory",
              after: datetime = None,
              before: datetime = None,
              time_range: str = None,
              limit: int = 100) -> List[VFSNode]:
        """
        Query memories by time
        
        Args:
            prefix: Path prefix
            after: Only memories after this time
            before: Only memories before this time
            time_range: Shorthand ("last_24h", "last_7d", "last_30d", "today")
            limit: Max results
        
        Returns:
            List of matching nodes, sorted by time (newest first)
        """
        # Parse time_range shorthand
        if time_range:
            after, before = self._parse_time_range(time_range)
        
        # Get all nodes
        nodes = self.store.list_nodes(prefix, limit=limit * 2)
        
        # Filter by time
        filtered = []
        for node in nodes:
            created = node.meta.get("created_at") or node.updated_at.isoformat()
            try:
                node_time = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except:
                node_time = node.updated_at
            
            if after and node_time < after:
                continue
            if before and node_time > before:
                continue
            
            filtered.append((node, node_time))
        
        # Sort by time descending
        filtered.sort(key=lambda x: x[1], reverse=True)
        
        return [n for n, _ in filtered[:limit]]
    
    def _parse_time_range(self, time_range: str) -> Tuple[datetime, datetime]:
        """Parse time range shorthand"""
        now = datetime.utcnow()
        
        ranges = {
            "last_1h": timedelta(hours=1),
            "last_24h": timedelta(hours=24),
            "last_7d": timedelta(days=7),
            "last_30d": timedelta(days=30),
            "last_90d": timedelta(days=90),
        }
        
        if time_range in ranges:
            return now - ranges[time_range], now
        
        if time_range == "today":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return start, now
        
        if time_range == "yesterday":
            end = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start = end - timedelta(days=1)
            return start, end
        
        # Default: last 7 days
        return now - timedelta(days=7), now
    
    def group_by_date(self, nodes: List[VFSNode]) -> Dict[str, List[VFSNode]]:
        """Group nodes by date"""
        grouped: Dict[str, List[VFSNode]] = {}
        
        for node in nodes:
            created = node.meta.get("created_at") or node.updated_at.isoformat()
            try:
                date_str = created[:10]  # YYYY-MM-DD
            except:
                date_str = "unknown"
            
            if date_str not in grouped:
                grouped[date_str] = []
            grouped[date_str].append(node)
        
        return grouped
