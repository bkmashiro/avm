"""
vfs/retrieval.py - 联动检索与动态文档构建

功能:
1. 语义搜索 (embedding)
2. 图扩展 (关联节点)
3. 动态文档合成
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Tuple

from .store import VFSStore
from .node import VFSNode
from .graph import EdgeType
from .embedding import EmbeddingStore, EmbeddingBackend


@dataclass
class RetrievalResult:
    """检索结果"""
    query: str
    nodes: List[VFSNode]
    scores: Dict[str, float]  # path -> relevance score
    sources: Dict[str, str]   # path -> source type (semantic/graph/fts)
    graph_edges: List[Tuple[str, str, str]]  # (from, to, type)
    
    @property
    def paths(self) -> List[str]:
        return [n.path for n in self.nodes]
    
    def get_score(self, path: str) -> float:
        return self.scores.get(path, 0.0)
    
    def get_source(self, path: str) -> str:
        return self.sources.get(path, "unknown")


@dataclass
class SynthesizedDocument:
    """动态合成的文档"""
    title: str
    content: str
    sections: List[Dict[str, Any]]
    sources: List[str]
    generated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_markdown(self) -> str:
        return self.content


class Retriever:
    """
    联动检索器
    
    支持:
    - 语义搜索 (需要 embedding)
    - FTS5 全文搜索 (fallback)
    - 图扩展
    - 结果融合
    """
    
    def __init__(self, store: VFSStore, 
                 embedding_store: EmbeddingStore = None):
        self.store = store
        self.embedding_store = embedding_store
    
    def retrieve(self, query: str,
                 k: int = 5,
                 expand_graph: bool = True,
                 graph_depth: int = 1,
                 prefix: str = None) -> RetrievalResult:
        """
        联动检索
        
        Args:
            query: 查询文本
            k: 返回数量
            expand_graph: 是否扩展关系图
            graph_depth: 图扩展深度
            prefix: 路径前缀过滤
        """
        nodes = []
        scores = {}
        sources = {}
        seen_paths: Set[str] = set()
        
        # 1. 语义搜索 (如果有 embedding)
        if self.embedding_store:
            semantic_results = self.embedding_store.search(query, k=k, prefix=prefix)
            for node, score in semantic_results:
                if node.path not in seen_paths:
                    nodes.append(node)
                    scores[node.path] = score
                    sources[node.path] = "semantic"
                    seen_paths.add(node.path)
        
        # 2. FTS5 全文搜索 (补充或 fallback)
        fts_results = self.store.search(query, limit=k)
        for node, score in fts_results:
            if node.path not in seen_paths:
                nodes.append(node)
                # 归一化 FTS score
                scores[node.path] = min(1.0, score / 10.0)
                sources[node.path] = "fts"
                seen_paths.add(node.path)
        
        # 3. 图扩展
        graph_edges = []
        if expand_graph and nodes:
            expanded = self._expand_graph(
                [n.path for n in nodes],
                depth=graph_depth,
                max_expand=k
            )
            
            for path, edge_info in expanded.items():
                if path not in seen_paths:
                    node = self.store.get_node(path)
                    if node:
                        nodes.append(node)
                        # 图扩展的 score 衰减
                        scores[path] = edge_info["score"] * 0.5
                        sources[path] = "graph"
                        seen_paths.add(path)
                        graph_edges.append((
                            edge_info["from"],
                            path,
                            edge_info["type"]
                        ))
        
        # 4. 按 score 排序
        nodes.sort(key=lambda n: scores.get(n.path, 0), reverse=True)
        
        return RetrievalResult(
            query=query,
            nodes=nodes[:k * 2],  # 返回更多以便合成
            scores=scores,
            sources=sources,
            graph_edges=graph_edges,
        )
    
    def _expand_graph(self, seed_paths: List[str], 
                      depth: int = 1,
                      max_expand: int = 10) -> Dict[str, Dict]:
        """
        从种子节点扩展关系图
        
        Returns: {path: {"from": src, "type": edge_type, "score": weight}}
        """
        expanded = {}
        visited = set(seed_paths)
        current_level = seed_paths
        
        for d in range(depth):
            next_level = []
            
            for path in current_level:
                edges = self.store.get_links(path, direction="both")
                
                for edge in edges:
                    other = edge.target if edge.source == path else edge.source
                    
                    if other not in visited and len(expanded) < max_expand:
                        visited.add(other)
                        next_level.append(other)
                        expanded[other] = {
                            "from": path,
                            "type": edge.edge_type.value,
                            "score": edge.weight,
                        }
            
            current_level = next_level
            if not current_level:
                break
        
        return expanded


class DocumentSynthesizer:
    """
    动态文档合成器
    
    将多个节点的内容聚合成一个结构化文档
    """
    
    def __init__(self, store: VFSStore):
        self.store = store
    
    def synthesize(self, result: RetrievalResult,
                   title: str = None,
                   max_sections: int = 5,
                   section_max_chars: int = 500) -> SynthesizedDocument:
        """
        合成动态文档
        
        Args:
            result: 检索结果
            title: 文档标题（默认使用 query）
            max_sections: 最大章节数
            section_max_chars: 每个章节的最大字符数
        """
        if not title:
            title = f"{result.query} (auto-generated)"
        
        sections = []
        sources = []
        
        # 按类别分组
        categorized = self._categorize_nodes(result.nodes)
        
        for category, nodes in categorized.items():
            if len(sections) >= max_sections:
                break
            
            section = self._build_section(
                category, nodes, result,
                max_chars=section_max_chars
            )
            sections.append(section)
            sources.extend([n.path for n in nodes])
        
        # 构建 Markdown
        content = self._build_markdown(title, sections, result)
        
        return SynthesizedDocument(
            title=title,
            content=content,
            sections=sections,
            sources=list(set(sources)),
        )
    
    def _categorize_nodes(self, nodes: List[VFSNode]) -> Dict[str, List[VFSNode]]:
        """按路径前缀分类节点"""
        categories = {}
        
        category_names = {
            "/market/indicators": "技术指标",
            "/market/news": "相关新闻",
            "/market/watchlist": "关联标的",
            "/trading/positions": "当前持仓",
            "/memory/lessons": "历史经验",
            "/memory": "记忆笔记",
            "/research": "研究报告",
            "/live": "实时数据",
        }
        
        for node in nodes:
            # 找最长匹配的前缀
            matched_prefix = None
            matched_name = "其他"
            
            for prefix, name in category_names.items():
                if node.path.startswith(prefix):
                    if matched_prefix is None or len(prefix) > len(matched_prefix):
                        matched_prefix = prefix
                        matched_name = name
            
            if matched_name not in categories:
                categories[matched_name] = []
            categories[matched_name].append(node)
        
        return categories
    
    def _build_section(self, category: str, 
                       nodes: List[VFSNode],
                       result: RetrievalResult,
                       max_chars: int = 500) -> Dict:
        """构建章节"""
        items = []
        
        for node in nodes[:3]:  # 每个类别最多3个
            # 提取摘要
            content = node.content
            
            # 尝试提取关键信息
            summary = self._extract_summary(content, max_chars // 3)
            
            items.append({
                "path": node.path,
                "summary": summary,
                "score": result.get_score(node.path),
                "source_type": result.get_source(node.path),
            })
        
        return {
            "category": category,
            "items": items,
        }
    
    def _extract_summary(self, content: str, max_chars: int) -> str:
        """提取内容摘要"""
        # 移除 Markdown 标题
        lines = content.split("\n")
        text_lines = []
        
        for line in lines:
            line = line.strip()
            if line and not line.startswith("#") and not line.startswith("*Updated:"):
                text_lines.append(line)
        
        text = " ".join(text_lines)
        
        if len(text) > max_chars:
            text = text[:max_chars] + "..."
        
        return text
    
    def _build_markdown(self, title: str, 
                        sections: List[Dict],
                        result: RetrievalResult) -> str:
        """构建 Markdown 文档"""
        lines = [
            f"# {title}",
            "",
            f"*Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
            f"*Query: \"{result.query}\"*",
            "",
        ]
        
        for section in sections:
            lines.append(f"## {section['category']}")
            lines.append("")
            
            for item in section["items"]:
                # 来源标注
                source_badge = ""
                if item["source_type"] == "semantic":
                    source_badge = "🎯"
                elif item["source_type"] == "graph":
                    source_badge = "🔗"
                else:
                    source_badge = "📝"
                
                lines.append(f"> {source_badge} 来源: `{item['path']}`")
                lines.append("")
                lines.append(item["summary"])
                lines.append("")
        
        # 关联图
        if result.graph_edges:
            lines.append("## 关联关系")
            lines.append("")
            for src, tgt, etype in result.graph_edges:
                lines.append(f"- {src} --[{etype}]--> {tgt}")
            lines.append("")
        
        return "\n".join(lines)
    
    def quick_summary(self, query: str, 
                      retriever: Retriever,
                      k: int = 5) -> str:
        """
        快速生成查询摘要
        
        一行调用：
            synthesizer.quick_summary("NVDA风险分析", retriever)
        """
        result = retriever.retrieve(query, k=k, expand_graph=True)
        doc = self.synthesize(result, max_sections=5)
        return doc.to_markdown()
