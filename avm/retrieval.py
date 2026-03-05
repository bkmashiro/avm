"""
avm/retrieval.py - Linked retrieval and dynamic document building

features:
1. semanticsearch (embedding)
2. graphextend (relatednode)
3. Dynamic document synthesis
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Tuple

from .store import AVMStore
from .node import AVMNode
from .graph import EdgeType
from .embedding import EmbeddingStore, EmbeddingBackend


@dataclass
class RetrievalResult:
    """retrieveresult"""
    query: str
    nodes: List[AVMNode]
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
    """Dynamically synthesized document"""
    title: str
    content: str
    sections: List[Dict[str, Any]]
    sources: List[str]
    generated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_markdown(self) -> str:
        return self.content


class Retriever:
    """
    Linked retriever
    
    supports:
    - semanticsearch (requires embedding)
    - FTS5 full-textsearch (fallback)
    - graphextend
    - resultfusion
    """
    
    def __init__(self, store: AVMStore, 
                 embedding_store: EmbeddingStore = None):
        self.store = store
        self.embedding_store = embedding_store
    
    def retrieve(self, query: str,
                 k: int = 5,
                 expand_graph: bool = True,
                 graph_depth: int = 1,
                 prefix: str = None) -> RetrievalResult:
        """
        Linked retrieval
        
        Args:
            query: Query text
            k: returncount
            expand_graph: whetherextendrelationgraph
            graph_depth: Graph expansion depth
            prefix: pathprefixfilter
        """
        nodes = []
        scores = {}
        sources = {}
        seen_paths: Set[str] = set()
        
        # 1. semanticsearch (if embedding)
        if self.embedding_store:
            semantic_results = self.embedding_store.search(query, k=k, prefix=prefix)
            for node, score in semantic_results:
                if node.path not in seen_paths:
                    nodes.append(node)
                    scores[node.path] = score
                    sources[node.path] = "semantic"
                    seen_paths.add(node.path)
        
        # 2. FTS5 full-text search (supplement or fallback)
        fts_results = self.store.search(query, limit=k)
        for node, score in fts_results:
            if node.path not in seen_paths:
                nodes.append(node)
                # Normalize FTS score
                scores[node.path] = min(1.0, score / 10.0)
                sources[node.path] = "fts"
                seen_paths.add(node.path)
        
        # 3. graphextend
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
                        # Score decay for graph expansion
                        scores[path] = edge_info["score"] * 0.5
                        sources[path] = "graph"
                        seen_paths.add(path)
                        graph_edges.append((
                            edge_info["from"],
                            path,
                            edge_info["type"]
                        ))
        
        # 4. Sort by score
        nodes.sort(key=lambda n: scores.get(n.path, 0), reverse=True)
        
        return RetrievalResult(
            query=query,
            nodes=nodes[:k * 2],  # Return more for synthesis
            scores=scores,
            sources=sources,
            graph_edges=graph_edges,
        )
    
    def _expand_graph(self, seed_paths: List[str], 
                      depth: int = 1,
                      max_expand: int = 10) -> Dict[str, Dict]:
        """
        fromseednodeextendrelationgraph
        
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
    Dynamic document synthesizer
    
    Aggregate multiple node contents into a structured document
    """
    
    def __init__(self, store: AVMStore):
        self.store = store
    
    def synthesize(self, result: RetrievalResult,
                   title: str = None,
                   max_sections: int = 5,
                   section_max_chars: int = 500) -> SynthesizedDocument:
        """
        Synthesize dynamic document
        
        Args:
            result: retrieveresult
            title: documenttitle（defaultuse query）
            max_sections: Max section count
            section_max_chars: Max characters per section
        """
        if not title:
            title = f"{result.query} (auto-generated)"
        
        sections = []
        sources = []
        
        # Group by category
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
        
        # build Markdown
        content = self._build_markdown(title, sections, result)
        
        return SynthesizedDocument(
            title=title,
            content=content,
            sections=sections,
            sources=list(set(sources)),
        )
    
    def _categorize_nodes(self, nodes: List[AVMNode]) -> Dict[str, List[AVMNode]]:
        """Categorize node by path prefix"""
        categories = {}
        
        category_names = {
            "/market/indicators": "technical indicators",
            "/market/news": "relatednews",
            "/market/watchlist": "Related assets",
            "/trading/positions": "currentpositions",
            "/memory/lessons": "historyexperience",
            "/memory": "Memory notes",
            "/research": "researchreport",
            "/live": "live data",
        }
        
        for node in nodes:
            # Find longest matching prefix
            matched_prefix = None
            matched_name = "other"
            
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
                       nodes: List[AVMNode],
                       result: RetrievalResult,
                       max_chars: int = 500) -> Dict:
        """buildsection"""
        items = []
        
        for node in nodes[:3]:  # At most 3 per category
            # extractsummary
            content = node.content
            
            # Try to extract key info
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
        """extractcontentsummary"""
        # remove Markdown title
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
        """build Markdown document"""
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
                # sourceannotation
                source_badge = ""
                if item["source_type"] == "semantic":
                    source_badge = "🎯"
                elif item["source_type"] == "graph":
                    source_badge = "🔗"
                else:
                    source_badge = "📝"
                
                lines.append(f"> {source_badge} source: `{item['path']}`")
                lines.append("")
                lines.append(item["summary"])
                lines.append("")
        
        # relatedgraph
        if result.graph_edges:
            lines.append("## relatedrelation")
            lines.append("")
            for src, tgt, etype in result.graph_edges:
                lines.append(f"- {src} --[{etype}]--> {tgt}")
            lines.append("")
        
        return "\n".join(lines)
    
    def quick_summary(self, query: str, 
                      retriever: Retriever,
                      k: int = 5) -> str:
        """
        Quickly generate query summary
        
        One-line call：
            synthesizer.quick_summary("NVDA risk analysis", retriever)
        """
        result = retriever.retrieve(query, k=k, expand_graph=True)
        doc = self.synthesize(result, max_sections=5)
        return doc.to_markdown()
