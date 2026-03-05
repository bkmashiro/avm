"""
vfs/graph.py - 知识图谱（adjacency list实现）
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum


class EdgeType(Enum):
    """边类型"""
    PEER = "peer"           # 同级关联（如：同板块股票）
    PARENT = "parent"       # 父子关系（如：板块→个股）
    CITATION = "citation"   # 引用关系（如：研报引用）
    DERIVED = "derived"     # 派生关系（如：信号来源于指标）
    RELATED = "related"     # 一般关联


@dataclass
class Edge:
    """
    图边
    """
    source: str         # 源节点路径
    target: str         # 目标节点路径
    edge_type: EdgeType = EdgeType.RELATED
    weight: float = 1.0
    meta: Dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_tuple(self) -> Tuple[str, str, str, float]:
        return (self.source, self.target, self.edge_type.value, self.weight)
    
    def __repr__(self) -> str:
        return f"Edge({self.source} --[{self.edge_type.value}]--> {self.target})"


class KVGraph:
    """
    知识图谱
    
    简单的 adjacency list 实现，支持：
    - 添加/删除边
    - 查询某节点的所有关联
    - 按边类型过滤
    - 路径查找（BFS）
    """
    
    def __init__(self):
        # adjacency list: {source: [Edge, ...]}
        self._outgoing: Dict[str, List[Edge]] = {}
        # 反向索引: {target: [Edge, ...]}
        self._incoming: Dict[str, List[Edge]] = {}
    
    def add_edge(self, source: str, target: str, 
                 edge_type: EdgeType = EdgeType.RELATED,
                 weight: float = 1.0,
                 meta: Dict = None) -> Edge:
        """添加边"""
        edge = Edge(
            source=source,
            target=target,
            edge_type=edge_type,
            weight=weight,
            meta=meta or {},
        )
        
        if source not in self._outgoing:
            self._outgoing[source] = []
        self._outgoing[source].append(edge)
        
        if target not in self._incoming:
            self._incoming[target] = []
        self._incoming[target].append(edge)
        
        return edge
    
    def remove_edge(self, source: str, target: str, 
                    edge_type: EdgeType = None) -> int:
        """删除边，返回删除数量"""
        removed = 0
        
        if source in self._outgoing:
            before = len(self._outgoing[source])
            self._outgoing[source] = [
                e for e in self._outgoing[source]
                if not (e.target == target and 
                       (edge_type is None or e.edge_type == edge_type))
            ]
            removed = before - len(self._outgoing[source])
        
        if target in self._incoming:
            self._incoming[target] = [
                e for e in self._incoming[target]
                if not (e.source == source and
                       (edge_type is None or e.edge_type == edge_type))
            ]
        
        return removed
    
    def get_outgoing(self, node: str, 
                     edge_type: EdgeType = None) -> List[Edge]:
        """获取出边"""
        edges = self._outgoing.get(node, [])
        if edge_type:
            edges = [e for e in edges if e.edge_type == edge_type]
        return edges
    
    def get_incoming(self, node: str,
                     edge_type: EdgeType = None) -> List[Edge]:
        """获取入边"""
        edges = self._incoming.get(node, [])
        if edge_type:
            edges = [e for e in edges if e.edge_type == edge_type]
        return edges
    
    def get_neighbors(self, node: str,
                      edge_type: EdgeType = None) -> Set[str]:
        """获取所有邻居节点"""
        neighbors = set()
        for e in self.get_outgoing(node, edge_type):
            neighbors.add(e.target)
        for e in self.get_incoming(node, edge_type):
            neighbors.add(e.source)
        return neighbors
    
    def find_path(self, source: str, target: str, 
                  max_depth: int = 5) -> Optional[List[str]]:
        """BFS查找路径"""
        if source == target:
            return [source]
        
        visited = {source}
        queue = [(source, [source])]
        
        while queue and len(queue[0][1]) <= max_depth:
            current, path = queue.pop(0)
            
            for neighbor in self.get_neighbors(current):
                if neighbor == target:
                    return path + [neighbor]
                
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        
        return None
    
    def get_subgraph(self, center: str, depth: int = 1) -> "KVGraph":
        """获取以某节点为中心的子图"""
        subgraph = KVGraph()
        visited = set()
        queue = [(center, 0)]
        
        while queue:
            node, d = queue.pop(0)
            if node in visited or d > depth:
                continue
            visited.add(node)
            
            for edge in self.get_outgoing(node):
                subgraph.add_edge(
                    edge.source, edge.target,
                    edge.edge_type, edge.weight, edge.meta
                )
                if d < depth:
                    queue.append((edge.target, d + 1))
            
            for edge in self.get_incoming(node):
                subgraph.add_edge(
                    edge.source, edge.target,
                    edge.edge_type, edge.weight, edge.meta
                )
                if d < depth:
                    queue.append((edge.source, d + 1))
        
        return subgraph
    
    def to_adjacency_list(self) -> Dict[str, List[Dict]]:
        """导出为邻接表"""
        result = {}
        for source, edges in self._outgoing.items():
            result[source] = [
                {"target": e.target, "type": e.edge_type.value, "weight": e.weight}
                for e in edges
            ]
        return result
    
    @property
    def node_count(self) -> int:
        """节点数"""
        nodes = set(self._outgoing.keys()) | set(self._incoming.keys())
        return len(nodes)
    
    @property
    def edge_count(self) -> int:
        """边数"""
        return sum(len(edges) for edges in self._outgoing.values())
    
    def __repr__(self) -> str:
        return f"KVGraph({self.node_count} nodes, {self.edge_count} edges)"
