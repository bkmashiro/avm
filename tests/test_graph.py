"""
test_graph.py - KVGraph test
"""

import pytest

from avm.graph import KVGraph, Edge, EdgeType


class TestEdge:
    """Edge test"""
    
    def test_create_edge(self):
        """createedge"""
        edge = Edge(
            source="/research/AAPL.md",
            target="/research/MSFT.md",
            edge_type=EdgeType.PEER,
            weight=0.8,
        )
        
        assert edge.source == "/research/AAPL.md"
        assert edge.target == "/research/MSFT.md"
        assert edge.edge_type == EdgeType.PEER
        assert edge.weight == 0.8
    
    def test_edge_to_tuple(self):
        """edge转元组"""
        edge = Edge(
            source="/a",
            target="/b",
            edge_type=EdgeType.CITATION,
            weight=1.5,
        )
        
        t = edge.to_tuple()
        assert t == ("/a", "/b", "citation", 1.5)


class TestKVGraph:
    """KVGraph test"""
    
    def test_add_edge(self):
        """addedge"""
        g = KVGraph()
        
        edge = g.add_edge("/a", "/b", EdgeType.PEER)
        
        assert edge.source == "/a"
        assert edge.target == "/b"
        assert g.edge_count == 1
        assert g.node_count == 2
    
    def test_remove_edge(self):
        """deleteedge"""
        g = KVGraph()
        g.add_edge("/a", "/b", EdgeType.PEER)
        g.add_edge("/a", "/c", EdgeType.PARENT)
        
        removed = g.remove_edge("/a", "/b")
        
        assert removed == 1
        assert g.edge_count == 1
    
    def test_get_outgoing(self):
        """get出edge"""
        g = KVGraph()
        g.add_edge("/a", "/b", EdgeType.PEER)
        g.add_edge("/a", "/c", EdgeType.PARENT)
        g.add_edge("/b", "/d", EdgeType.PEER)
        
        edges = g.get_outgoing("/a")
        
        assert len(edges) == 2
        targets = {e.target for e in edges}
        assert targets == {"/b", "/c"}
    
    def test_get_outgoing_filtered(self):
        """按typefilter出edge"""
        g = KVGraph()
        g.add_edge("/a", "/b", EdgeType.PEER)
        g.add_edge("/a", "/c", EdgeType.PARENT)
        
        edges = g.get_outgoing("/a", EdgeType.PEER)
        
        assert len(edges) == 1
        assert edges[0].target == "/b"
    
    def test_get_incoming(self):
        """get入edge"""
        g = KVGraph()
        g.add_edge("/a", "/c", EdgeType.PEER)
        g.add_edge("/b", "/c", EdgeType.PEER)
        
        edges = g.get_incoming("/c")
        
        assert len(edges) == 2
        sources = {e.source for e in edges}
        assert sources == {"/a", "/b"}
    
    def test_get_neighbors(self):
        """get邻居"""
        g = KVGraph()
        g.add_edge("/a", "/b")
        g.add_edge("/c", "/a")
        
        neighbors = g.get_neighbors("/a")
        
        assert neighbors == {"/b", "/c"}
    
    def test_find_path(self):
        """查找path"""
        g = KVGraph()
        g.add_edge("/a", "/b")
        g.add_edge("/b", "/c")
        g.add_edge("/c", "/d")
        
        path = g.find_path("/a", "/d")
        
        assert path == ["/a", "/b", "/c", "/d"]
    
    def test_find_path_not_found(self):
        """path不exists"""
        g = KVGraph()
        g.add_edge("/a", "/b")
        g.add_edge("/c", "/d")
        
        path = g.find_path("/a", "/d")
        
        assert path is None
    
    def test_get_subgraph(self):
        """get子图"""
        g = KVGraph()
        g.add_edge("/a", "/b")
        g.add_edge("/b", "/c")
        g.add_edge("/c", "/d")
        g.add_edge("/x", "/y")  # 不连通
        
        sub = g.get_subgraph("/a", depth=2)
        
        assert sub.node_count >= 3  # a, b, c
        assert sub.edge_count >= 2
    
    def test_to_adjacency_list(self):
        """export邻接table"""
        g = KVGraph()
        g.add_edge("/a", "/b", EdgeType.PEER)
        g.add_edge("/a", "/c", EdgeType.PARENT)
        
        adj = g.to_adjacency_list()
        
        assert "/a" in adj
        assert len(adj["/a"]) == 2
