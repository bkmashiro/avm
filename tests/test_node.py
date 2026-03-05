"""
test_node.py - AVMNode test
"""

import pytest
from datetime import datetime

from avm.node import AVMNode, NodeDiff, NodeType, Permission


class TestAVMNode:
    """AVMNode 基础test"""
    
    def test_create_node(self):
        """createnode"""
        node = AVMNode(
            path="/memory/test.md",
            content="# Test\n\nContent here.",
        )
        
        assert node.path == "/memory/test.md"
        assert "Test" in node.content
        assert node.version == 1
        assert node.node_type == NodeType.FILE
    
    def test_writable_path(self):
        """可写path检测"""
        memory_node = AVMNode(path="/memory/test.md")
        assert memory_node.is_writable is True
        
        research_node = AVMNode(path="/research/AAPL.md")
        assert research_node.is_writable is False
        
        live_node = AVMNode(path="/live/positions.md")
        assert live_node.is_writable is False
    
    def test_live_node(self):
        """Live node检测"""
        live_node = AVMNode(
            path="/live/positions.md",
            meta={"ttl_seconds": 60}
        )
        assert live_node.is_live is True
        assert live_node.ttl_seconds == 60
        
        static_node = AVMNode(path="/research/AAPL.md")
        assert static_node.is_live is False
        assert static_node.ttl_seconds is None
    
    def test_content_hash(self):
        """content哈希"""
        node1 = AVMNode(path="/memory/a.md", content="Hello")
        node2 = AVMNode(path="/memory/b.md", content="Hello")
        node3 = AVMNode(path="/memory/c.md", content="World")
        
        assert node1.content_hash == node2.content_hash
        assert node1.content_hash != node3.content_hash
    
    def test_to_dict_from_dict(self):
        """序column化/反序column化"""
        node = AVMNode(
            path="/memory/test.md",
            content="Content",
            meta={"key": "value"},
            version=5,
        )
        
        data = node.to_dict()
        restored = AVMNode.from_dict(data)
        
        assert restored.path == node.path
        assert restored.content == node.content
        assert restored.meta == node.meta
        assert restored.version == node.version


class TestNodeDiff:
    """NodeDiff test"""
    
    def test_create_diff(self):
        """create diff"""
        diff = NodeDiff(
            node_path="/memory/test.md",
            version=2,
            old_hash="abc123",
            new_hash="def456",
            diff_content="- old\n+ new",
            change_type="update",
        )
        
        assert diff.node_path == "/memory/test.md"
        assert diff.version == 2
        assert diff.change_type == "update"
    
    def test_diff_to_dict(self):
        """Diff 序column化"""
        diff = NodeDiff(
            node_path="/memory/test.md",
            version=1,
            old_hash=None,
            new_hash="abc",
            diff_content="content",
            change_type="create",
        )
        
        data = diff.to_dict()
        assert data["node_path"] == "/memory/test.md"
        assert data["change_type"] == "create"
