"""
Tests for retrieval.py - Semantic retrieval and synthesis
"""

import os
import pytest
import tempfile

os.environ["XDG_DATA_HOME"] = tempfile.mkdtemp()

from avm import AVMStore, AVMNode
from avm.retrieval import Retriever, DocumentSynthesizer, RetrievalResult


class TestRetriever:
    """Tests for Retriever."""
    
    @pytest.fixture
    def store_with_data(self):
        store = AVMStore()
        # Add test nodes
        store.put_node(AVMNode(path="/memory/btc", content="BTC RSI at 70, overbought signal"))
        store.put_node(AVMNode(path="/memory/eth", content="ETH MACD golden cross detected"))
        store.put_node(AVMNode(path="/memory/market", content="Overall market sentiment bullish"))
        
        # Add links
        store.add_edge("/memory/btc", "/memory/market")
        store.add_edge("/memory/eth", "/memory/market")
        return store
    
    def test_retrieve_basic(self, store_with_data):
        """Test basic retrieval."""
        retriever = Retriever(store_with_data)
        result = retriever.retrieve("BTC signal", k=3)
        
        assert isinstance(result, RetrievalResult)
        assert len(result.nodes) > 0
    
    def test_retrieve_with_graph_expand(self, store_with_data):
        """Test retrieval with graph expansion."""
        retriever = Retriever(store_with_data)
        result = retriever.retrieve("BTC", k=1, expand_graph=True, graph_depth=1)
        
        # Should find BTC and expand to market
        paths = [n.path for n in result.nodes]
        assert "/memory/btc" in paths
    
    def test_retrieval_result_properties(self, store_with_data):
        """Test RetrievalResult properties."""
        retriever = Retriever(store_with_data)
        result = retriever.retrieve("market", k=5)
        
        # Test basic structure
        assert result is not None
        assert hasattr(result, 'nodes')


class TestDocumentSynthesizer:
    """Tests for DocumentSynthesizer."""
    
    @pytest.fixture
    def store_with_data(self):
        store = AVMStore()
        store.put_node(AVMNode(path="/memory/note1", content="First note content"))
        store.put_node(AVMNode(path="/memory/note2", content="Second note content"))
        return store
    
    def test_synthesize_basic(self, store_with_data):
        """Test basic synthesis."""
        retriever = Retriever(store_with_data)
        synthesizer = DocumentSynthesizer(store_with_data)
        
        result = retriever.retrieve("note", k=2)
        doc = synthesizer.synthesize(result, title="Notes Summary")
        
        # Doc may be empty if no results, but should not crash
        assert doc is not None
    
    def test_synthesize_empty(self, store_with_data):
        """Test synthesis with no results."""
        retriever = Retriever(store_with_data)
        synthesizer = DocumentSynthesizer(store_with_data)
        
        result = retriever.retrieve("nonexistent xyz", k=2)
        doc = synthesizer.synthesize(result)
        
        assert doc is not None
