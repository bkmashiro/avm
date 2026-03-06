#!/usr/bin/env python3
"""
Direct AVM test without FUSE.
Tests core functionality: write, read, search, recall.
"""

import os
import sys
import tempfile

# Use temp db
os.environ["XDG_DATA_HOME"] = tempfile.mkdtemp()

from avm import AVM
from avm.agent_memory import AgentMemory, MemoryConfig

def main():
    print("=== AVM Direct Test ===\n")
    
    # Create AVM
    avm = AVM()
    print(f"DB: {avm.store.db_path}")
    
    # Create agent memory
    config = MemoryConfig(duplicate_check=True, duplicate_threshold=0.7)
    agent = AgentMemory(avm, "test-agent", config=config)
    
    # Test remember
    print("\n--- Remember ---")
    r1 = agent.remember("BTC RSI at 70, showing overbought signal", 
                        title="btc-signal", importance=0.9, tags=["crypto", "technical"])
    print(f"Wrote: {r1.path}")
    print(f"Similar: {r1.similar}")
    
    r2 = agent.remember("ETH MACD golden cross detected", 
                        title="eth-signal", importance=0.8, tags=["crypto", "technical"])
    print(f"Wrote: {r2.path}")
    
    r3 = agent.remember("BTC RSI now at 72, still overbought",
                        title="btc-update", importance=0.7)
    print(f"Wrote: {r3.path}")
    print(f"Similar found: {r3.has_similar}")
    if r3.similar:
        for m in r3.similar:
            print(f"  - {m.path}: {m.similarity:.2f}")
    
    # Test recall
    print("\n--- Recall ---")
    result = agent.recall("crypto signals", max_tokens=500)
    print(f"Recalled {len(result)} chars")
    print(result[:200] + "..." if len(result) > 200 else result)
    
    # Test search
    print("\n--- Search ---")
    results = avm.search("RSI overbought", limit=5)
    print(f"Found {len(results)} results")
    for node, score in results:
        print(f"  {node.path}: score={score:.4f}")
    
    # Test list
    print("\n--- List ---")
    nodes = avm.store.list_nodes(limit=10)
    print(f"Total nodes: {len(nodes)}")
    for n in nodes:
        print(f"  {n.path}")
    
    # Test stats
    print("\n--- Stats ---")
    stats = avm.store.stats()
    print(f"Nodes: {stats.get('total_nodes', 0)}")
    print(f"Edges: {stats.get('total_edges', 0)}")
    
    print("\n=== Test Complete ===")


if __name__ == '__main__':
    main()
