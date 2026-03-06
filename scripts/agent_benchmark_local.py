#!/usr/bin/env python3
"""
Agent Benchmark (Local): Retrieval quality test without external API.

Tests if AVM recall returns the right context for answering questions.
Prints context so human/agent can evaluate.
"""

import time
from dataclasses import dataclass
from typing import List

from avm import AVM


@dataclass
class AgentTestCase:
    name: str
    memories: List[str]
    question: str
    key_facts: List[str]  # Facts that should be in context


TEST_CASES = [
    AgentTestCase(
        name="laptop_recommendation",
        memories=[
            "User budget is $1500 for a new laptop",
            "User needs laptop for programming",
            "MacBook Air M3 costs $1299, has 18hr battery",
            "ThinkPad X1 Carbon costs $1449, has 15hr battery",
            "MacBook Pro 14 costs $1999, over budget",
        ],
        question="laptop budget MacBook ThinkPad",  # Keywords
        key_facts=["1500", "MacBook", "ThinkPad"],
    ),
    AgentTestCase(
        name="trading_decision",
        memories=[
            "BTC RSI is at 72 overbought",
            "User rule sell when RSI above 70",
            "User holds 0.5 BTC at 48000",
            "Current BTC price 52000",
        ],
        question="BTC RSI sell trading",  # Keywords
        key_facts=["RSI", "72", "sell", "70"],
    ),
    AgentTestCase(
        name="project_status",
        memories=[
            "Project Alpha deadline March 15th",
            "John leads development team",
            "Budget 50000 approved",
            "Current progress 60 percent complete",
        ],
        question="Project Alpha deadline budget",  # Keywords
        key_facts=["March 15", "John", "50000", "60"],
    ),
]


def run_benchmark():
    print("=" * 60)
    print("Agent Benchmark: Context Quality Test")
    print("=" * 60)
    
    avm = AVM()
    # Use unique agent_id per run
    agent_id = f"qbench_{int(time.time())}"
    agent = avm.agent_memory(agent_id)
    print(f"Agent: {agent_id}")
    
    results = []
    
    for tc in TEST_CASES:
        print(f"\n{'─'*60}")
        print(f"📋 {tc.name}")
        print("─" * 60)
        
        # Store memories with relevant tags
        for mem in tc.memories:
            # Extract keywords from memory for tags
            words = mem.lower().split()[:3]
            agent.remember(mem, tags=words)
        
        # Recall
        context = agent.recall(tc.question, max_tokens=300)
        
        # Check key facts
        found = []
        missing = []
        for fact in tc.key_facts:
            if fact.lower() in context.lower():
                found.append(fact)
            else:
                missing.append(fact)
        
        hit_rate = len(found) / len(tc.key_facts) * 100
        
        print(f"\n❓ Question: {tc.question}")
        print(f"\n📝 Context returned ({len(context)} chars):")
        print("─" * 40)
        print(context[:500])
        if len(context) > 500:
            print("...")
        print("─" * 40)
        
        print(f"\n✅ Found: {found}")
        if missing:
            print(f"❌ Missing: {missing}")
        print(f"📊 Hit Rate: {hit_rate:.0f}%")
        
        results.append({
            "name": tc.name,
            "hit_rate": hit_rate,
            "found": len(found),
            "total": len(tc.key_facts),
        })
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    avg_hit_rate = sum(r["hit_rate"] for r in results) / len(results)
    
    print(f"\n{'Test':<25} {'Found':<10} {'Hit Rate':<10}")
    print("-" * 45)
    for r in results:
        print(f"{r['name']:<25} {r['found']}/{r['total']:<7} {r['hit_rate']:.0f}%")
    print("-" * 45)
    print(f"{'AVERAGE':<25} {'':<10} {avg_hit_rate:.0f}%")
    
    if avg_hit_rate >= 80:
        grade = "✓ PASS"
    else:
        grade = "✗ NEEDS IMPROVEMENT"
    
    print(f"\nResult: {grade}")
    print("\n✓ Benchmark complete!")


if __name__ == "__main__":
    run_benchmark()
