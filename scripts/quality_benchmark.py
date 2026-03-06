#!/usr/bin/env python3
"""
Quality Benchmark: Test retrieval accuracy with ground truth.

Measures:
- Hit Rate: Does the result contain expected content?
- Token Efficiency: How many tokens used vs baseline?
"""

import time
from dataclasses import dataclass
from typing import List
from avm import AVM


@dataclass
class TestCase:
    name: str
    content: str  # Memory to store
    tags: List[str]
    query: str  # Query to search
    expected: List[str]  # Expected strings in result


# Test cases with ground truth
TEST_CASES = [
    TestCase(
        name="battery_comparison",
        content="MacBook Air has 18 hours battery, ThinkPad X1 has 15 hours battery life",
        tags=["laptop", "battery"],
        query="laptop battery hours",
        expected=["18 hours", "15 hours"],
    ),
    TestCase(
        name="refund_policy",
        content="Refund policy: Full refund within 30 days of purchase, no questions asked",
        tags=["policy", "refund"],
        query="refund policy days",
        expected=["30 days", "refund"],
    ),
    TestCase(
        name="api_rate_limit",
        content="API rate limits: Free tier 100 requests/min, Pro 1000/min, Enterprise unlimited",
        tags=["api", "limits"],
        query="API rate limit requests",
        expected=["100", "1000"],
    ),
    TestCase(
        name="user_timezone",
        content="User preference: timezone is Europe/London, prefers 24-hour format",
        tags=["user", "timezone"],
        query="user timezone",
        expected=["Europe/London"],
    ),
    TestCase(
        name="error_payment",
        content="Error log: Payment failed with Stripe error code 402, card declined for user_123",
        tags=["error", "payment"],
        query="payment error stripe",
        expected=["402", "card declined"],
    ),
    TestCase(
        name="meeting_decision",
        content="Meeting notes: Team decided to use PostgreSQL instead of MySQL for the new project",
        tags=["meeting", "database"],
        query="database decision PostgreSQL",
        expected=["PostgreSQL", "MySQL"],
    ),
    TestCase(
        name="trading_rule",
        content="Trading rule: RSI above 70 is overbought, consider selling. RSI below 30 is oversold, consider buying",
        tags=["trading", "rsi"],
        query="RSI trading overbought",
        expected=["70", "overbought"],
    ),
    TestCase(
        name="deployment_config",
        content="Deployment config: Production server at 10.0.1.50, staging at 10.0.1.51, port 8080",
        tags=["deploy", "config"],
        query="production server IP",
        expected=["10.0.1.50"],
    ),
    TestCase(
        name="user_allergy",
        content="User dietary info: vegetarian, allergic to peanuts and shellfish",
        tags=["user", "food"],
        query="user allergy food",
        expected=["peanuts", "shellfish"],
    ),
    TestCase(
        name="project_deadline",
        content="Project Alpha deadline: March 15th 2024, must complete API integration by March 10th",
        tags=["project", "deadline"],
        query="project deadline March",
        expected=["March 15", "March 10"],
    ),
]


def run_quality_benchmark():
    print("=" * 60)
    print("Quality Benchmark: Retrieval Accuracy")
    print("=" * 60)
    
    avm = AVM()
    agent_id = f"quality_{int(time.time())}"
    agent = avm.agent_memory(agent_id)
    
    # Store all test memories
    print(f"\n1. Storing {len(TEST_CASES)} test memories...")
    for tc in TEST_CASES:
        agent.remember(tc.content, tags=tc.tags)
    print("   Done")
    
    # Add noise (unrelated memories)
    print("\n2. Adding 50 noise memories...")
    for i in range(50):
        agent.remember(f"Random noise memory entry number {i} with no useful info", tags=["noise"])
    print("   Done")
    
    # Run queries and check hits
    print("\n3. Running quality tests...\n")
    
    results = []
    for tc in TEST_CASES:
        result = agent.recall(tc.query, max_tokens=200)
        
        # Check hits
        hits = [exp for exp in tc.expected if exp.lower() in result.lower()]
        hit_rate = len(hits) / len(tc.expected)
        
        status = "✓" if hit_rate == 1.0 else ("◐" if hit_rate > 0 else "✗")
        print(f"   {status} {tc.name}: {len(hits)}/{len(tc.expected)} expected found")
        
        if hit_rate < 1.0:
            missing = [exp for exp in tc.expected if exp.lower() not in result.lower()]
            print(f"      Missing: {missing}")
        
        results.append({
            "name": tc.name,
            "hit_rate": hit_rate,
            "hits": len(hits),
            "expected": len(tc.expected),
        })
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total_hits = sum(r["hits"] for r in results)
    total_expected = sum(r["expected"] for r in results)
    overall_hit_rate = total_hits / total_expected if total_expected > 0 else 0
    
    perfect = sum(1 for r in results if r["hit_rate"] == 1.0)
    partial = sum(1 for r in results if 0 < r["hit_rate"] < 1.0)
    missed = sum(1 for r in results if r["hit_rate"] == 0)
    
    print(f"\nTest Cases: {len(TEST_CASES)}")
    print(f"  ✓ Perfect (100%): {perfect}")
    print(f"  ◐ Partial: {partial}")
    print(f"  ✗ Missed (0%): {missed}")
    
    print(f"\nOverall Hit Rate: {overall_hit_rate*100:.1f}%")
    print(f"  ({total_hits}/{total_expected} expected strings found)")
    
    # Grade
    if overall_hit_rate >= 0.9:
        grade = "A"
    elif overall_hit_rate >= 0.7:
        grade = "B"
    elif overall_hit_rate >= 0.5:
        grade = "C"
    else:
        grade = "D"
    
    print(f"\nGrade: {grade}")
    print("\n✓ Quality benchmark complete!")


if __name__ == "__main__":
    run_quality_benchmark()
