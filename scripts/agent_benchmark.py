#!/usr/bin/env python3
"""
Agent Benchmark: End-to-end evaluation with LLM.

Flow:
1. Store memories in AVM
2. Agent receives question
3. Agent uses AVM recall to get context
4. Agent generates answer
5. LLM judges if answer is correct

Requires: ANTHROPIC_API_KEY in environment
"""

import os
import json
import time
import urllib.request
from dataclasses import dataclass
from typing import List, Optional

from avm import AVM


@dataclass
class AgentTestCase:
    name: str
    memories: List[str]  # Memories to store
    question: str  # Question to ask
    expected_answer: str  # What the answer should contain
    tags: List[str] = None


TEST_CASES = [
    AgentTestCase(
        name="laptop_recommendation",
        memories=[
            "User budget is $1500 for a new laptop",
            "User needs laptop for programming and video editing",
            "User prefers long battery life over performance",
            "MacBook Air M3 costs $1299, has 18hr battery",
            "ThinkPad X1 Carbon costs $1449, has 15hr battery",
            "MacBook Pro 14 costs $1999, has 12hr battery",
        ],
        question="What laptop should I buy within my budget?",
        expected_answer="MacBook Air or ThinkPad X1 (both under $1500, good battery)",
        tags=["laptop", "recommendation"],
    ),
    AgentTestCase(
        name="trading_advice",
        memories=[
            "BTC current price is $52,000",
            "BTC RSI is at 72, which is overbought territory",
            "User's rule: sell when RSI > 70",
            "User's position: 0.5 BTC bought at $48,000",
            "Current profit: $2,000 (8.3%)",
        ],
        question="Should I sell my BTC position now?",
        expected_answer="Consider selling (RSI overbought at 72, matches user's rule)",
        tags=["trading", "btc"],
    ),
    AgentTestCase(
        name="meeting_context",
        memories=[
            "Monday meeting: discussed Q2 roadmap",
            "Team decided to prioritize mobile app",
            "John will lead mobile development",
            "Deadline for MVP is April 15th",
            "Budget approved: $50,000 for mobile project",
        ],
        question="What did we decide about the mobile project?",
        expected_answer="John leads, MVP by April 15th, $50k budget",
        tags=["meeting", "project"],
    ),
]


def call_claude(prompt: str, system: str = None) -> str:
    """Call Claude API."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    
    messages = [{"role": "user", "content": prompt}]
    
    data = json.dumps({
        "model": "claude-3-5-haiku-20241022",  # Cheap model
        "max_tokens": 500,
        "messages": messages,
        "system": system or "You are a helpful assistant.",
    }).encode()
    
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=data,
        headers={
            "x-api-key": api_key,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01",
        }
    )
    
    with urllib.request.urlopen(req, timeout=30) as r:
        result = json.loads(r.read())
    
    return result["content"][0]["text"]


def judge_answer(question: str, context: str, answer: str, expected: str) -> dict:
    """Use LLM to judge if answer is correct."""
    prompt = f"""Judge if the assistant's answer correctly addresses the question based on the context.

Question: {question}

Context provided to assistant:
{context}

Assistant's answer:
{answer}

Expected answer should cover:
{expected}

Rate the answer:
1. Correctness (1-5): Does it match expected answer?
2. Relevance (1-5): Does it use the context appropriately?
3. Completeness (1-5): Does it cover key points?

Respond in JSON format:
{{"correctness": N, "relevance": N, "completeness": N, "explanation": "brief reason"}}"""
    
    response = call_claude(prompt, system="You are a fair judge. Output only valid JSON.")
    
    try:
        # Extract JSON from response
        start = response.find("{")
        end = response.rfind("}") + 1
        return json.loads(response[start:end])
    except:
        return {"correctness": 0, "relevance": 0, "completeness": 0, "explanation": "Parse error"}


def run_agent_benchmark():
    print("=" * 60)
    print("Agent Benchmark: End-to-End with LLM")
    print("=" * 60)
    
    # Check API key
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("\n❌ ANTHROPIC_API_KEY not set!")
        print("   Export it or add to .env")
        return
    
    avm = AVM()
    agent_id = f"agent_{int(time.time())}"
    agent = avm.agent_memory(agent_id)
    
    results = []
    
    for i, tc in enumerate(TEST_CASES):
        print(f"\n{'='*60}")
        print(f"Test {i+1}: {tc.name}")
        print("=" * 60)
        
        # 1. Store memories
        print("\n1. Storing memories...")
        for mem in tc.memories:
            agent.remember(mem, tags=tc.tags or [])
        print(f"   Stored {len(tc.memories)} memories")
        
        # 2. Recall context
        print("\n2. Recalling context...")
        context = agent.recall(tc.question, max_tokens=500)
        print(f"   Retrieved {len(context)} chars")
        
        # 3. Generate answer
        print("\n3. Generating answer...")
        answer_prompt = f"""Based on the following context, answer the question.

Context:
{context}

Question: {tc.question}

Answer concisely:"""
        
        answer = call_claude(answer_prompt)
        print(f"   Answer: {answer[:100]}...")
        
        # 4. Judge answer
        print("\n4. Judging answer...")
        judgment = judge_answer(tc.question, context, answer, tc.expected_answer)
        
        avg_score = (judgment["correctness"] + judgment["relevance"] + judgment["completeness"]) / 3
        print(f"   Correctness: {judgment['correctness']}/5")
        print(f"   Relevance: {judgment['relevance']}/5")
        print(f"   Completeness: {judgment['completeness']}/5")
        print(f"   Avg Score: {avg_score:.1f}/5")
        print(f"   Reason: {judgment.get('explanation', 'N/A')}")
        
        results.append({
            "name": tc.name,
            "scores": judgment,
            "avg": avg_score,
        })
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    overall_avg = sum(r["avg"] for r in results) / len(results)
    
    print(f"\n{'Test Case':<25} {'Correct':<10} {'Relevant':<10} {'Complete':<10} {'Avg':<8}")
    print("-" * 63)
    for r in results:
        s = r["scores"]
        print(f"{r['name']:<25} {s['correctness']:<10} {s['relevance']:<10} {s['completeness']:<10} {r['avg']:.1f}")
    print("-" * 63)
    print(f"{'OVERALL':<25} {'':<10} {'':<10} {'':<10} {overall_avg:.1f}/5")
    
    # Grade
    if overall_avg >= 4.5:
        grade = "A"
    elif overall_avg >= 3.5:
        grade = "B"
    elif overall_avg >= 2.5:
        grade = "C"
    else:
        grade = "D"
    
    print(f"\nGrade: {grade}")
    print("\n✓ Agent benchmark complete!")


if __name__ == "__main__":
    run_agent_benchmark()
