#!/usr/bin/env python3
"""
Multi-Scenario Benchmark: Different use cases for AVM vs File Read

Scenarios:
1. Recent Context - Reading recent conversation/notes
2. Code Exploration - Finding relevant code in a large codebase
3. Long-term Memory - Recalling from months of accumulated knowledge
4. Mixed Content - Various types of content (notes, code, data)

Usage:
    python scripts/scenario_benchmark.py
"""

import os
import json
import random
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Tuple
import time as _time

from avm import AVM


def count_tokens(text: str) -> int:
    """Estimate token count."""
    return len(text) // 4


@dataclass  
class ScenarioResult:
    scenario: str
    method: str
    tokens_used: int
    latency_ms: float
    recall_quality: str  # "full", "partial", "relevant", "none"


def run_file_method(files: List[Path], query: str) -> Tuple[int, float]:
    """Read all files, return token count and latency."""
    start = _time.perf_counter()
    content = "\n---\n".join(f.read_text() for f in files)
    latency = (_time.perf_counter() - start) * 1000
    return count_tokens(content), latency


def run_avm_method(agent, query: str, max_tokens: int) -> Tuple[int, float, str]:
    """Use AVM recall, return token count, latency, and quality."""
    start = _time.perf_counter()
    result = agent.recall(query, max_tokens=max_tokens)
    latency = (_time.perf_counter() - start) * 1000
    
    # Assess quality
    if "No relevant" in result:
        quality = "none"
    elif len(result) < 100:
        quality = "partial"
    else:
        quality = "relevant"
    
    return count_tokens(result), latency, quality


# ============================================================
# SCENARIO 1: Recent Context
# ============================================================
def scenario_recent_context(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: Agent needs context from recent conversation.
    - 10 recent messages/notes
    - Query about something mentioned recently
    """
    print("\n📝 Scenario 1: Recent Context")
    print("-" * 40)
    
    agent_id = f"recent_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    # Create recent context
    messages = [
        "User asked about the weather in Tokyo",
        "I recommended checking weather.com for forecasts",
        "User mentioned they're planning a trip next week",
        "Discussed best time to visit Tokyo - spring is nice",
        "User asked about currency exchange rates",
        "Suggested using Wise for better rates",
        "User thanked me and said they'll book flights",
        "Reminded user about travel insurance",
        "User asked if I remember their Tokyo trip plans",
        "Last message was about packing suggestions",
    ]
    
    for i, msg in enumerate(messages):
        agent.remember(msg, tags=["conversation", "tokyo", "travel"])
        f = tmpdir / f"msg_{i:02d}.md"
        f.write_text(f"# Message {i}\n\n{msg}")
        files.append(f)
    
    # Query
    query = "Tokyo trip travel"
    max_tokens = 200
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("recent_context", "file", file_tokens, file_latency, "full"),
        ScenarioResult("recent_context", "avm", avm_tokens, avm_latency, quality),
    ]


# ============================================================
# SCENARIO 2: Code Exploration  
# ============================================================
def scenario_code_exploration(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: Agent exploring a codebase to find relevant functions.
    - 50 code files with various functions
    - Query about specific functionality
    """
    print("\n💻 Scenario 2: Code Exploration")
    print("-" * 40)
    
    agent_id = f"code_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    # Simulate code files
    code_snippets = [
        ("auth.py", "def login(user, password): return authenticate(user, password)"),
        ("auth.py", "def logout(session): session.invalidate()"),
        ("auth.py", "def register(email, password): create_user(email, hash(password))"),
        ("database.py", "def connect(): return psycopg2.connect(DATABASE_URL)"),
        ("database.py", "def query(sql): return conn.execute(sql).fetchall()"),
        ("database.py", "def migrate(): run_migrations()"),
        ("api.py", "def get_users(): return User.query.all()"),
        ("api.py", "def create_user(data): return User.create(**data)"),
        ("api.py", "def delete_user(id): User.query.get(id).delete()"),
        ("utils.py", "def hash_password(pwd): return bcrypt.hash(pwd)"),
        ("utils.py", "def verify_password(pwd, hashed): return bcrypt.verify(pwd, hashed)"),
        ("utils.py", "def generate_token(): return secrets.token_urlsafe(32)"),
        ("cache.py", "def get_cache(key): return redis.get(key)"),
        ("cache.py", "def set_cache(key, val): redis.set(key, val)"),
        ("cache.py", "def invalidate_cache(pattern): redis.delete(*redis.keys(pattern))"),
    ]
    
    # Add more noise files
    for i in range(35):
        code_snippets.append((f"module_{i}.py", f"def function_{i}(): return process_data_{i}()"))
    
    for i, (filename, code) in enumerate(code_snippets):
        content = f"# {filename}\n{code}"
        agent.remember(content, tags=["code", filename.replace(".py", "")])
        f = tmpdir / f"{i:02d}_{filename}"
        f.write_text(content)
        files.append(f)
    
    # Query
    query = "password authentication login"
    max_tokens = 300
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("code_exploration", "file", file_tokens, file_latency, "full"),
        ScenarioResult("code_exploration", "avm", avm_tokens, avm_latency, quality),
    ]


# ============================================================
# SCENARIO 3: Long-term Memory
# ============================================================
def scenario_longterm_memory(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: Agent recalling something from months of accumulated memory.
    - 200 memories over "6 months"
    - Query about something specific from the past
    """
    print("\n🧠 Scenario 3: Long-term Memory")
    print("-" * 40)
    
    agent_id = f"longterm_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    # Simulate 6 months of memories
    topics = [
        ("project_alpha", "Started working on Project Alpha with React frontend"),
        ("project_alpha", "Completed authentication module for Alpha"),
        ("project_beta", "New project Beta using Python FastAPI"),
        ("meeting", "Weekly standup - discussed roadmap Q2"),
        ("learning", "Learned about Docker compose for local dev"),
        ("bug", "Fixed memory leak in production server"),
        ("decision", "Decided to use PostgreSQL over MySQL"),
        ("user_feedback", "Users requested dark mode feature"),
        ("deployment", "Deployed v2.0 to production successfully"),
        ("incident", "Downtime incident - database connection pool exhausted"),
    ]
    
    # Generate 200 memories
    for i in range(200):
        topic, base_content = random.choice(topics)
        content = f"{base_content} - Entry #{i}"
        tags = [topic, f"month_{i // 30}"]
        
        # Add specific memorable event
        if i == 42:
            content = "IMPORTANT: User John complained about slow API response times on /users endpoint"
            tags = ["incident", "performance", "john"]
        
        agent.remember(content, tags=tags, importance=random.uniform(0.3, 0.9))
        f = tmpdir / f"memory_{i:03d}.md"
        f.write_text(f"# Memory {i}\nTags: {', '.join(tags)}\n\n{content}")
        files.append(f)
    
    # Query about the specific event
    query = "John API performance slow"
    max_tokens = 300
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("longterm_memory", "file", file_tokens, file_latency, "full"),
        ScenarioResult("longterm_memory", "avm", avm_tokens, avm_latency, quality),
    ]


# ============================================================
# SCENARIO 4: Mixed Content
# ============================================================
def scenario_mixed_content(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: Agent with various types of content - notes, code, data.
    - Mix of markdown notes, code snippets, JSON data
    - Query across different content types
    """
    print("\n📚 Scenario 4: Mixed Content")
    print("-" * 40)
    
    agent_id = f"mixed_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    contents = [
        # Notes
        ("note", "Meeting notes: Discussed Q3 roadmap with team"),
        ("note", "TODO: Review pull request #234 for auth changes"),
        ("note", "Idea: Use Redis for session caching instead of memory"),
        
        # Code
        ("code", "def calculate_revenue(sales): return sum(s.amount for s in sales)"),
        ("code", "class PaymentProcessor: def charge(self, amount): pass"),
        ("code", "async def fetch_exchange_rate(currency): return await api.get_rate(currency)"),
        
        # Data
        ("data", "Config: database_host=db.prod.internal, redis_host=cache.prod"),
        ("data", "Metrics: avg_response_time=145ms, p99=890ms, error_rate=0.02%"),
        ("data", "Users: total=15420, active_daily=3200, premium=890"),
        
        # More varied content
        ("note", "Bug report: Users seeing 500 error on checkout page"),
        ("code", "def process_checkout(cart): validate(cart); charge(cart.total)"),
        ("data", "Checkout errors last week: 42 failures, mostly timeout"),
    ]
    
    # Add noise
    for i in range(30):
        contents.append(("misc", f"Random log entry {i}: system nominal"))
    
    for i, (ctype, content) in enumerate(contents):
        agent.remember(content, tags=[ctype, "workspace"])
        f = tmpdir / f"{ctype}_{i:02d}.md"
        f.write_text(f"# {ctype.title()} {i}\n\n{content}")
        files.append(f)
    
    # Query
    query = "checkout error payment"
    max_tokens = 250
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("mixed_content", "file", file_tokens, file_latency, "full"),
        ScenarioResult("mixed_content", "avm", avm_tokens, avm_latency, quality),
    ]


# ============================================================
# SCENARIO 5: Multi-turn Conversation
# ============================================================
def scenario_conversation(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: Agent in multi-turn conversation, needs to recall earlier context.
    - 30 conversation turns
    - User asks about something mentioned 20 turns ago
    """
    print("\n💬 Scenario 5: Multi-turn Conversation")
    print("-" * 40)
    
    agent_id = f"conv_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    # Simulate conversation
    turns = [
        "User: Hi, I'm planning to buy a new laptop",
        "Assistant: What's your budget and use case?",
        "User: Around $1500, mainly for programming",
        "Assistant: I recommend MacBook Air M3 or ThinkPad X1 Carbon",
        "User: What about the MacBook Pro?",
        "Assistant: MacBook Pro 14 is $1999, above your budget",
        "User: I also need good battery life",
        "Assistant: MacBook Air has 18 hours, ThinkPad has 15 hours",
        "User: Let me think about it. By the way, I'm learning Rust",
        "Assistant: Great choice! Rust has excellent performance",
        "User: Any book recommendations for Rust?",
        "Assistant: The Rust Programming Language (official book) is best",
        "User: Thanks. I also want to learn about databases",
        "Assistant: PostgreSQL is popular, also consider SQLite for embedded",
        "User: What's the difference?",
        "Assistant: PostgreSQL is client-server, SQLite is embedded file-based",
        "User: I'll start with SQLite then",
        "Assistant: Good choice for learning. It's used in many mobile apps",
        "User: Speaking of mobile, should I learn Swift or Kotlin?",
        "Assistant: Swift for iOS, Kotlin for Android. Both are modern languages",
        "User: I have an iPhone so maybe Swift",
        "Assistant: SwiftUI makes iOS development easier now",
        "User: Cool. Back to laptops - what was the battery comparison again?",
    ]
    
    for i, turn in enumerate(turns):
        agent.remember(turn, tags=["conversation", f"turn_{i}"])
        f = tmpdir / f"turn_{i:02d}.md"
        f.write_text(turn)
        files.append(f)
    
    # Query about earlier topic
    query = "laptop battery hours MacBook ThinkPad"
    max_tokens = 200
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("conversation", "file", file_tokens, file_latency, "full"),
        ScenarioResult("conversation", "avm", avm_tokens, avm_latency, quality),
    ]


# ============================================================
# SCENARIO 6: User Preferences
# ============================================================
def scenario_user_preferences(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: Agent remembering user preferences over time.
    - 50 preference entries
    - Query about specific preference
    """
    print("\n⚙️ Scenario 6: User Preferences")
    print("-" * 40)
    
    agent_id = f"prefs_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    # Various preferences
    prefs = [
        ("timezone", "User timezone is Europe/London"),
        ("language", "User prefers responses in English"),
        ("format", "User likes bullet points over paragraphs"),
        ("tone", "User prefers casual, friendly tone"),
        ("notifications", "User wants daily summaries at 9am"),
        ("theme", "User prefers dark mode"),
        ("units", "User uses metric system"),
        ("currency", "User's currency is GBP"),
        ("name", "User's name is Alex"),
        ("expertise", "User is senior developer"),
        ("interests", "User interested in AI, blockchain, gaming"),
        ("food", "User is vegetarian, allergic to nuts"),
        ("music", "User likes jazz and electronic music"),
        ("work", "User works at a fintech startup"),
        ("schedule", "User usually available 10am-6pm"),
    ]
    
    # Add noise
    for i in range(35):
        prefs.append((f"pref_{i}", f"Random preference setting {i}"))
    
    for i, (category, pref) in enumerate(prefs):
        agent.remember(pref, tags=["preference", category], importance=0.8)
        f = tmpdir / f"pref_{i:02d}.md"
        f.write_text(f"# {category}\n{pref}")
        files.append(f)
    
    # Query specific preference
    query = "vegetarian food allergic nuts"
    max_tokens = 150
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("user_preferences", "file", file_tokens, file_latency, "full"),
        ScenarioResult("user_preferences", "avm", avm_tokens, avm_latency, quality),
    ]


# ============================================================
# SCENARIO 7: Error Logs
# ============================================================
def scenario_error_logs(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: Finding specific error in large log history.
    - 500 log entries
    - Query about specific error
    """
    print("\n🔴 Scenario 7: Error Logs")
    print("-" * 40)
    
    agent_id = f"logs_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    log_types = [
        ("INFO", "Request processed successfully"),
        ("INFO", "User logged in"),
        ("INFO", "Cache hit for key"),
        ("DEBUG", "Processing batch job"),
        ("WARN", "Slow query detected"),
        ("INFO", "Scheduled task completed"),
    ]
    
    # Generate 500 logs
    for i in range(500):
        if i == 237:
            # The specific error we'll search for
            level, msg = "ERROR", "Payment gateway timeout: Stripe API returned 504 for transaction tx_abc123"
            tags = ["error", "payment", "stripe", "timeout"]
        else:
            level, msg = random.choice(log_types)
            tags = ["log", level.lower()]
        
        content = f"[2024-01-{(i%28)+1:02d} {(i%24):02d}:{(i%60):02d}] {level}: {msg}"
        agent.remember(content, tags=tags)
        f = tmpdir / f"log_{i:03d}.md"
        f.write_text(content)
        files.append(f)
    
    # Query about the specific error
    query = "payment stripe timeout error 504"
    max_tokens = 200
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("error_logs", "file", file_tokens, file_latency, "full"),
        ScenarioResult("error_logs", "avm", avm_tokens, avm_latency, quality),
    ]


# ============================================================
# SCENARIO 8: Knowledge Base FAQ
# ============================================================
def scenario_faq(avm: AVM, tmpdir: Path) -> List[ScenarioResult]:
    """
    Scenario: FAQ knowledge base lookup.
    - 100 FAQ entries
    - Query about specific topic
    """
    print("\n❓ Scenario 8: Knowledge Base FAQ")
    print("-" * 40)
    
    agent_id = f"faq_{int(_time.time())}"
    agent = avm.agent_memory(agent_id)
    files = []
    
    faqs = [
        ("How do I reset my password?", "Go to Settings > Account > Reset Password"),
        ("What payment methods are accepted?", "We accept Visa, Mastercard, PayPal"),
        ("How to cancel subscription?", "Go to Billing > Subscriptions > Cancel"),
        ("Is there a free trial?", "Yes, 14-day free trial for all plans"),
        ("How to export my data?", "Settings > Data > Export as CSV or JSON"),
        ("What is the refund policy?", "Full refund within 30 days of purchase"),
        ("How to enable 2FA?", "Security > Two-Factor > Enable with authenticator app"),
        ("API rate limits?", "Free: 100/min, Pro: 1000/min, Enterprise: unlimited"),
        ("How to upgrade plan?", "Billing > Plans > Select new plan"),
        ("Contact support?", "Email support@company.com or use in-app chat"),
    ]
    
    # Add more FAQs
    for i in range(90):
        faqs.append((f"FAQ question {i}?", f"Answer for FAQ {i} with some details"))
    
    for i, (q, a) in enumerate(faqs):
        content = f"Q: {q}\nA: {a}"
        agent.remember(content, tags=["faq", "support"])
        f = tmpdir / f"faq_{i:02d}.md"
        f.write_text(content)
        files.append(f)
    
    # Query
    query = "refund policy money back"
    max_tokens = 150
    
    file_tokens, file_latency = run_file_method(files, query)
    avm_tokens, avm_latency, quality = run_avm_method(agent, query, max_tokens)
    
    savings = (1 - avm_tokens / file_tokens) * 100 if file_tokens > 0 else 0
    print(f"   Query: '{query}'")
    print(f"   📁 File: {file_tokens} tokens ({file_latency:.1f}ms)")
    print(f"   🧠 AVM:  {avm_tokens} tokens ({avm_latency:.1f}ms) [{quality}]")
    print(f"   💰 Savings: {savings:.1f}%")
    
    return [
        ScenarioResult("faq", "file", file_tokens, file_latency, "full"),
        ScenarioResult("faq", "avm", avm_tokens, avm_latency, quality),
    ]


def main():
    print("=" * 60)
    print("Multi-Scenario Benchmark: File Read vs AVM Recall")
    print("=" * 60)
    
    avm = AVM()
    all_results = []
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Run all scenarios
        (tmpdir / "s1").mkdir()
        all_results.extend(scenario_recent_context(avm, tmpdir / "s1"))
        
        (tmpdir / "s2").mkdir()
        all_results.extend(scenario_code_exploration(avm, tmpdir / "s2"))
        
        (tmpdir / "s3").mkdir()
        all_results.extend(scenario_longterm_memory(avm, tmpdir / "s3"))
        
        (tmpdir / "s4").mkdir()
        all_results.extend(scenario_mixed_content(avm, tmpdir / "s4"))
        
        (tmpdir / "s5").mkdir()
        all_results.extend(scenario_conversation(avm, tmpdir / "s5"))
        
        (tmpdir / "s6").mkdir()
        all_results.extend(scenario_user_preferences(avm, tmpdir / "s6"))
        
        (tmpdir / "s7").mkdir()
        all_results.extend(scenario_error_logs(avm, tmpdir / "s7"))
        
        (tmpdir / "s8").mkdir()
        all_results.extend(scenario_faq(avm, tmpdir / "s8"))
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    scenarios = [
        "recent_context", "code_exploration", "longterm_memory", "mixed_content",
        "conversation", "user_preferences", "error_logs", "faq"
    ]
    
    total_file = 0
    total_avm = 0
    
    print(f"\n{'Scenario':<20} {'File':<12} {'AVM':<12} {'Savings':<10}")
    print("-" * 54)
    
    for scenario in scenarios:
        file_result = next(r for r in all_results if r.scenario == scenario and r.method == "file")
        avm_result = next(r for r in all_results if r.scenario == scenario and r.method == "avm")
        
        savings = (1 - avm_result.tokens_used / file_result.tokens_used) * 100 if file_result.tokens_used > 0 else 0
        
        print(f"{scenario:<20} {file_result.tokens_used:<12} {avm_result.tokens_used:<12} {savings:.1f}%")
        
        total_file += file_result.tokens_used
        total_avm += avm_result.tokens_used
    
    print("-" * 54)
    overall_savings = (1 - total_avm / total_file) * 100 if total_file > 0 else 0
    print(f"{'TOTAL':<20} {total_file:<12} {total_avm:<12} {overall_savings:.1f}%")
    
    print(f"\n💰 Overall Token Savings: {overall_savings:.1f}%")
    print(f"   Tokens saved: {total_file - total_avm}")
    
    # Cost estimate
    cost_per_token = 0.15 / 1_000_000
    print(f"\n💵 Estimated cost savings per 1000 runs: ${(total_file - total_avm) * cost_per_token * 1000:.4f}")
    
    print("\n✓ Benchmark complete!")


if __name__ == "__main__":
    main()
