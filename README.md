# VFS - AI Virtual Filesystem

A config-driven virtual filesystem for AI agents to read/write structured knowledge via file paths.

## Install

```bash
pip install -e .
```

## Quick Start

```python
from vfs import VFS

vfs = VFS()

# Read/Write
vfs.write("/memory/lesson1.md", "# Trading Lesson\n\nBe cautious when RSI > 70")
node = vfs.read("/memory/lesson1.md")

# Search
results = vfs.search("RSI")

# Link nodes
vfs.link("/memory/lesson1.md", "/market/indicators/NVDA.md", "related_to")
```

## Core Features

### 1. Config-Driven

```yaml
# config.yaml
providers:
  - pattern: "/live/positions*"
    type: alpaca_positions
    ttl: 60
  - pattern: "/live/indicators/*"
    type: technical_indicators
    ttl: 300

permissions:
  - pattern: "/memory/*"
    access: rw
  - pattern: "/live/*"
    access: ro

default_access: ro
```

```python
vfs = VFS(config_path="config.yaml")
```

### 2. Linked Retrieval

Semantic search + full-text search + graph expansion:

```python
# Enable semantic search (optional)
vfs.enable_embedding(model="text-embedding-3-small")
vfs.embed_all()

# Linked retrieval
result = vfs.retrieve("NVDA risk", expand_graph=True)
for node in result.nodes:
    print(f"{result.get_source(node.path)} {node.path}")

# 🎯 /market/indicators/NVDA.md  (semantic match)
# 📝 /memory/lessons/nvda.md     (keyword match)
# 🔗 /market/indicators/AMD.md   (graph expansion)
```

### 3. Dynamic Document Synthesis

Aggregate related nodes into a structured document:

```python
doc = vfs.synthesize("NVDA risk analysis")
print(doc)
```

Output:
```markdown
# NVDA risk analysis (auto-generated)

## Technical Indicators
> 🎯 Source: `/market/indicators/NVDA.md`
RSI: 72 (overbought warning), MACD: death cross forming

## Historical Lessons
> 📝 Source: `/memory/lessons/nvda.md`
Last time RSI > 70, price dropped 15%

## Related Assets
> 🔗 Source: `/market/indicators/AMD.md`
AMD RSI: 65, correlation: 0.85
```

### 4. Agent Memory

Token-aware memory retrieval with multi-agent isolation:

```python
memory = vfs.agent_memory("akashi")

# Write memory
memory.remember(
    "Be cautious when RSI > 70, NVDA dropped 15% last time",
    importance=0.8,
    tags=["trading", "risk"]
)

# Token-aware retrieval
context = memory.recall(
    "NVDA risk",
    max_tokens=4000,
    strategy="balanced"  # importance/recency/relevance/balanced
)
print(context)
```

Output:
```markdown
## Relevant Memory (2 items, ~150 tokens)

[/memory/private/akashi/nvda_lesson.md] (0.85) Be cautious when RSI > 70, NVDA dropped 15% last time

[/memory/shared/trading/risk_rules.md] (0.72) Position size < 15%, always set stop-loss

---
*Tokens: ~150/4000 | Strategy: balanced | Query: "NVDA risk"*
```

#### Path Structure

```
/memory/private/{agent_id}/*   # Private memory
/memory/shared/{namespace}/*   # Shared space
```

#### Scoring Strategies

| Strategy | Description |
|----------|-------------|
| `importance` | By node importance (0-1) |
| `recency` | By last access time (exponential decay, half-life: 1 week) |
| `relevance` | By semantic/keyword relevance |
| `balanced` | Weighted combination (default: relevance 0.5 + importance 0.3 + recency 0.2) |

## CLI

```bash
# Read/Write
vfs read /memory/lesson1.md
vfs write /memory/lesson1.md "content"

# Search
vfs search "RSI"
vfs retrieve "NVDA risk" --depth 2

# Dynamic document
vfs synthesize "NVDA risk analysis"

# Agent Memory
vfs recall "RSI" --agent akashi --max-tokens 2000
vfs remember --agent akashi -c "lesson content" -i 0.8 --tags "trading"
vfs memory-stats --agent akashi

# Links
vfs links /memory/lesson1.md
vfs link /a.md /b.md related_to

# Management
vfs list /memory
vfs history /memory/lesson1.md
vfs refresh /live/indicators/*
vfs config
vfs stats
```

## Provider Types

| Type | Description |
|------|-------------|
| `alpaca_positions` | Alpaca positions |
| `alpaca_orders` | Alpaca orders |
| `technical_indicators` | Technical indicators (Yahoo Finance) |
| `news` | News (RSS) |
| `watchlist` | Watchlist |
| `memory` | Local memory |
| `http_json` | Generic HTTP JSON |

### Custom Provider

```python
from vfs import VFS, register_provider_type
from vfs.providers.base import BaseProvider

class MyProvider(BaseProvider):
    def fetch(self, path: str, params: dict) -> str:
        return f"# Data for {path}\n\nCustom content here"

register_provider_type("my_provider", MyProvider)

# Use in config:
# providers:
#   - pattern: "/custom/*"
#     type: my_provider
```

## Config Examples

### Trading Bot

```yaml
# trading_bot.yaml
providers:
  - pattern: "/live/positions*"
    type: alpaca_positions
    ttl: 60
    params:
      api_key: ${ALPACA_API_KEY}
      api_secret: ${ALPACA_API_SECRET}

  - pattern: "/live/indicators/*"
    type: technical_indicators
    ttl: 300

permissions:
  - pattern: "/memory/private/*"
    access: rw
  - pattern: "/memory/shared/*"
    access: rw
  - pattern: "/live/*"
    access: ro

retrieval:
  default_max_tokens: 4000
  scoring_weights:
    importance: 0.3
    recency: 0.2
    relevance: 0.5
```

### Home Assistant

```yaml
# home_assistant.yaml
providers:
  - pattern: "/devices/*"
    type: http_json
    ttl: 30
    params:
      base_url: ${HA_URL}/api/states
      headers:
        Authorization: "Bearer ${HA_TOKEN}"

  - pattern: "/automations/*"
    type: memory
    ttl: 0

permissions:
  - pattern: "/devices/*"
    access: ro
  - pattern: "/automations/*"
    access: rw
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                      VFS API                        │
│  read() write() search() retrieve() synthesize()   │
├─────────────────────────────────────────────────────┤
│                   AgentMemory                       │
│  recall() remember() share() (token-aware)         │
├─────────────────────────────────────────────────────┤
│                    Retriever                        │
│  Semantic + FTS + Graph Expansion + Synthesis       │
├─────────────────────────────────────────────────────┤
│                   VFSConfig                         │
│  Providers / Permissions / YAML loading             │
├─────────────────────────────────────────────────────┤
│                   VFSStore                          │
│  SQLite + FTS5 + Diff Tracking                      │
├──────────────────────┬──────────────────────────────┤
│      KVGraph         │        EmbeddingStore        │
│  Adjacency List      │   Cosine Similarity Search   │
├──────────────────────┴──────────────────────────────┤
│                    Providers                        │
│  Alpaca | Indicators | News | HTTP | Memory | ...   │
└─────────────────────────────────────────────────────┘
```

## Versions

- **v0.4.0** - Agent Memory (token-aware recall)
- **v0.3.0** - Linked Retrieval + Document Synthesis
- **v0.2.0** - Config-driven providers/permissions
- **v0.1.0** - Core VFS (read/write/search/links)

## License

MIT
