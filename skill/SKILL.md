# AVM Memory Skill

AI Virtual Memory for agents. Token-aware recall, knowledge graphs, multi-agent support.

## Quick Start

### 1. Mount AVM for an Agent

```bash
# Mount to agent workspace
avm-mount ~/.openclaw/workspace-{agent}/avm/ --agent {agent} --daemon

# Verify
ls ~/.openclaw/workspace-{agent}/avm/
```

### 2. Basic Usage

```bash
# Store a memory
echo "RSI > 70 indicates overbought" > avm/notes/rsi_rule.md

# Recall with query
cat "avm/:recall?q=RSI trading"

# See what's in memory
cat avm/:topics

# Browse structure
cat avm/:list
```

### 3. Virtual Paths

| Path | Description |
|------|-------------|
| `avm/:recall?q=...` | Semantic search |
| `avm/:topics` | Topic overview |
| `avm/:list` | List all memories |
| `avm/:handlers` | Available handlers |
| `avm/:handlers/{name}` | Specific handler docs |
| `avm/notes/*.md` | Read/write memories |

## For Sub-Agents

When spawning a sub-agent that needs memory:

### Step 1: Mount AVM

```bash
avm-mount ~/.openclaw/workspace-{subagent}/avm/ \
  --agent {subagent} \
  --db ~/.local/share/avm/avm.db \
  --daemon
```

### Step 2: Add to AGENTS.md

```markdown
## Memory

This agent has AVM memory mounted at `avm/`.

### Recall
\`\`\`bash
cat "avm/:recall?q=your query"
\`\`\`

### Remember
\`\`\`bash
echo "content" > avm/notes/title.md
\`\`\`

### Topics
\`\`\`bash
cat avm/:topics
\`\`\`
```

### Step 3: Verify

```bash
# Test recall
cat "avm/:recall?q=test"
```

## Commands

```bash
# Mount (foreground)
avm-mount mount /path --agent {id}

# Mount (daemon)
avm-mount mount /path --agent {id} --daemon

# Stop
avm-mount stop /path

# Status
avm-mount status

# Restart
avm-mount restart /path --agent {id}

# Import existing memories
avm import /path/to/*.md --agent {id}

# CLI operations
avm recall "query" --agent {id}
avm remember "content" --title "name" --agent {id}
avm topics --agent {id}
```

## Multi-Agent Setup

### Shared Namespace

Multiple agents can share memories:

```bash
# Agent A writes to shared
echo "market signal" > avm/shared/market/signal.md

# Agent B reads shared
cat "avm/:recall?q=market signal"  # Finds it
```

### Private Memories

Each agent's writes go to their private namespace by default:

```
avm/
├── private/{agent}/*.md   # Only this agent
└── shared/{namespace}/*.md # All agents
```

## Tips

1. **Use recall, not search** - `cat avm/:recall?q=...` ranks by relevance + importance
2. **Set importance** - High importance memories surface first
3. **Use tags** - Add tags via file content: `tags: [trading, risk]`
4. **Link memories** - Use `avm link` to build knowledge graph
5. **Check topics first** - `cat avm/:topics` to see what's available

## Troubleshooting

### Mount fails
```bash
# Check if already mounted
mount | grep avm

# Force unmount
fusermount -uz /path/to/mount
```

### Permission denied
```bash
# Check ownership
ls -la ~/.local/share/avm/

# Fix if needed
chown -R $USER ~/.local/share/avm/
```

### No results
```bash
# Check if memories exist
avm list --agent {id}

# Check topics
cat avm/:topics
```
