# VFS Memory Skill

AI Virtual Filesystem for structured knowledge management.

## Installation

```bash
pip install -e .
```

## MCP Server

Start the VFS MCP server:

```bash
vfs-mcp --user akashi
```

Or with API key authentication:

```bash
vfs-mcp --api-key $VFS_API_KEY
```

## MCP Configuration

Add to your MCP config (e.g., `mcp_servers.yaml`):

```yaml
vfs-memory:
  command: vfs-mcp
  args:
    - --user
    - ${VFS_USER:-default}
  env:
    VFS_API_KEY: ${VFS_API_KEY}
```

## Tools

### vfs_recall

Search and retrieve relevant memories within a token budget.

```json
{
  "query": "NVDA risk analysis",
  "max_tokens": 4000,
  "strategy": "balanced"
}
```

Returns compact markdown with matching memories.

### vfs_remember

Store a new memory.

```json
{
  "content": "RSI > 70 indicates overbought",
  "title": "RSI Trading Rule",
  "importance": 0.8,
  "tags": ["trading", "indicators"]
}
```

### vfs_search

Full-text search across memories.

```json
{
  "query": "RSI",
  "limit": 10
}
```

### vfs_list

List memories in a path prefix.

```json
{
  "prefix": "/memory/shared/market",
  "limit": 20
}
```

### vfs_read

Read a specific memory by path.

```json
{
  "path": "/memory/private/akashi/rsi_rule.md"
}
```

### vfs_tags

Get tag frequency distribution.

```json
{
  "limit": 20
}
```

### vfs_recent

Get recent memories.

```json
{
  "time_range": "last_24h",
  "limit": 10
}
```

### vfs_stats

Get memory statistics.

```json
{}
```

## Usage Examples

### Agent Workflow

1. **Recall context before responding:**
   ```
   User: "What's the NVDA situation?"
   Agent: [calls vfs_recall("NVDA")]
   → Gets relevant memories about NVDA
   → Formulates response with context
   ```

2. **Store insights for future:**
   ```
   Agent: [calls vfs_remember("NVDA showing weakness...", tags=["market", "nvda"])]
   → Memory stored for future recall
   ```

3. **Build knowledge over time:**
   ```
   Agent: [calls vfs_remember(..., derived_from=["/memory/shared/market/NVDA.md"])]
   → Creates reasoning chain
   ```

## Permissions

The server respects VFS permissions:

- **Root**: Full access
- **Owner**: rwx on own files
- **Group**: Based on mode bits
- **API Keys**: Scoped access for skills

## Database

Default location: `~/.openclaw/vfs/vfs.db`

Override with `--db`:

```bash
vfs-mcp --db /path/to/custom.db
```
