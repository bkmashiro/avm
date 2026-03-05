# AI Virtual Filesystem (VFS)

让 AI Agent 通过文件路径读写结构化知识。

## 设计理念

- **对 Bot 接口是"文件"**：路径 + 内容，简单直观
- **内部是结构化存储**：SQLite + FTS5 + 关系图
- **权限硬编码**：`/memory` 可写，`/research` `/live` 只读

## 安装

```bash
cd ~/.openclaw/workspace/vfs
pip install -e .
```

## 使用

### 读取

```bash
# 读取 live 数据（自动刷新过期缓存）
vfs read /live/positions.md

# 强制刷新
vfs read /live/positions.md --refresh

# 读取 memory
vfs read /memory/lessons/001.md
```

### 写入

```bash
# 写入 memory（仅 /memory/* 可写）
vfs write /memory/lesson.md --content "今天学到了..."

# 从文件写入
vfs write /memory/report.md --file ./report.md

# 从 stdin 写入
echo "内容" | vfs write /memory/note.md
```

### 搜索

```bash
# 全文搜索
vfs search "能源板块超卖"

# 限制结果数
vfs search "RSI" --limit 5
```

### 关系图

```bash
# 查看关联
vfs links /research/MSFT.md

# 添加关联
vfs link /research/MSFT.md /research/AAPL.md --type peer

# 关联类型：peer, parent, citation, derived, related
```

### 其他

```bash
# 列出节点
vfs list /memory

# 查看历史
vfs history /memory/lesson.md

# 存储统计
vfs stats
```

## 路径设计

| 前缀 | 说明 | 权限 | TTL |
|------|------|------|-----|
| `/live` | 实时数据 | 只读 | 有 |
| `/research` | 静态研报 | 只读 | 无 |
| `/memory` | Bot 记忆 | 读写 | 无 |
| `/links` | 关系索引 | 只读 | 无 |

## 架构

```
Bot ←→ VFS CLI ←→ VFSStore ←→ SQLite
                     ↓
              ┌──────┴──────┐
              │   nodes     │ ← 节点内容
              │   nodes_fts │ ← FTS5 全文索引
              │   edges     │ ← 关系图
              │   diffs     │ ← 变更历史
              │   embeddings│ ← 向量（预留）
              └─────────────┘
```

## Provider

- `AlpacaPositionsProvider`: 从 Alpaca 获取持仓数据 → `/live/positions.md`
- `MemoryProvider`: Bot 记忆区 → `/memory/*`

## TODO

- [ ] sqlite-vec 向量搜索
- [ ] 更多 provider（新闻、财报、技术指标）
- [ ] 批量导入
- [ ] 过期数据清理
