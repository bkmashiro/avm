#!/usr/bin/env python3
"""
vfs/mcp_server.py - MCP Server for VFS

Exposes VFS functionality as MCP tools for AI agents.

Usage:
    vfs-mcp --api-key $VFS_API_KEY
    vfs-mcp --config /path/to/config.yaml

Tools:
    - avm_recall: Retrieve relevant memories
    - avm_remember: Store new memory
    - avm_search: Full-text search
    - avm_list: List memories
    - avm_read: Read specific memory
    - avm_tags: Get tag cloud
    - avm_recent: Get recent memories
"""

import os
import sys
import json
import argparse
from typing import Any, Dict, List, Optional
from datetime import datetime


# MCP Protocol Implementation
class MCPServer:
    """
    MCP (Model Context Protocol) Server
    
    Implements the MCP protocol for stdio communication.
    """
    
    def __init__(self, vfs, user):
        self.vfs = vfs
        self.user = user
        self.memory = vfs.agent_memory(user.name)
        
        # Register tools
        self.tools = {
            "avm_recall": self._tool_recall,
            "avm_remember": self._tool_remember,
            "avm_search": self._tool_search,
            "avm_list": self._tool_list,
            "avm_read": self._tool_read,
            "avm_tags": self._tool_tags,
            "avm_recent": self._tool_recent,
            "avm_stats": self._tool_stats,
            # Two-phase retrieval
            "avm_browse": self._tool_browse,
            "avm_fetch": self._tool_fetch,
        }
    
    def get_tool_definitions(self) -> List[Dict]:
        """Return tool definitions for MCP"""
        return [
            {
                "name": "avm_recall",
                "description": "Search and retrieve relevant memories within a token budget. Returns a compact markdown summary of matching memories.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query to find relevant memories"
                        },
                        "max_tokens": {
                            "type": "number",
                            "description": "Maximum tokens in response (default: 4000)",
                            "default": 4000
                        },
                        "time_range": {
                            "type": "string",
                            "description": "Time filter: last_1h, last_24h, last_7d, last_30d",
                            "enum": ["last_1h", "last_24h", "last_7d", "last_30d"]
                        },
                        "strategy": {
                            "type": "string",
                            "description": "Scoring strategy: balanced, importance, recency, relevance",
                            "enum": ["balanced", "importance", "recency", "relevance"],
                            "default": "balanced"
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "avm_remember",
                "description": "Store a new memory. Automatically handles deduplication and linking.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "Memory content to store"
                        },
                        "title": {
                            "type": "string",
                            "description": "Optional title for the memory"
                        },
                        "importance": {
                            "type": "number",
                            "description": "Importance score 0-1 (default: 0.5)",
                            "minimum": 0,
                            "maximum": 1,
                            "default": 0.5
                        },
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Tags for categorization"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Shared namespace (e.g., 'market', 'projects')"
                        },
                        "derived_from": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Source paths this memory is derived from"
                        }
                    },
                    "required": ["content"]
                }
            },
            {
                "name": "avm_search",
                "description": "Full-text search across memories. Returns matching paths and snippets.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query"
                        },
                        "limit": {
                            "type": "number",
                            "description": "Maximum results (default: 10)",
                            "default": 10
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "avm_list",
                "description": "List memories in a path prefix.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "prefix": {
                            "type": "string",
                            "description": "Path prefix (default: user's private memory)",
                            "default": ""
                        },
                        "limit": {
                            "type": "number",
                            "description": "Maximum results (default: 20)",
                            "default": 20
                        }
                    }
                }
            },
            {
                "name": "avm_read",
                "description": "Read a specific memory by path.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Full path to the memory"
                        }
                    },
                    "required": ["path"]
                }
            },
            {
                "name": "avm_tags",
                "description": "Get tag cloud showing tag frequencies.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "number",
                            "description": "Maximum tags to return (default: 20)",
                            "default": 20
                        }
                    }
                }
            },
            {
                "name": "avm_recent",
                "description": "Get recent memories within a time range.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "time_range": {
                            "type": "string",
                            "description": "Time range: last_1h, last_24h, last_7d, last_30d",
                            "enum": ["last_1h", "last_24h", "last_7d", "last_30d"],
                            "default": "last_24h"
                        },
                        "limit": {
                            "type": "number",
                            "description": "Maximum results (default: 10)",
                            "default": 10
                        }
                    }
                }
            },
            {
                "name": "avm_stats",
                "description": "Get memory statistics for the current user.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "avm_browse",
                "description": "Browse memories - returns paths and short summaries only (not full content). Use this first to find relevant memories, then use avm_fetch to get full content of selected paths. Saves tokens for large result sets.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query"
                        },
                        "limit": {
                            "type": "number",
                            "description": "Maximum results (default: 10)",
                            "default": 10
                        },
                        "summary_length": {
                            "type": "number",
                            "description": "Characters per summary (default: 80)",
                            "default": 80
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "avm_fetch",
                "description": "Fetch full content of specific memory paths. Use after avm_browse to get complete content of selected memories.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "paths": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of paths to fetch"
                        }
                    },
                    "required": ["paths"]
                }
            }
        ]
    
    # ─── Tool Implementations ─────────────────────────────
    
    def _tool_recall(self, params: Dict) -> str:
        """Retrieve relevant memories"""
        query = params.get("query", "")
        max_tokens = params.get("max_tokens", 4000)
        time_range = params.get("time_range")
        strategy = params.get("strategy", "balanced")
        
        from .agent_memory import ScoringStrategy
        
        if time_range:
            return self.memory.recall_recent(
                query, 
                time_range=time_range,
                max_tokens=max_tokens
            )
        else:
            return self.memory.recall(
                query,
                max_tokens=max_tokens,
                strategy=ScoringStrategy(strategy)
            )
    
    def _tool_remember(self, params: Dict) -> str:
        """Store new memory"""
        content = params.get("content", "")
        title = params.get("title")
        importance = params.get("importance", 0.5)
        tags = params.get("tags", [])
        namespace = params.get("namespace")
        derived_from = params.get("derived_from")
        
        if derived_from:
            node = self.memory.remember_derived(
                content,
                derived_from=derived_from,
                title=title,
                importance=importance,
                tags=tags,
                namespace=namespace,
            )
        else:
            node = self.memory.remember(
                content,
                title=title,
                importance=importance,
                tags=tags,
                namespace=namespace,
            )
        
        return f"Stored: {node.path}"
    
    def _tool_search(self, params: Dict) -> str:
        """Full-text search"""
        query = params.get("query", "")
        limit = params.get("limit", 10)
        
        results = self.vfs.search(query, limit=limit)
        
        lines = [f"Found {len(results)} results for '{query}':\n"]
        for node, score in results:
            snippet = node.content[:100].replace("\n", " ")
            lines.append(f"- [{score:.2f}] {node.path}")
            lines.append(f"  {snippet}...")
            lines.append("")
        
        return "\n".join(lines)
    
    def _tool_list(self, params: Dict) -> str:
        """List memories"""
        prefix = params.get("prefix") or self.memory.private_prefix
        limit = params.get("limit", 20)
        
        nodes = self.vfs.list(prefix, limit=limit)
        
        lines = [f"Memories in {prefix} ({len(nodes)} items):\n"]
        for node in nodes:
            tags = node.meta.get("tags", [])
            tag_str = f" [{', '.join(tags)}]" if tags else ""
            lines.append(f"- {node.path}{tag_str}")
        
        return "\n".join(lines)
    
    def _tool_read(self, params: Dict) -> str:
        """Read specific memory"""
        path = params.get("path", "")
        
        node = self.vfs.read(path)
        if not node:
            return f"Not found: {path}"
        
        return f"# {path}\n\n{node.content}"
    
    def _tool_tags(self, params: Dict) -> str:
        """Get tag cloud"""
        limit = params.get("limit", 20)
        
        cloud = self.memory.tag_cloud()
        
        lines = ["Tag Cloud:\n"]
        for tag, count in list(cloud.items())[:limit]:
            lines.append(f"- {tag}: {count}")
        
        return "\n".join(lines)
    
    def _tool_recent(self, params: Dict) -> str:
        """Get recent memories"""
        time_range = params.get("time_range", "last_24h")
        limit = params.get("limit", 10)
        
        nodes = self.vfs.query_time(
            prefix="/memory",
            time_range=time_range,
            limit=limit
        )
        
        lines = [f"Recent memories ({time_range}):\n"]
        for node in nodes:
            created = node.meta.get("created_at", "")[:19]
            lines.append(f"- [{created}] {node.path}")
        
        return "\n".join(lines)
    
    def _tool_stats(self, params: Dict) -> str:
        """Get statistics"""
        stats = self.memory.stats()
        
        lines = [
            "Memory Statistics:",
            f"- Agent: {stats['agent_id']}",
            f"- Private memories: {stats['private_count']}",
            f"- Shared accessible: {stats['shared_accessible']}",
            f"- Max tokens: {stats['config']['max_tokens']}",
            f"- Strategy: {stats['config']['strategy']}",
        ]
        
        return "\n".join(lines)
    
    def _tool_browse(self, params: Dict) -> str:
        """Browse memories - paths and summaries only"""
        query = params.get("query", "")
        limit = params.get("limit", 10)
        summary_length = params.get("summary_length", 80)
        
        # Use retrieve for semantic + FTS search
        result = self.vfs.retrieve(query, k=limit, expand_graph=True)
        
        if not result.nodes:
            return f"No memories found for: {query}"
        
        lines = [
            f"Found {len(result.nodes)} memories for \"{query}\":",
            f"(Use avm_fetch to get full content)",
            ""
        ]
        
        for node in result.nodes:
            score = result.get_score(node.path)
            source = result.get_source(node.path)
            
            # Create short summary
            content = node.content.replace("\n", " ").strip()
            # Skip markdown headers and metadata
            content = ' '.join([
                line for line in content.split()
                if not line.startswith('#') and not line.startswith('*')
            ])
            summary = content[:summary_length]
            if len(content) > summary_length:
                summary += "..."
            
            # Source badge
            badge = {"semantic": "🎯", "fts": "📝", "graph": "🔗"}.get(source, "")
            
            lines.append(f"{badge} [{score:.2f}] {node.path}")
            lines.append(f"    {summary}")
            
            # Add tags if present
            tags = node.meta.get("tags", [])
            if tags:
                lines.append(f"    Tags: {', '.join(tags)}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def _tool_fetch(self, params: Dict) -> str:
        """Fetch full content of specific paths"""
        paths = params.get("paths", [])
        
        if not paths:
            return "No paths specified"
        
        contents = []
        not_found = []
        
        for path in paths:
            node = self.vfs.read(path)
            if node:
                contents.append(f"## {path}\n\n{node.content}")
            else:
                not_found.append(path)
        
        result = "\n\n---\n\n".join(contents)
        
        if not_found:
            result += f"\n\n*Not found: {', '.join(not_found)}*"
        
        return result
    
    # ─── MCP Protocol ─────────────────────────────────────
    
    def handle_request(self, request: Dict) -> Dict:
        """Handle MCP request"""
        method = request.get("method", "")
        params = request.get("params", {})
        request_id = request.get("id")
        
        try:
            if method == "initialize":
                return self._handle_initialize(request_id, params)
            elif method == "tools/list":
                return self._handle_tools_list(request_id)
            elif method == "tools/call":
                return self._handle_tools_call(request_id, params)
            else:
                return self._error_response(request_id, -32601, f"Unknown method: {method}")
        except Exception as e:
            return self._error_response(request_id, -32000, str(e))
    
    def _handle_initialize(self, request_id: Any, params: Dict) -> Dict:
        """Handle initialize request"""
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "serverInfo": {
                    "name": "avm-memory",
                    "version": "0.7.0"
                },
                "capabilities": {
                    "tools": {}
                }
            }
        }
    
    def _handle_tools_list(self, request_id: Any) -> Dict:
        """Handle tools/list request"""
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": self.get_tool_definitions()
            }
        }
    
    def _handle_tools_call(self, request_id: Any, params: Dict) -> Dict:
        """Handle tools/call request"""
        tool_name = params.get("name", "")
        tool_params = params.get("arguments", {})
        
        if tool_name not in self.tools:
            return self._error_response(request_id, -32602, f"Unknown tool: {tool_name}")
        
        result = self.tools[tool_name](tool_params)
        
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": result
                    }
                ]
            }
        }
    
    def _error_response(self, request_id: Any, code: int, message: str) -> Dict:
        """Create error response"""
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": code,
                "message": message
            }
        }
    
    def run_stdio(self):
        """Run MCP server on stdio"""
        import sys
        
        while True:
            try:
                line = sys.stdin.readline()
                if not line:
                    break
                
                request = json.loads(line)
                response = self.handle_request(request)
                
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
                
            except json.JSONDecodeError:
                continue
            except KeyboardInterrupt:
                break


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="VFS MCP Server")
    parser.add_argument("--api-key", "-k", help="API key for authentication")
    parser.add_argument("--config", "-c", help="Config file path")
    parser.add_argument("--db", "-d", help="Database path")
    parser.add_argument("--user", "-u", default="default", help="User name")
    
    args = parser.parse_args()
    
    # Initialize VFS
    from . import VFS
    from .config import AVMConfig
    
    config = AVMConfig(db_path=args.db) if args.db else None
    vfs = VFS(config=config)
    
    # Initialize permissions and authenticate
    vfs.init_permissions()
    
    if args.api_key:
        user = vfs.authenticate(args.api_key)
        if not user:
            # Create user if not exists
            user = vfs.create_user(args.user)
    else:
        # Use default user
        user = vfs.create_user(args.user)
    
    # Run server
    server = MCPServer(vfs, user)
    server.run_stdio()


if __name__ == "__main__":
    main()
