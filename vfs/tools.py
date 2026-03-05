"""
vfs/tools.py - VFS 实用工具

批量导入、导出、同步等功能
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
import glob

from .store import VFSStore
from .node import VFSNode, NodeType
from .graph import EdgeType


class VFSImporter:
    """
    批量导入工具
    
    支持:
    - Markdown 文件导入
    - JSON 批量导入
    - 目录递归导入
    """
    
    def __init__(self, store: VFSStore):
        self.store = store
    
    def import_file(self, local_path: str, vfs_path: str = None,
                    meta: Dict = None) -> VFSNode:
        """
        导入单个文件
        
        Args:
            local_path: 本地文件路径
            vfs_path: VFS路径（默认: /research/filename）
            meta: 元数据
        """
        path = Path(local_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {local_path}")
        
        content = path.read_text()
        
        if vfs_path is None:
            vfs_path = f"/research/{path.name}"
        
        # 确保路径在允许的前缀下（导入用 /research）
        if not vfs_path.startswith("/research"):
            vfs_path = f"/research{vfs_path}" if vfs_path.startswith("/") else f"/research/{vfs_path}"
        
        node_meta = meta or {}
        node_meta["imported_from"] = str(path.absolute())
        node_meta["imported_at"] = datetime.utcnow().isoformat()
        
        node = VFSNode(
            path=vfs_path,
            content=content,
            meta=node_meta,
            node_type=NodeType.FILE,
        )
        
        return self.store._put_node_internal(node)
    
    def import_directory(self, local_dir: str, vfs_prefix: str = "/research",
                         pattern: str = "**/*.md",
                         flatten: bool = False) -> List[VFSNode]:
        """
        批量导入目录
        
        Args:
            local_dir: 本地目录
            vfs_prefix: VFS路径前缀
            pattern: glob 模式
            flatten: 是否展平目录结构
        """
        base = Path(local_dir)
        if not base.is_dir():
            raise NotADirectoryError(f"Not a directory: {local_dir}")
        
        nodes = []
        for file_path in base.glob(pattern):
            if not file_path.is_file():
                continue
            
            if flatten:
                vfs_path = f"{vfs_prefix}/{file_path.name}"
            else:
                rel_path = file_path.relative_to(base)
                vfs_path = f"{vfs_prefix}/{rel_path}"
            
            try:
                node = self.import_file(str(file_path), vfs_path)
                nodes.append(node)
            except Exception as e:
                print(f"Failed to import {file_path}: {e}")
        
        return nodes
    
    def import_json(self, json_path: str) -> List[VFSNode]:
        """
        从 JSON 批量导入
        
        JSON 格式:
        [
            {"path": "/research/a.md", "content": "...", "meta": {}},
            ...
        ]
        """
        with open(json_path) as f:
            data = json.load(f)
        
        nodes = []
        for item in data:
            node = VFSNode(
                path=item["path"],
                content=item.get("content", ""),
                meta=item.get("meta", {}),
                node_type=NodeType(item.get("node_type", "file")),
            )
            saved = self.store._put_node_internal(node)
            nodes.append(saved)
        
        return nodes


class VFSExporter:
    """
    导出工具
    """
    
    def __init__(self, store: VFSStore):
        self.store = store
    
    def export_to_json(self, prefix: str = "/", 
                       output_path: str = None) -> List[Dict]:
        """
        导出为 JSON
        """
        nodes = self.store.list_nodes(prefix, limit=10000)
        
        data = [n.to_dict() for n in nodes]
        
        if output_path:
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
        
        return data
    
    def export_to_directory(self, prefix: str, output_dir: str) -> int:
        """
        导出到目录（保持路径结构）
        """
        nodes = self.store.list_nodes(prefix, limit=10000)
        base = Path(output_dir)
        base.mkdir(parents=True, exist_ok=True)
        
        count = 0
        for node in nodes:
            # 转换路径
            rel_path = node.path.lstrip("/")
            file_path = base / rel_path
            
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(node.content)
            count += 1
        
        return count


class VFSSync:
    """
    同步工具
    
    保持本地目录和 VFS 的同步
    """
    
    def __init__(self, store: VFSStore):
        self.store = store
    
    def sync_from_local(self, local_dir: str, vfs_prefix: str,
                        delete_missing: bool = False) -> Dict[str, int]:
        """
        从本地目录同步到 VFS
        
        Returns: {"added": n, "updated": m, "deleted": k}
        """
        importer = VFSImporter(self.store)
        base = Path(local_dir)
        
        stats = {"added": 0, "updated": 0, "deleted": 0}
        local_files = set()
        
        for file_path in base.glob("**/*.md"):
            if not file_path.is_file():
                continue
            
            rel_path = file_path.relative_to(base)
            vfs_path = f"{vfs_prefix}/{rel_path}"
            local_files.add(vfs_path)
            
            existing = self.store.get_node(vfs_path)
            local_content = file_path.read_text()
            
            if existing is None:
                importer.import_file(str(file_path), vfs_path)
                stats["added"] += 1
            elif existing.content != local_content:
                importer.import_file(str(file_path), vfs_path)
                stats["updated"] += 1
        
        if delete_missing:
            vfs_nodes = self.store.list_nodes(vfs_prefix, limit=10000)
            for node in vfs_nodes:
                if node.path not in local_files:
                    # 只能删除 /memory 下的
                    if node.path.startswith("/memory"):
                        self.store.delete_node(node.path)
                        stats["deleted"] += 1
        
        return stats


class RelationBuilder:
    """
    关系构建工具
    
    自动发现和建立节点间的关系
    """
    
    def __init__(self, store: VFSStore):
        self.store = store
    
    def auto_link_by_symbol(self, prefix: str = "/") -> int:
        """
        根据内容中的股票代码自动建立关联
        """
        import re
        
        # 常见股票代码模式
        symbol_pattern = re.compile(r'\b([A-Z]{1,5})\b')
        
        nodes = self.store.list_nodes(prefix, limit=10000)
        links_added = 0
        
        # 收集每个 symbol 出现的节点
        symbol_nodes: Dict[str, List[str]] = {}
        
        for node in nodes:
            symbols = set(symbol_pattern.findall(node.content))
            # 过滤常见单词
            symbols -= {"THE", "AND", "FOR", "NOT", "BUT", "ARE", "WAS", "HAS"}
            
            for sym in symbols:
                if sym not in symbol_nodes:
                    symbol_nodes[sym] = []
                symbol_nodes[sym].append(node.path)
        
        # 建立同 symbol 节点之间的 peer 关系
        for sym, paths in symbol_nodes.items():
            if len(paths) < 2:
                continue
            
            for i, p1 in enumerate(paths):
                for p2 in paths[i+1:]:
                    self.store.add_edge(p1, p2, EdgeType.PEER, meta={"symbol": sym})
                    links_added += 1
        
        return links_added
    
    def link_by_tags(self) -> int:
        """
        根据标签建立关联
        """
        nodes = self.store.list_nodes("/", limit=10000)
        links_added = 0
        
        # 收集每个 tag 的节点
        tag_nodes: Dict[str, List[str]] = {}
        
        for node in nodes:
            tags = node.meta.get("tags", [])
            for tag in tags:
                if tag not in tag_nodes:
                    tag_nodes[tag] = []
                tag_nodes[tag].append(node.path)
        
        # 建立同 tag 节点间的关系
        for tag, paths in tag_nodes.items():
            if len(paths) < 2:
                continue
            
            for i, p1 in enumerate(paths):
                for p2 in paths[i+1:]:
                    self.store.add_edge(p1, p2, EdgeType.PEER, meta={"tag": tag})
                    links_added += 1
        
        return links_added
