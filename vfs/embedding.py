"""
vfs/embedding.py - Embedding 存储与语义搜索

支持多种 embedding 后端:
- OpenAI (text-embedding-3-small)
- Local (sentence-transformers)
- Custom
"""

import json
import struct
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path

from .store import VFSStore
from .node import VFSNode


class EmbeddingBackend(ABC):
    """Embedding 后端基类"""
    
    @property
    @abstractmethod
    def dimension(self) -> int:
        """向量维度"""
        pass
    
    @abstractmethod
    def embed(self, text: str) -> List[float]:
        """生成单个文本的 embedding"""
        pass
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """批量生成 embedding（默认逐个调用）"""
        return [self.embed(t) for t in texts]


class OpenAIEmbedding(EmbeddingBackend):
    """OpenAI Embedding"""
    
    DIMENSIONS = {
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
        "text-embedding-ada-002": 1536,
    }
    
    def __init__(self, model: str = "text-embedding-3-small", 
                 api_key: str = None):
        self.model = model
        self.api_key = api_key or self._load_api_key()
        self._dimension = self.DIMENSIONS.get(model, 1536)
    
    def _load_api_key(self) -> str:
        import os
        return os.environ.get("OPENAI_API_KEY", "")
    
    @property
    def dimension(self) -> int:
        return self._dimension
    
    def embed(self, text: str) -> List[float]:
        import urllib.request
        
        data = json.dumps({
            "input": text[:8000],  # 截断
            "model": self.model,
        }).encode()
        
        req = urllib.request.Request(
            "https://api.openai.com/v1/embeddings",
            data=data,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )
        
        with urllib.request.urlopen(req, timeout=30) as r:
            result = json.loads(r.read())
        
        return result["data"][0]["embedding"]
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        import urllib.request
        
        data = json.dumps({
            "input": [t[:8000] for t in texts],
            "model": self.model,
        }).encode()
        
        req = urllib.request.Request(
            "https://api.openai.com/v1/embeddings",
            data=data,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )
        
        with urllib.request.urlopen(req, timeout=60) as r:
            result = json.loads(r.read())
        
        # 按 index 排序
        embeddings = sorted(result["data"], key=lambda x: x["index"])
        return [e["embedding"] for e in embeddings]


class LocalEmbedding(EmbeddingBackend):
    """
    本地 Embedding (sentence-transformers)
    
    需要安装: pip install sentence-transformers
    """
    
    def __init__(self, model: str = "all-MiniLM-L6-v2"):
        self.model_name = model
        self._model = None
        self._dimension = None
    
    def _load_model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
            self._dimension = self._model.get_sentence_embedding_dimension()
    
    @property
    def dimension(self) -> int:
        if self._dimension is None:
            self._load_model()
        return self._dimension
    
    def embed(self, text: str) -> List[float]:
        self._load_model()
        return self._model.encode(text).tolist()
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        self._load_model()
        return self._model.encode(texts).tolist()


class EmbeddingStore:
    """
    Embedding 存储
    
    使用 SQLite 存储向量，支持余弦相似度搜索
    """
    
    def __init__(self, store: VFSStore, backend: EmbeddingBackend):
        self.store = store
        self.backend = backend
        self._init_table()
    
    def _init_table(self):
        """初始化向量表"""
        with self.store._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS embeddings (
                    path TEXT PRIMARY KEY,
                    vector BLOB NOT NULL,
                    content_hash TEXT,
                    model TEXT,
                    updated_at TEXT
                )
            """)
    
    def _serialize_vector(self, vec: List[float]) -> bytes:
        """序列化向量为 bytes"""
        return struct.pack(f'{len(vec)}f', *vec)
    
    def _deserialize_vector(self, data: bytes) -> List[float]:
        """反序列化 bytes 为向量"""
        count = len(data) // 4  # float = 4 bytes
        return list(struct.unpack(f'{count}f', data))
    
    def _content_hash(self, content: str) -> str:
        """计算内容哈希"""
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def embed_node(self, node: VFSNode, force: bool = False) -> bool:
        """
        为节点生成 embedding
        
        Returns: 是否实际生成了新的 embedding
        """
        content_hash = self._content_hash(node.content)
        
        # 检查是否需要更新
        if not force:
            with self.store._conn() as conn:
                row = conn.execute(
                    "SELECT content_hash FROM embeddings WHERE path = ?",
                    (node.path,)
                ).fetchone()
                if row and row[0] == content_hash:
                    return False  # 已存在且内容未变
        
        # 生成 embedding
        # 使用标题 + 内容前2000字
        text = f"{node.path}\n\n{node.content[:2000]}"
        vector = self.backend.embed(text)
        
        # 存储
        with self.store._conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO embeddings 
                    (path, vector, content_hash, model, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                node.path,
                self._serialize_vector(vector),
                content_hash,
                getattr(self.backend, 'model', 'unknown'),
                datetime.utcnow().isoformat(),
            ))
        
        return True
    
    def embed_all(self, prefix: str = "/", limit: int = 1000) -> int:
        """为所有节点生成 embedding"""
        nodes = self.store.list_nodes(prefix, limit)
        count = 0
        
        for node in nodes:
            if self.embed_node(node):
                count += 1
        
        return count
    
    def search(self, query: str, k: int = 5, 
               prefix: str = None) -> List[Tuple[VFSNode, float]]:
        """
        语义搜索
        
        Returns: [(node, similarity), ...]
        """
        # 生成查询向量
        query_vec = self.backend.embed(query)
        
        # 获取所有向量并计算相似度
        results = []
        
        with self.store._conn() as conn:
            sql = "SELECT path, vector FROM embeddings"
            params = []
            
            if prefix:
                sql += " WHERE path LIKE ?"
                params.append(prefix + "%")
            
            for row in conn.execute(sql, params):
                path = row[0]
                vec = self._deserialize_vector(row[1])
                
                # 余弦相似度
                similarity = self._cosine_similarity(query_vec, vec)
                results.append((path, similarity))
        
        # 排序取 top-k
        results.sort(key=lambda x: x[1], reverse=True)
        top_k = results[:k]
        
        # 获取完整节点
        final = []
        for path, sim in top_k:
            node = self.store.get_node(path)
            if node:
                final.append((node, sim))
        
        return final
    
    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """计算余弦相似度"""
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
        
        return dot / (norm_a * norm_b)
    
    def stats(self) -> Dict[str, Any]:
        """统计信息"""
        with self.store._conn() as conn:
            count = conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
            
            models = {}
            for row in conn.execute(
                "SELECT model, COUNT(*) FROM embeddings GROUP BY model"
            ):
                models[row[0] or "unknown"] = row[1]
        
        return {
            "embedded_nodes": count,
            "by_model": models,
            "backend": type(self.backend).__name__,
            "dimension": self.backend.dimension,
        }
