"""
providers.py — Built-in VFS Providers

Three example providers:
1. StaticFileProvider  — serve local files under a mapped directory
2. HTTPProvider        — fetch from an HTTP URL template
3. FunctionProvider    — wrap any Python callable as a provider
"""

import time, urllib.request
from pathlib import Path
from typing import Callable, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent))
from avm import AVMProvider, AVMNode


# ── 1. StaticFileProvider ─────────────────────────────────────────────────

class StaticFileProvider(AVMProvider):
    """
    Map VFS paths like /static/foo.md to real files on disk.

    Example:
        provider = StaticFileProvider(
            pattern='/static/*',
            base_dir='/home/user/docs',
        )
        # /static/notes.md → /home/user/docs/notes.md
    """

    def __init__(self, pattern: str = '/static/*', base_dir: str = '.', ttl: int = 0):
        self.pattern = pattern
        self.base_dir = Path(base_dir).expanduser().resolve()
        self.ttl = ttl   # 0 = never expires (files don't change often)

    def fetch(self, path: str, **kwargs) -> Optional[AVMNode]:
        # Strip the pattern prefix to get the relative file path
        prefix = self.pattern.rstrip('*').rstrip('/')
        rel = path[len(prefix):].lstrip('/')
        file_path = self.base_dir / rel

        if not file_path.exists():
            return None

        content = file_path.read_text(encoding='utf-8', errors='replace')
        return AVMNode(
            path=path,
            content=content,
            raw_data={'file': str(file_path)},
            sources=['local_file'],
            confidence=1.0,
        )

    def can_write(self) -> bool:
        return True

    def write(self, path: str, content: str, **kwargs) -> bool:
        prefix = self.pattern.rstrip('*').rstrip('/')
        rel = path[len(prefix):].lstrip('/')
        file_path = self.base_dir / rel
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding='utf-8')
        return True


# ── 2. HTTPProvider ───────────────────────────────────────────────────────

class HTTPProvider(AVMProvider):
    """
    Fetch content from an HTTP URL template.

    The URL template may contain {path} which is replaced with the VFS path.

    Example:
        provider = HTTPProvider(
            pattern='/wiki/*',
            url_template='https://en.wikipedia.org/api/rest_v1/page/summary/{slug}',
            path_to_kwargs=lambda p: {'slug': p.split('/')[-1]},
            ttl=3600,
        )
    """

    def __init__(
        self,
        pattern: str = '/http/*',
        url_template: str = 'https://example.com/{path}',
        headers: Optional[dict] = None,
        ttl: int = 3600,
        content_extractor: Optional[Callable] = None,
    ):
        self.pattern = pattern
        self.url_template = url_template
        self.headers = headers or {}
        self.ttl = ttl
        # Optional: transform raw response bytes → Markdown string
        self.content_extractor = content_extractor or (lambda b: b.decode('utf-8', errors='replace'))

    def fetch(self, path: str, **kwargs) -> Optional[AVMNode]:
        url = self.url_template.format(path=path.lstrip('/'), **kwargs)
        try:
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
            content = self.content_extractor(raw)
            return AVMNode(
                path=path,
                content=content,
                raw_data={'url': url, 'status': resp.status},
                sources=['http'],
                confidence=0.9,
            )
        except Exception as exc:
            # Return error node so callers know something went wrong
            return AVMNode(
                path=path,
                content=f'[HTTPProvider error] {exc}',
                sources=['http'],
                confidence=0.0,
            )


# ── 3. FunctionProvider ───────────────────────────────────────────────────

class FunctionProvider(AVMProvider):
    """
    Wrap any Python function as a VFS provider. The most flexible option.

    The function receives `path` and must return either:
      - a string   → used as content
      - a AVMNode  → used directly
      - None       → cache miss

    Example:
        def my_fn(path):
            ticker = path.split('/')[-1].replace('.md', '')
            return f"# {ticker}\\nLast price: $123.45"

        provider = FunctionProvider(
            pattern='/prices/*.md',
            fn=my_fn,
            ttl=60,
        )
    """

    def __init__(self, pattern: str, fn: Callable, ttl: int = 300, sources: Optional[list] = None):
        self.pattern = pattern
        self.fn = fn
        self.ttl = ttl
        self._sources = sources or ['function']

    def fetch(self, path: str, **kwargs) -> Optional[AVMNode]:
        try:
            result = self.fn(path, **kwargs)
        except Exception as exc:
            return AVMNode(
                path=path,
                content=f'[FunctionProvider error] {exc}',
                sources=self._sources,
                confidence=0.0,
            )

        if result is None:
            return None
        if isinstance(result, AVMNode):
            return result
        # Assume string content
        return AVMNode(
            path=path,
            content=str(result),
            sources=self._sources,
            confidence=1.0,
        )
