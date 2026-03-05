"""
vfs/providers/news.py - news Provider

Fetch financial news from public RSS/API
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Dict
import urllib.request
import urllib.parse

from .base import LiveProvider
from ..node import AVMNode
from ..store import AVMStore


class NewsProvider(LiveProvider):
    """
    financialnewsdata
    
    path:
        /live/news/market.md       - marketnews
        /live/news/AAPL.md         - stockrelatednews
        /live/news/crypto.md       - cryptocurrencynews
    """
    
    # RSS 源
    RSS_SOURCES = {
        "market": [
            ("Yahoo Finance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^DJI,^GSPC,^IXIC&region=US&lang=en-US"),
            ("Investing.com", "https://www.investing.com/rss/market_overview_Ede.rss"),
        ],
        "crypto": [
            ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
        ],
    }
    
    def __init__(self, store: AVMStore, ttl_seconds: int = 600):
        super().__init__(store, "/live/news", ttl_seconds)
    
    def fetch(self, path: str) -> Optional[AVMNode]:
        parts = path.replace("/live/news/", "").replace(".md", "")
        
        try:
            if parts == "market":
                return self._fetch_market_news()
            elif parts == "crypto":
                return self._fetch_crypto_news()
            elif parts.isupper():  # Stock symbol
                return self._fetch_stock_news(parts)
        except Exception as e:
            return self._make_node(
                path,
                f"# Error\n\nFailed to fetch news: {e}",
                {"error": str(e)}
            )
        
        return None
    
    def _fetch_rss(self, url: str, limit: int = 10) -> List[Dict]:
        """Fetch RSS content"""
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; VFS/1.0)",
            })
            with urllib.request.urlopen(req, timeout=10) as r:
                content = r.read()
            
            root = ET.fromstring(content)
            items = []
            
            for item in root.findall(".//item")[:limit]:
                title = item.findtext("title", "")
                link = item.findtext("link", "")
                pub_date = item.findtext("pubDate", "")
                description = item.findtext("description", "")
                
                # cleanup description
                if description:
                    description = description[:200].replace("<", "&lt;").replace(">", "&gt;")
                
                items.append({
                    "title": title,
                    "link": link,
                    "date": pub_date,
                    "description": description,
                })
            
            return items
        except Exception:
            return []
    
    def _fetch_market_news(self) -> AVMNode:
        """getmarketnews"""
        all_items = []
        
        for source_name, url in self.RSS_SOURCES.get("market", []):
            items = self._fetch_rss(url, limit=5)
            for item in items:
                item["source"] = source_name
            all_items.extend(items)
        
        lines = [
            "# Market News",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
            "",
        ]
        
        for item in all_items[:15]:
            lines.append(f"### {item['title']}")
            lines.append(f"*{item.get('source', 'Unknown')} | {item['date'][:25] if item['date'] else 'N/A'}*")
            if item["description"]:
                lines.append(f"\n{item['description']}...")
            if item["link"]:
                lines.append(f"\n[Read more]({item['link']})")
            lines.append("")
        
        return self._make_node(
            "/live/news/market.md",
            "\n".join(lines),
            {"item_count": len(all_items)}
        )
    
    def _fetch_crypto_news(self) -> AVMNode:
        """getcryptocurrencynews"""
        all_items = []
        
        for source_name, url in self.RSS_SOURCES.get("crypto", []):
            items = self._fetch_rss(url, limit=10)
            for item in items:
                item["source"] = source_name
            all_items.extend(items)
        
        lines = [
            "# Crypto News",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
            "",
        ]
        
        for item in all_items[:10]:
            lines.append(f"### {item['title']}")
            lines.append(f"*{item.get('source', 'Unknown')} | {item['date'][:25] if item['date'] else 'N/A'}*")
            if item["description"]:
                lines.append(f"\n{item['description']}...")
            lines.append("")
        
        return self._make_node(
            "/live/news/crypto.md",
            "\n".join(lines),
            {"item_count": len(all_items)}
        )
    
    def _fetch_stock_news(self, symbol: str) -> AVMNode:
        """Fetch stock-related news (Yahoo Finance RSS)"""
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        items = self._fetch_rss(url, limit=10)
        
        lines = [
            f"# {symbol} News",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
            "",
        ]
        
        if not items:
            lines.append("No recent news found.")
        else:
            for item in items:
                lines.append(f"### {item['title']}")
                lines.append(f"*{item['date'][:25] if item['date'] else 'N/A'}*")
                if item["description"]:
                    lines.append(f"\n{item['description']}...")
                if item["link"]:
                    lines.append(f"\n[Read more]({item['link']})")
                lines.append("")
        
        return self._make_node(
            f"/live/news/{symbol}.md",
            "\n".join(lines),
            {"symbol": symbol, "item_count": len(items)}
        )
