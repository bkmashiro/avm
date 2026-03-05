"""
vfs/providers/watchlist.py - watchlistcolumntable Provider

Aggregate multiple data sources, generate watchlist overview
"""

from datetime import datetime
from typing import Optional, List, Dict

from .base import LiveProvider
from .indicators import TechnicalIndicatorsProvider
from ..node import AVMNode
from ..store import AVMStore


class WatchlistProvider(LiveProvider):
    """
    Watchlist overview
    
    path:
        /live/watchlist.md              - defaultcolumntable
        /live/watchlist/tech.md         - tech stocks
        /live/watchlist/value.md        - value stocks
        /live/watchlist/custom.md       - custom
    """
    
    # Preset watchlist
    WATCHLISTS = {
        "default": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL"],
        "tech": ["AAPL", "MSFT", "NVDA", "AMD", "INTC", "AVGO", "QCOM", "TSM"],
        "value": ["BRK-B", "JPM", "JNJ", "PG", "KO", "WMT", "XOM", "CVX"],
        "crypto": ["COIN", "MARA", "RIOT", "MSTR", "SQ", "PYPL"],
    }
    
    def __init__(self, store: AVMStore, 
                 custom_symbols: List[str] = None,
                 ttl_seconds: int = 300):
        super().__init__(store, "/live/watchlist", ttl_seconds)
        self.indicators_provider = TechnicalIndicatorsProvider(store, ttl_seconds)
        self.custom_symbols = custom_symbols or []
    
    def fetch(self, path: str) -> Optional[AVMNode]:
        name = path.replace("/live/watchlist", "").replace(".md", "").strip("/")
        
        if not name:
            name = "default"
        
        if name == "custom":
            symbols = self.custom_symbols
        else:
            symbols = self.WATCHLISTS.get(name, self.WATCHLISTS["default"])
        
        if not symbols:
            return self._make_node(
                path,
                f"# Watchlist: {name}\n\nNo symbols configured.",
                {"name": name, "symbols": []}
            )
        
        return self._fetch_watchlist(path, name, symbols)
    
    def _fetch_watchlist(self, path: str, name: str, 
                         symbols: List[str]) -> AVMNode:
        """getwatchlistdata"""
        lines = [
            f"# Watchlist: {name.title()}",
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
            "",
            "## Quick Overview",
            "",
            "| Symbol | Price | RSI | MACD | Trend | Signal |",
            "|--------|-------|-----|------|-------|--------|",
        ]
        
        symbol_data = []
        
        for symbol in symbols:
            try:
                data = self.indicators_provider._fetch_yahoo_data(symbol)
                closes = data["closes"]
                price = data["current_price"]
                
                rsi = self.indicators_provider._calc_rsi(closes)
                macd = self.indicators_provider._calc_macd(closes)
                sma20 = self.indicators_provider._calc_sma(closes, 20)
                sma50 = self.indicators_provider._calc_sma(closes, 50)
                
                # Trend judgment
                trend = "—"
                if sma20 and sma50:
                    trend = "🟢" if sma20 > sma50 else "🔴"
                
                # Signals
                signals = []
                if rsi:
                    if rsi < 30:
                        signals.append("oversold")
                    elif rsi > 70:
                        signals.append("overbought")
                
                if macd and macd["cross"] == "golden":
                    signals.append("golden cross")
                elif macd and macd["cross"] == "death":
                    signals.append("death cross")
                
                signal_str = "/".join(signals) if signals else "—"
                macd_emoji = ""
                if macd:
                    if macd["histogram"] > 0:
                        macd_emoji = "📈"
                    else:
                        macd_emoji = "📉"
                
                rsi_str = f"{rsi:.0f}" if rsi else "—"
                lines.append(
                    f"| {symbol} | ${price:.2f} | {rsi_str} | {macd_emoji} | {trend} | {signal_str} |"
                )
                
                symbol_data.append({
                    "symbol": symbol,
                    "price": price,
                    "rsi": rsi,
                    "trend": "bullish" if trend == "🟢" else "beenarish" if trend == "🔴" else "neutral",
                    "signals": signals,
                })
                
            except Exception as e:
                lines.append(f"| {symbol} | Error | — | — | — | {str(e)[:20]} |")
                symbol_data.append({"symbol": symbol, "error": str(e)})
        
        # adddetailedanalysis
        lines.extend([
            "",
            "## Alerts",
            "",
        ])
        
        oversold = [s for s in symbol_data if "oversold" in s.get("signals", [])]
        overbought = [s for s in symbol_data if "overbought" in s.get("signals", [])]
        golden = [s for s in symbol_data if "golden cross" in s.get("signals", [])]
        death = [s for s in symbol_data if "death cross" in s.get("signals", [])]
        
        if oversold:
            lines.append(f"🟢 **Oversold:** {', '.join(s['symbol'] for s in oversold)}")
        if overbought:
            lines.append(f"🔴 **Overbought:** {', '.join(s['symbol'] for s in overbought)}")
        if golden:
            lines.append(f"✅ **Golden Cross:** {', '.join(s['symbol'] for s in golden)}")
        if death:
            lines.append(f"❌ **Death Cross:** {', '.join(s['symbol'] for s in death)}")
        
        if not any([oversold, overbought, golden, death]):
            lines.append("No significant alerts.")
        
        return self._make_node(
            path,
            "\n".join(lines),
            {
                "name": name,
                "symbols": symbols,
                "data": symbol_data,
            }
        )
    
    def set_custom_watchlist(self, symbols: List[str]):
        """settingscustomwatchlist"""
        self.custom_symbols = symbols
