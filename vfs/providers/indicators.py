"""
vfs/providers/indicators.py - technical indicators Provider

Fetch data from Yahoo Finance and calculate technical indicators
"""

import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import urllib.request

from .base import LiveProvider
from ..node import VFSNode
from ..store import VFSStore


class TechnicalIndicatorsProvider(LiveProvider):
    """
    technical indicatorsdata
    
    path:
        /live/indicators/AAPL.md       - 单stockcomplete指标
        /live/indicators/AAPL/rsi.md   - RSI
        /live/indicators/AAPL/macd.md  - MACD
        /live/indicators/AAPL/ma.md    - moving average线
        /live/indicators/AAPL/bb.md    - Bollinger Bands
    """
    
    def __init__(self, store: VFSStore, ttl_seconds: int = 300):
        super().__init__(store, "/live/indicators", ttl_seconds)
    
    def fetch(self, path: str) -> Optional[VFSNode]:
        parts = path.replace("/live/indicators/", "").split("/")
        if not parts or not parts[0]:
            return None
        
        symbol = parts[0].replace(".md", "").upper()
        indicator = parts[1].replace(".md", "") if len(parts) > 1 else None
        
        try:
            data = self._fetch_yahoo_data(symbol)
            if indicator:
                return self._make_indicator_node(symbol, indicator, data)
            else:
                return self._make_full_report(symbol, data)
        except Exception as e:
            return self._make_node(
                path,
                f"# Error\n\nFailed to fetch {symbol}: {e}",
                {"error": str(e), "symbol": symbol}
            )
    
    def _fetch_yahoo_data(self, symbol: str, days: int = 120) -> Dict[str, Any]:
        """Fetch historical data from Yahoo Finance"""
        end = int(datetime.now().timestamp())
        start = end - days * 86400
        
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            f"?interval=1d&period1={start}&period2={end}"
        )
        
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        
        result = data.get("chart", {}).get("result", [{}])[0]
        quote = result.get("indicators", {}).get("quote", [{}])[0]
        
        closes = [c for c in quote.get("close", []) if c is not None]
        highs = [h for h in quote.get("high", []) if h is not None]
        lows = [l for l in quote.get("low", []) if l is not None]
        volumes = [v for v in quote.get("volume", []) if v is not None]
        
        return {
            "symbol": symbol,
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
            "current_price": closes[-1] if closes else 0,
        }
    
    def _calc_rsi(self, closes: List[float], period: int = 14) -> Optional[float]:
        if len(closes) < period + 1:
            return None
        
        gains = []
        losses = []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(diff if diff > 0 else 0)
            losses.append(-diff if diff < 0 else 0)
        
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
    
    def _calc_ema(self, closes: List[float], period: int) -> Optional[float]:
        if len(closes) < period:
            return None
        k = 2 / (period + 1)
        ema = sum(closes[:period]) / period
        for price in closes[period:]:
            ema = price * k + ema * (1 - k)
        return ema
    
    def _calc_sma(self, closes: List[float], period: int) -> Optional[float]:
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period
    
    def _calc_macd(self, closes: List[float]) -> Optional[Dict[str, float]]:
        if len(closes) < 35:
            return None
        
        ema12 = self._calc_ema(closes, 12)
        ema26 = self._calc_ema(closes, 26)
        
        if ema12 is None or ema26 is None:
            return None
        
        macd_line = ema12 - ema26
        
        # calculate MACD historyfor Signal
        macd_history = []
        for i in range(26, len(closes) + 1):
            e12 = self._calc_ema(closes[:i], 12)
            e26 = self._calc_ema(closes[:i], 26)
            if e12 and e26:
                macd_history.append(e12 - e26)
        
        if len(macd_history) < 9:
            return None
        
        # Signal = EMA9 of MACD
        k = 2 / 10
        signal = sum(macd_history[:9]) / 9
        for m in macd_history[9:]:
            signal = m * k + signal * (1 - k)
        
        histogram = macd_line - signal
        
        # 检测golden cross/death cross
        prev_macd = macd_history[-2] if len(macd_history) > 1 else None
        prev_hist = None
        if prev_macd and len(macd_history) > 2:
            prev_k = 2 / 10
            prev_signal = sum(macd_history[:9]) / 9
            for m in macd_history[9:-1]:
                prev_signal = m * prev_k + prev_signal * (1 - prev_k)
            prev_hist = prev_macd - prev_signal
        
        cross = "none"
        if prev_hist is not None:
            if prev_hist < 0 and histogram > 0:
                cross = "golden"  # golden cross
            elif prev_hist > 0 and histogram < 0:
                cross = "death"   # death cross
        
        return {
            "macd": macd_line,
            "signal": signal,
            "histogram": histogram,
            "cross": cross,
        }
    
    def _calc_bollinger(self, closes: List[float], period: int = 20, 
                        num_std: float = 2) -> Optional[Dict[str, float]]:
        if len(closes) < period:
            return None
        
        recent = closes[-period:]
        sma = sum(recent) / period
        variance = sum((x - sma) ** 2 for x in recent) / period
        std = variance ** 0.5
        
        upper = sma + num_std * std
        lower = sma - num_std * std
        
        price = closes[-1]
        bb_pct = (price - lower) / (upper - lower) if upper != lower else 0.5
        
        return {
            "upper": upper,
            "middle": sma,
            "lower": lower,
            "width": (upper - lower) / sma,
            "percent": bb_pct,
        }
    
    def _calc_atr(self, highs: List[float], lows: List[float], 
                  closes: List[float], period: int = 14) -> Optional[float]:
        if len(closes) < period + 1:
            return None
        
        trs = []
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            trs.append(tr)
        
        if len(trs) < period:
            return None
        
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        
        return atr
    
    def _make_full_report(self, symbol: str, data: Dict) -> VFSNode:
        closes = data["closes"]
        highs = data["highs"]
        lows = data["lows"]
        price = data["current_price"]
        
        rsi = self._calc_rsi(closes)
        macd = self._calc_macd(closes)
        bb = self._calc_bollinger(closes)
        atr = self._calc_atr(highs, lows, closes)
        sma20 = self._calc_sma(closes, 20)
        sma50 = self._calc_sma(closes, 50)
        ema12 = self._calc_ema(closes, 12)
        ema26 = self._calc_ema(closes, 26)
        
        lines = [
            f"# {symbol} Technical Indicators",
            "",
            f"**Current Price:** ${price:.2f}",
            "",
            "## RSI (14)",
            f"- **Value:** {rsi:.1f}" if rsi else "- N/A",
        ]
        
        if rsi:
            if rsi < 30:
                lines.append("- **Signal:** 🟢 Oversold (potential buy)")
            elif rsi > 70:
                lines.append("- **Signal:** 🔴 Overbought (potential sell)")
            else:
                lines.append("- **Signal:** ⚪ Neutral")
        
        lines.extend([
            "",
            "## MACD",
        ])
        
        if macd:
            lines.extend([
                f"- **MACD Line:** {macd['macd']:.4f}",
                f"- **Signal Line:** {macd['signal']:.4f}",
                f"- **Histogram:** {macd['histogram']:.4f}",
            ])
            if macd["cross"] == "golden":
                lines.append("- **Signal:** 🟢 Golden Cross (bullish)")
            elif macd["cross"] == "death":
                lines.append("- **Signal:** 🔴 Death Cross (bearish)")
            else:
                lines.append("- **Signal:** ⚪ No crossover")
        else:
            lines.append("- N/A")
        
        lines.extend([
            "",
            "## Moving Averages",
            f"- **SMA 20:** ${sma20:.2f}" if sma20 else "- SMA 20: N/A",
            f"- **SMA 50:** ${sma50:.2f}" if sma50 else "- SMA 50: N/A",
            f"- **EMA 12:** ${ema12:.2f}" if ema12 else "- EMA 12: N/A",
            f"- **EMA 26:** ${ema26:.2f}" if ema26 else "- EMA 26: N/A",
        ])
        
        if sma20 and sma50:
            if sma20 > sma50:
                lines.append("- **Trend:** 🟢 SMA20 > SMA50 (bullish)")
            else:
                lines.append("- **Trend:** 🔴 SMA20 < SMA50 (bearish)")
        
        if price and sma50:
            if price > sma50:
                lines.append(f"- **Price vs SMA50:** 🟢 Above (+{(price/sma50-1)*100:.1f}%)")
            else:
                lines.append(f"- **Price vs SMA50:** 🔴 Below ({(price/sma50-1)*100:.1f}%)")
        
        lines.extend([
            "",
            "## Bollinger Bands",
        ])
        
        if bb:
            lines.extend([
                f"- **Upper:** ${bb['upper']:.2f}",
                f"- **Middle:** ${bb['middle']:.2f}",
                f"- **Lower:** ${bb['lower']:.2f}",
                f"- **Width:** {bb['width']*100:.1f}%",
                f"- **%B:** {bb['percent']*100:.1f}%",
            ])
            if bb["percent"] < 0.2:
                lines.append("- **Signal:** 🟢 Near lower band (potential bounce)")
            elif bb["percent"] > 0.8:
                lines.append("- **Signal:** 🔴 Near upper band (potential pullback)")
        else:
            lines.append("- N/A")
        
        lines.extend([
            "",
            "## Volatility",
            f"- **ATR (14):** ${atr:.2f}" if atr else "- ATR: N/A",
        ])
        
        if atr and price:
            lines.append(f"- **ATR %:** {atr/price*100:.2f}%")
        
        lines.extend([
            "",
            f"*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*",
        ])
        
        return self._make_node(
            f"/live/indicators/{symbol}.md",
            "\n".join(lines),
            {
                "symbol": symbol,
                "price": price,
                "rsi": rsi,
                "macd": macd,
                "bb": bb,
                "atr": atr,
            }
        )
    
    def _make_indicator_node(self, symbol: str, indicator: str, 
                             data: Dict) -> VFSNode:
        closes = data["closes"]
        highs = data["highs"]
        lows = data["lows"]
        price = data["current_price"]
        
        if indicator == "rsi":
            rsi = self._calc_rsi(closes)
            content = f"# {symbol} RSI\n\n"
            content += f"**RSI (14):** {rsi:.1f}\n" if rsi else "N/A\n"
            if rsi:
                if rsi < 30:
                    content += "**Status:** Oversold\n"
                elif rsi > 70:
                    content += "**Status:** Overbought\n"
                else:
                    content += "**Status:** Neutral\n"
            meta = {"rsi": rsi}
        
        elif indicator == "macd":
            macd = self._calc_macd(closes)
            content = f"# {symbol} MACD\n\n"
            if macd:
                content += f"**MACD:** {macd['macd']:.4f}\n"
                content += f"**Signal:** {macd['signal']:.4f}\n"
                content += f"**Histogram:** {macd['histogram']:.4f}\n"
                content += f"**Cross:** {macd['cross']}\n"
            else:
                content += "N/A\n"
            meta = {"macd": macd}
        
        elif indicator == "ma":
            sma20 = self._calc_sma(closes, 20)
            sma50 = self._calc_sma(closes, 50)
            ema12 = self._calc_ema(closes, 12)
            ema26 = self._calc_ema(closes, 26)
            content = f"# {symbol} Moving Averages\n\n"
            content += f"**SMA 20:** ${sma20:.2f}\n" if sma20 else ""
            content += f"**SMA 50:** ${sma50:.2f}\n" if sma50 else ""
            content += f"**EMA 12:** ${ema12:.2f}\n" if ema12 else ""
            content += f"**EMA 26:** ${ema26:.2f}\n" if ema26 else ""
            content += f"**Price:** ${price:.2f}\n"
            meta = {"sma20": sma20, "sma50": sma50, "ema12": ema12, "ema26": ema26}
        
        elif indicator == "bb":
            bb = self._calc_bollinger(closes)
            content = f"# {symbol} Bollinger Bands\n\n"
            if bb:
                content += f"**Upper:** ${bb['upper']:.2f}\n"
                content += f"**Middle:** ${bb['middle']:.2f}\n"
                content += f"**Lower:** ${bb['lower']:.2f}\n"
                content += f"**%B:** {bb['percent']*100:.1f}%\n"
            else:
                content += "N/A\n"
            meta = {"bb": bb}
        
        else:
            content = f"# {symbol}\n\nUnknown indicator: {indicator}\n"
            meta = {}
        
        content += f"\n*Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*\n"
        
        return self._make_node(
            f"/live/indicators/{symbol}/{indicator}.md",
            content,
            {"symbol": symbol, "indicator": indicator, **meta}
        )
