"""
trading/providers.py — Trading-specific VFS Providers

Providers:
1. AlpacaPositionsProvider  — /trading/positions.md  (TTL=300s, live Alpaca API)
2. AlpacaAccountProvider    — /trading/account.md    (TTL=60s)
3. ResearchProvider         — /research/*.md         (TTL=86400s, local files)
"""

import sys, os, json, urllib.request, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from avm import AVMProvider, AVMNode


def _load_trading_env() -> dict:
    """Load .env from ~/.openclaw/workspace/trading/.env"""
    env_path = Path('~/.openclaw/workspace/trading/.env').expanduser()
    if not env_path.exists():
        raise FileNotFoundError(f"Trading .env not found: {env_path}")
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _alpaca_get(url: str, key: str, secret: str) -> dict:
    headers = {
        'APCA-API-KEY-ID': key,
        'APCA-API-SECRET-KEY': secret,
        'Accept': 'application/json',
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


# ── 1. AlpacaPositionsProvider ─────────────────────────────────────────────

class AlpacaPositionsProvider(AVMProvider):
    """
    Fetches live portfolio positions from Alpaca and formats as Markdown.
    Path: /trading/positions.md
    TTL:  300s (5 minutes)
    """
    pattern = '/trading/positions.md'
    ttl = 300

    def fetch(self, path: str, **kwargs) -> AVMNode | None:
        try:
            env = _load_trading_env()
            key    = env['ALPACA_API_KEY']
            secret = env['ALPACA_SECRET_KEY']
            base   = env['ALPACA_BASE_URL'].rstrip('/')

            acc       = _alpaca_get(f"{base}/v2/account", key, secret)
            positions = _alpaca_get(f"{base}/v2/positions", key, secret)

            pos_lines = '\n'.join(
                f"- **{p['symbol']}** {p['qty']}股 "
                f"@ ${float(p['avg_entry_price']):.2f} | "
                f"市value ${float(p['market_value']):,.2f} | "
                f"盈亏 ${float(p['unrealized_pl']):,.2f} "
                f"({float(p['unrealized_plpc'])*100:.1f}%)"
                for p in positions
            ) or '_null仓_'

            now_utc = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
            content = f"""# current持仓

**总资产**: ${float(acc['equity']):,.2f}
**现金**: ${float(acc['cash']):,.2f}
**当日盈亏**: ${float(acc.get('equity', 0)) - float(acc.get('last_equity', acc.get('equity', 0))):,.2f}
**买入力**: ${float(acc.get('buying_power', 0)):,.2f}
**updated_at**: {now_utc}

## 持仓明细

{pos_lines}
"""
            return AVMNode(
                path=path,
                content=content,
                raw_data={'account': acc, 'positions': positions},
                sources=['alpaca_api'],
                confidence=1.0,
            )
        except FileNotFoundError as e:
            return AVMNode(
                path=path,
                content=f'# 持仓\n\n⚠️ {e}\n\n请create `~/.openclaw/workspace/trading/.env`',
                sources=['alpaca_api'],
                confidence=0.0,
            )
        except Exception as e:
            return AVMNode(
                path=path,
                content=f'# 持仓\n\n❌ Alpaca API error: {e}',
                sources=['alpaca_api'],
                confidence=0.0,
            )


# ── 2. AlpacaAccountProvider ───────────────────────────────────────────────

class AlpacaAccountProvider(AVMProvider):
    """
    Fetches account summary from Alpaca.
    Path: /trading/account.md
    TTL:  60s
    """
    pattern = '/trading/account.md'
    ttl = 60

    def fetch(self, path: str, **kwargs) -> AVMNode | None:
        try:
            env = _load_trading_env()
            key    = env['ALPACA_API_KEY']
            secret = env['ALPACA_SECRET_KEY']
            base   = env['ALPACA_BASE_URL'].rstrip('/')

            acc = _alpaca_get(f"{base}/v2/account", key, secret)
            now_utc = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

            content = f"""# 账户摘要

| field | value |
|------|-----|
| 总资产 | ${float(acc['equity']):,.2f} |
| combine价value | ${float(acc.get('portfolio_value', acc['equity'])):,.2f} |
| 现金 | ${float(acc['cash']):,.2f} |
| 买入力（日内） | ${float(acc.get('daytrading_buying_power', 0)):,.2f} |
| 买入力（隔夜） | ${float(acc.get('regt_buying_power', 0)):,.2f} |
| 状态 | {acc.get('status', 'unknown')} |
| updated_at | {now_utc} |
"""
            return AVMNode(
                path=path,
                content=content,
                raw_data=acc,
                sources=['alpaca_api'],
                confidence=1.0,
            )
        except Exception as e:
            return AVMNode(
                path=path,
                content=f'# 账户\n\n❌ {e}',
                sources=['alpaca_api'],
                confidence=0.0,
            )


# ── 3. ResearchProvider ────────────────────────────────────────────────────

class ResearchProvider(AVMProvider):
    """
    Serves research reports from a local directory.
    VFS path /research/AAPL.md → <reports_dir>/AAPL.md
    TTL: 86400s (24h) — reports don't change often
    """
    pattern = '/research/*.md'
    ttl = 86400

    def __init__(self, reports_dir: str = '~/.openclaw/workspace/trading/research_reports'):
        self.reports_dir = Path(reports_dir).expanduser()

    def fetch(self, path: str, **kwargs) -> AVMNode | None:
        filename = path.split('/')[-1]
        file_path = self.reports_dir / filename

        if not file_path.exists():
            # Return a stub so callers know the path exists but has no data
            ticker = filename.replace('.md', '').upper()
            return AVMNode(
                path=path,
                content=f'# {ticker} Research\n\n_No report found. Place a file at `{file_path}`._',
                sources=['research_local'],
                confidence=0.0,
            )

        content = file_path.read_text(encoding='utf-8', errors='replace')
        return AVMNode(
            path=path,
            content=content,
            raw_data={'file': str(file_path)},
            sources=['research_local'],
            confidence=1.0,
        )

    def can_write(self) -> bool:
        return True

    def write(self, path: str, content: str, **kwargs) -> bool:
        filename = path.split('/')[-1]
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        (self.reports_dir / filename).write_text(content, encoding='utf-8')
        return True


# ── Registration ───────────────────────────────────────────────────────────

def register_providers(engine):
    """Mount all trading providers onto the VFS engine."""
    engine.mount(AlpacaPositionsProvider())
    engine.mount(AlpacaAccountProvider())
    engine.mount(ResearchProvider())
