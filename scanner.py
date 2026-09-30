import asyncio
import json
import logging
import os
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import websockets

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DEX_BASE = "https://api.dexscreener.com"
DB_PATH = Path(os.getenv("DB_PATH", "pump_genie.db"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_SECONDS = max(15, int(os.getenv("POLL_SECONDS", "30")))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes"}
CHAIN_IDS = {x.strip().lower() for x in os.getenv("CHAIN_IDS", "solana").split(",") if x.strip()}
ENABLE_PUMP_STREAM = os.getenv("ENABLE_PUMP_STREAM", "true").lower() in {"1", "true", "yes"}
PUMP_WS_URL = os.getenv("PUMP_WS_URL", "wss://pumpdev.io/ws").strip()

MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "15000"))
MIN_VOLUME_H1_USD = float(os.getenv("MIN_VOLUME_H1_USD", "10000"))
MIN_TXNS_H1 = int(os.getenv("MIN_TXNS_H1", "80"))
MIN_BUY_SELL_RATIO = float(os.getenv("MIN_BUY_SELL_RATIO", "1.10"))
MIN_PAIR_AGE_MINUTES = float(os.getenv("MIN_PAIR_AGE_MINUTES", "3"))
MAX_PAIR_AGE_HOURS = float(os.getenv("MAX_PAIR_AGE_HOURS", "24"))
MAX_FDV_LIQUIDITY_RATIO = float(os.getenv("MAX_FDV_LIQUIDITY_RATIO", "40"))
MAX_ABS_PRICE_CHANGE_H1 = float(os.getenv("MAX_ABS_PRICE_CHANGE_H1", "400"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "65"))
MAX_CANDIDATES_PER_CYCLE = int(os.getenv("MAX_CANDIDATES_PER_CYCLE", "50"))
CANDIDATE_TTL_MINUTES = int(os.getenv("CANDIDATE_TTL_MINUTES", "180"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("pump-genie")


@dataclass
class Candidate:
    chain: str
    mint: str
    source: str
    discovered_at: float
    name: str = ""
    symbol: str = ""
    last_checked: float = 0


@dataclass
class Evaluation:
    passed: bool
    score: int
    reasons: list[str]
    failures: list[str]
    warnings: list[str]
    pair: dict[str, Any]


class PumpLaunchFeed:
    def __init__(self, outbox: queue.Queue[Candidate]) -> None:
        self.outbox = outbox

    async def listen(self) -> None:
        delay = 2
        while True:
            try:
                async with websockets.connect(PUMP_WS_URL, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    log.info("Pump.fun launch feed connected")
                    delay = 2
                    async for raw in ws:
                        try:
                            event = json.loads(raw)
                        except (TypeError, json.JSONDecodeError):
                            continue
                        mint = event.get("mint") or event.get("tokenAddress")
                        if not mint:
                            continue
                        candidate = Candidate(
                            chain="solana",
                            mint=mint,
                            source="pump.fun live",
                            discovered_at=time.time(),
                            name=event.get("name") or "",
                            symbol=event.get("symbol") or "",
                        )
                        try:
                            self.outbox.put_nowait(candidate)
                        except queue.Full:
                            log.warning("Pump feed queue full; dropping %s", mint)
            except Exception as exc:
                log.warning("Pump feed disconnected (%s); reconnecting in %ss", exc, delay)
                await asyncio.sleep(delay)
                delay = min(60, delay * 2)

    def start(self) -> None:
        threading.Thread(target=lambda: asyncio.run(self.listen()), daemon=True, name="pump-feed").start()


class PumpGenie:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "PumpGenie/2.0"})
        self.db = sqlite3.connect(DB_PATH)
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS alerts_v2 (
                chain TEXT NOT NULL, mint TEXT NOT NULL, symbol TEXT, score INTEGER,
                first_alerted_at INTEGER, last_alerted_at INTEGER, payload TEXT,
                PRIMARY KEY(chain, mint)
            )"""
        )
        self.db.commit()
        self.inbox: queue.Queue[Candidate] = queue.Queue(maxsize=10000)
        self.candidates: dict[tuple[str, str], Candidate] = {}
        self.webhook_enabled = False
        self.dry_run_enabled = DRY_RUN

    def get_json(self, url: str) -> Any:
        for attempt in range(4):
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if response.status_code == 429:
                    delay = min(60, 2 ** (attempt + 2))
                    log.warning("Rate limited; sleeping %ss", delay)
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                if attempt == 3:
                    raise
                delay = 2 ** attempt
                log.warning("Request failed (%s); retrying in %ss", exc, delay)
                time.sleep(delay)
        return None

    def discover_dex(self) -> list[Candidate]:
        endpoints = [
            ("DEX profile", f"{DEX_BASE}/token-profiles/latest/v1"),
            ("DEX boost", f"{DEX_BASE}/token-boosts/latest/v1"),
        ]
        found: list[Candidate] = []
        seen: set[tuple[str, str]] = set()
        for source, endpoint in endpoints:
            try:
                rows = self.get_json(endpoint) or []
            except requests.RequestException as exc:
                log.error("Discovery endpoint failed: %s", exc)
                continue
            if isinstance(rows, dict):
                rows = rows.get("data", [])
            for row in rows:
                chain = str(row.get("chainId") or "").lower()
                mint = row.get("tokenAddress")
                key = (chain, mint)
                if chain not in CHAIN_IDS or not mint or key in seen:
                    continue
                seen.add(key)
                found.append(Candidate(chain, mint, source, time.time()))
        return found

    def drain_live_feed(self) -> int:
        count = 0
        while True:
            try:
                item = self.inbox.get_nowait()
            except queue.Empty:
                break
            self.candidates[(item.chain, item.mint)] = item
            count += 1
        return count

    def fetch_pairs(self, chain: str, mint: str) -> list[dict[str, Any]]:
        try:
            rows = self.get_json(f"{DEX_BASE}/token-pairs/v1/{chain}/{mint}") or []
        except requests.RequestException as exc:
            log.warning("Pair lookup failed for %s:%s: %s", chain, mint, exc)
            return []
        return rows if isinstance(rows, list) else rows.get("pairs", [])

    @staticmethod
    def best_pair(pairs: list[dict[str, Any]], chain: str | None = None) -> dict[str, Any] | None:
        valid = [p for p in pairs if not chain or str(p.get("chainId", "")).lower() == chain.lower()]
        if not valid:
            return None
        return max(valid, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))

    @staticmethod
    def evaluate(pair: dict[str, Any], now_ms: int | None = None) -> Evaluation:
        now_ms = now_ms or int(time.time() * 1000)
        liq = float((pair.get("liquidity") or {}).get("usd") or 0)
        vol_h1 = float((pair.get("volume") or {}).get("h1") or 0)
        tx_h1 = (pair.get("txns") or {}).get("h1") or {}
        buys, sells = int(tx_h1.get("buys") or 0), int(tx_h1.get("sells") or 0)
        txns, ratio = buys + sells, buys / max(sells, 1)
        fdv = float(pair.get("fdv") or pair.get("marketCap") or 0)
        fdv_liq = fdv / liq if liq > 0 and fdv > 0 else 0
        change_h1 = float((pair.get("priceChange") or {}).get("h1") or 0)
        created = int(pair.get("pairCreatedAt") or now_ms)
        age_minutes = max(0.0, (now_ms - created) / 60000)
        info = pair.get("info") or {}
        websites, socials = info.get("websites") or [], info.get("socials") or []

        failures: list[str] = []
        if liq < MIN_LIQUIDITY_USD: failures.append(f"Liquidity ${liq:,.0f} below ${MIN_LIQUIDITY_USD:,.0f}")
        if vol_h1 < MIN_VOLUME_H1_USD: failures.append(f"1h volume ${vol_h1:,.0f} below ${MIN_VOLUME_H1_USD:,.0f}")
        if txns < MIN_TXNS_H1: failures.append(f"1h transactions {txns} below {MIN_TXNS_H1}")
        if ratio < MIN_BUY_SELL_RATIO: failures.append(f"Buy/sell ratio {ratio:.2f} below {MIN_BUY_SELL_RATIO:.2f}")
        if age_minutes < MIN_PAIR_AGE_MINUTES: failures.append(f"Pair age {age_minutes:.1f}m below {MIN_PAIR_AGE_MINUTES:.1f}m")
        if age_minutes > MAX_PAIR_AGE_HOURS * 60: failures.append(f"Pair age {age_minutes/60:.1f}h above {MAX_PAIR_AGE_HOURS:.1f}h")
        if fdv_liq > MAX_FDV_LIQUIDITY_RATIO: failures.append(f"FDV/liquidity {fdv_liq:.1f} above {MAX_FDV_LIQUIDITY_RATIO:.1f}")
        if abs(change_h1) > MAX_ABS_PRICE_CHANGE_H1: failures.append(f"1h move {change_h1:+.0f}% is extreme")

        score, reasons = 0, []
        if liq >= 75000: score += 23; reasons.append(f"Strong liquidity ${liq:,.0f}")
        elif liq >= 30000: score += 18; reasons.append(f"Good liquidity ${liq:,.0f}")
        elif liq >= MIN_LIQUIDITY_USD: score += 11; reasons.append(f"Liquidity threshold met ${liq:,.0f}")
        if vol_h1 >= 100000: score += 23; reasons.append(f"High 1h volume ${vol_h1:,.0f}")
        elif vol_h1 >= 30000: score += 18; reasons.append(f"Healthy 1h volume ${vol_h1:,.0f}")
        elif vol_h1 >= MIN_VOLUME_H1_USD: score += 11; reasons.append(f"Volume threshold met ${vol_h1:,.0f}")
        if txns >= 500: score += 18; reasons.append(f"High activity {txns} txns")
        elif txns >= 200: score += 13; reasons.append(f"Good activity {txns} txns")
        elif txns >= MIN_TXNS_H1: score += 8; reasons.append(f"Activity threshold met {txns} txns")
        if ratio >= 2.0: score += 14; reasons.append(f"Strong buy pressure {ratio:.2f}x")
        elif ratio >= 1.4: score += 10; reasons.append(f"Positive buy pressure {ratio:.2f}x")
        elif ratio >= MIN_BUY_SELL_RATIO: score += 6; reasons.append(f"Buy pressure positive {ratio:.2f}x")
        if 10 <= age_minutes <= 360: score += 10; reasons.append(f"Early pair age {age_minutes:.0f}m")
        elif age_minutes <= MAX_PAIR_AGE_HOURS * 60: score += 5; reasons.append(f"Pair age {age_minutes/60:.1f}h")
        if 0 < fdv_liq <= 15: score += 5; reasons.append(f"Reasonable FDV/liquidity {fdv_liq:.1f}x")
        if websites: score += 4; reasons.append("Project website listed")
        if socials: score += 3; reasons.append("Social profile listed")
        score = min(score, 100)

        warnings = ["Market-data score only; contract and holder security are not certified"]
        if not websites: warnings.append("No website listed")
        if not socials: warnings.append("No social profile listed")
        return Evaluation(not failures and score >= MIN_SCORE, score, reasons, failures, warnings, pair)

    def in_cooldown(self, chain: str, mint: str) -> bool:
        row = self.db.execute("SELECT last_alerted_at FROM alerts_v2 WHERE chain=? AND mint=?", (chain, mint)).fetchone()
        return bool(row and int(time.time()) - int(row[0]) < ALERT_COOLDOWN_MINUTES * 60)

    def save_alert(self, chain: str, mint: str, symbol: str, evaluation: Evaluation) -> None:
        now = int(time.time())
        payload = json.dumps(evaluation.pair, separators=(",", ":"))
        self.db.execute(
            """INSERT INTO alerts_v2(chain,mint,symbol,score,first_alerted_at,last_alerted_at,payload)
               VALUES(?,?,?,?,?,?,?) ON CONFLICT(chain,mint) DO UPDATE SET
               score=excluded.score,last_alerted_at=excluded.last_alerted_at,payload=excluded.payload""",
            (chain, mint, symbol, evaluation.score, now, now, payload),
        )
        self.db.commit()

    def send_alert(self, candidate: Candidate, evaluation: Evaluation) -> None:
        pair, chain, mint = evaluation.pair, candidate.chain, candidate.mint
        base = pair.get("baseToken") or {}
        symbol = base.get("symbol") or candidate.symbol or "UNKNOWN"
        name = base.get("name") or candidate.name or symbol
        liq = float((pair.get("liquidity") or {}).get("usd") or 0)
        vol_h1 = float((pair.get("volume") or {}).get("h1") or 0)
        tx = (pair.get("txns") or {}).get("h1") or {}
        buys, sells = int(tx.get("buys") or 0), int(tx.get("sells") or 0)
        mc = float(pair.get("marketCap") or pair.get("fdv") or 0)
        change_h1 = float((pair.get("priceChange") or {}).get("h1") or 0)
        pair_url = pair.get("url") or f"https://dexscreener.com/{chain}/{pair.get('pairAddress','')}"
        links = [f"[DEX Screener]({pair_url})"]
        if chain == "solana":
            links.insert(0, f"[Pump.fun](https://pump.fun/coin/{mint})")
        reasons = "\n".join(f"• {x}" for x in evaluation.reasons[:7]) or "Thresholds met"
        warnings = "\n".join(f"• {x}" for x in evaluation.warnings[:4])
        payload = {
            "username": "Pump Genie", "allowed_mentions": {"parse": []},
            "embeds": [{
                "title": f"🧞 {symbol} — {evaluation.score}/100 quality score",
                "description": f"**{name}** passed configured market filters. Research alert only—not a buy signal.",
                "color": 0x22C55E,
                "fields": [
                    {"name": "Chain / Source", "value": f"{chain.title()} / {candidate.source}", "inline": True},
                    {"name": "Market Cap / FDV", "value": f"${mc:,.0f}", "inline": True},
                    {"name": "Liquidity", "value": f"${liq:,.0f}", "inline": True},
                    {"name": "1h Volume", "value": f"${vol_h1:,.0f}", "inline": True},
                    {"name": "1h Buys / Sells", "value": f"{buys} / {sells}", "inline": True},
                    {"name": "1h Change", "value": f"{change_h1:+.1f}%", "inline": True},
                    {"name": "Scorecard", "value": reasons, "inline": False},
                    {"name": "Risk warnings", "value": warnings, "inline": False},
                    {"name": "Research / trade pages", "value": " • ".join(links), "inline": False},
                    {"name": "Contract", "value": f"`{mint}`", "inline": False},
                ],
                "footer": {"text": "Verify authority controls, holders, insiders, and LP security before acting."},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }],
        }
        if self.dry_run_enabled:
            log.info("DRY RUN alert: %s", json.dumps(payload, indent=2))
        else:
            response = self.session.post(WEBHOOK_URL, json=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        self.save_alert(chain, mint, symbol, evaluation)
        log.info("Alerted %s (%s:%s), score=%s", symbol, chain, mint, evaluation.score)

    def cycle(self) -> None:
        for item in self.discover_dex():
            self.candidates.setdefault((item.chain, item.mint), item)
        live_count = self.drain_live_feed()
        cutoff = time.time() - CANDIDATE_TTL_MINUTES * 60
        self.candidates = {k: v for k, v in self.candidates.items() if v.discovered_at >= cutoff}
        eligible = sorted(self.candidates.values(), key=lambda c: c.last_checked)[:MAX_CANDIDATES_PER_CYCLE]
        log.info("Candidate pool=%s live_added=%s checking=%s", len(self.candidates), live_count, len(eligible))
        for candidate in eligible:
            candidate.last_checked = time.time()
            if self.in_cooldown(candidate.chain, candidate.mint):
                continue
            pair = self.best_pair(self.fetch_pairs(candidate.chain, candidate.mint), candidate.chain)
            if not pair:
                continue
            evaluation = self.evaluate(pair)
            if evaluation.passed:
                try:
                    self.send_alert(candidate, evaluation)
                except requests.RequestException as exc:
                    log.error("Discord alert failed for %s: %s", candidate.mint, exc)
            else:
                log.debug("Rejected %s score=%s failures=%s", candidate.mint, evaluation.score, evaluation.failures)
            time.sleep(0.2)

    def run(self) -> None:
        # Determine webhook and dry-run status
        if not WEBHOOK_URL:
            self.dry_run_enabled = True
            log.warning("DISCORD_WEBHOOK_URL is not set; automatically running in dry-run mode")
        else:
            self.webhook_enabled = True

        # Log enabled settings (non-secret)
        enabled_settings = []
        if ENABLE_PUMP_STREAM:
            enabled_settings.append("Pump.fun live stream")
        if self.webhook_enabled:
            enabled_settings.append("Discord webhook posting")
        if self.dry_run_enabled:
            enabled_settings.append("dry-run mode")

        log.info(
            "Pump Genie started: chains=%s poll=%ss min_score=%s enabled=[%s]",
            sorted(CHAIN_IDS),
            POLL_SECONDS,
            MIN_SCORE,
            ", ".join(enabled_settings) or "none",
        )

        if ENABLE_PUMP_STREAM:
            PumpLaunchFeed(self.inbox).start()

        while True:
            started = time.time()
            try:
                self.cycle()
            except Exception:
                log.exception("Cycle failed")
            time.sleep(max(1, POLL_SECONDS - (time.time() - started)))


if __name__ == "__main__":
    PumpGenie().run()
