# Pump Genie 2.0 — 24/7 Railway Scanner

Pump Genie combines a live Pump.fun launch feed with DEX Screener discovery. It keeps candidates in a rolling pool, waits for usable market data, scores quality, rejects weak setups, and sends Discord alerts with direct research/trade-page links.

## Included

- Always-on Railway worker configuration.
- Live Pump.fun new-token WebSocket with automatic reconnect and exponential backoff.
- DEX Screener token-profile and boosted-token feeds for every chain in `CHAIN_IDS`.
- Pair selection by strongest USD liquidity.
- Hard-fail filters plus a 100-point quality scorecard.
- Discord embeds with Pump.fun and DEX Screener links, metrics, score reasons, risk warnings, and the contract address.
- SQLite deduplication and a configurable repeat-alert cooldown.
- Dry-run mode, retry handling, rate-limit handling, logs, tests, Dockerfile, and secret-safe configuration.

## Deploy now

1. Create a **private** GitHub repository named `pump-genie`.
2. Upload all files from this folder. Keep the folder contents at the repository root.
3. In Discord, open the alert channel and select **Edit Channel → Integrations → Webhooks → New Webhook → Copy Webhook URL**.
4. In Railway, select **New Project → Deploy from GitHub Repo**, then choose the private repository.
5. Open the Railway service's **Variables** tab and add `DISCORD_WEBHOOK_URL` with the copied URL.
6. Add `DRY_RUN=true` for the first deployment.
7. Deploy and check logs for `Pump Genie started`, `Pump.fun launch feed connected`, and `Candidate pool=`.
8. When the logs are healthy, change `DRY_RUN=false` and redeploy.
9. In Railway service settings, confirm **Restart Policy: Always**. Railway limits this policy on free/trial plans.

## Configure feeds

The default scans Solana and consumes live Pump.fun launches:

```env
CHAIN_IDS=solana
ENABLE_PUMP_STREAM=true
PUMP_WS_URL=wss://pumpdev.io/ws
```

DEX Screener discovery can cover additional supported chains by adding chain IDs:

```env
CHAIN_IDS=solana,base,ethereum,bsc
```

Pump.fun itself is Solana-based; “multi-chain” in this package means Pump.fun/Solana live launches plus DEX Screener discovery and analysis for other selected chains.

## Default quality gates

| Gate | Default |
|---|---:|
| Minimum liquidity | $15,000 |
| Minimum one-hour volume | $10,000 |
| Minimum one-hour transactions | 80 |
| Minimum buy/sell ratio | 1.10 |
| Pair age | 3 minutes to 24 hours |
| Maximum FDV/liquidity | 40x |
| Maximum absolute one-hour move | 400% |
| Minimum quality score | 65/100 |
| Repeat-alert cooldown | 180 minutes |
| Candidate retention | 180 minutes |

The score rewards liquidity, volume, transaction activity, buy pressure, early-but-not-instant pair age, reasonable FDV/liquidity, and available website/social data. A token must pass every hard gate and meet the minimum score.

## Tune without coding

Add or change variables in Railway:

```env
MIN_LIQUIDITY_USD=25000
MIN_VOLUME_H1_USD=20000
MIN_TXNS_H1=120
MIN_BUY_SELL_RATIO=1.20
MIN_SCORE=72
MAX_PAIR_AGE_HOURS=12
ALERT_COOLDOWN_MINUTES=360
```

Higher values reduce alerts and generally make the feed more selective. Begin with the defaults, observe results for at least a day, then change one or two settings at a time.

## Local validation

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Set DRY_RUN=true inside .env
python scanner.py
```

Tests:

```bash
pip install pytest
pytest -q
```

## Security boundary

This scanner never executes trades and does not need a wallet, private key, or seed phrase. Links open external trading/research pages; they are not automatic purchase buttons.

The score is based on available market activity and profile information. It does **not** prove that mint/freeze authority is revoked, liquidity is locked or burned, holders are distributed safely, insiders are absent, metadata is immutable, or a token cannot rug. Verify contract security separately before taking action.

## Secret rules

- Never paste a Discord webhook into chat, source code, screenshots, or a public repository.
- Store it only in Railway Variables or a local `.env` file ignored by Git.
- If exposed, delete the webhook in Discord and create a new one.
- Never add a wallet secret to this project.
