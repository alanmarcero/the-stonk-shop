# The Stonk Shop

![Python 3.12](https://img.shields.io/badge/Python-3.12-blue)
![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-orange)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)

Serverless weekly scanner and web dashboard for detecting 5-period EMA crossovers/crossdowns across ~1,900 US equities and ETFs.

## Architecture

EventBridge (Scheduler) → Orchestrator Lambda → SQS → Worker Lambda → S3 → CloudFront

**Cost goal: under $1/month.** Free-tier-eligible resources, no provisioned capacity.

### Key Features

- **EMA Analysis** — 5-week EMA crossovers/crossdowns and consecutive period tracking (Daily/Weekly/Monthly/Quarterly).
- **Performance Profiles** — Full-market lookback including **YTD, 1-Year, and 5-Year** performance for every ticker.
- **Market Cap Intelligence** — Integrated market capitalization data for all symbols; precise **$200B+ (Mega)** and **$200B- (Small/Mid)** filtering.
- **Index & Sector Benchmarks** — Consolidated dashboard comparing performance against major indices (SPY, QQQ, DIA) and all 11 S&P 500 sectors.
- **Breadth Stats** — Percentage of market above 200-day and 200-week SMAs.
- **VIX Spike Correlation** — Returns since major market volatility events.
- **Swing Levels** — Breakout/breakdown price levels with historical date tracking.
- **Interactive Dashboard** — Responsive web UI with real-time scan status, sorting, and multi-source filtering.
- **On-Demand Scans** — Secure "Run Now" trigger protected by developer-key authentication.

## Project Structure

```
src/
  orchestrator/    # Fan-out with SQS batching and unique run-id locking
  worker/          # Analysis engine: EMA, RSI, swing, quarterly, VIX, stats
tests/             # 243 tests (Python + Node.js UI logic)
web/               # Single-page dashboard (index.html, JS, CSS)
terraform/         # IaC: S3 OAC, CloudFront, Lambda URLs, IAM
scripts/           # $5B Market Cap pruning and maintenance utilities
symbols/           # Curated US equity/ETF list (SYMBOL,MARKET_CAP format)
```

## Development

```bash
# Run tests (executes Python suite and Node.js UI filter tests)
python3 -m pytest tests/ -v

# Manual trigger (requires profile configuration)
aws lambda invoke --function-name ema-scanner-orchestrator /dev/stdout
```

## Deployment

**GitHub Actions:** Automatically triggered on push to `main`.
1.  **Test:** Executes 243 unit tests via `pytest`.
2.  **Infrastructure:** Applies Terraform changes (IAM, Lambda, SQS, S3).
3.  **App:** Injects `ORCHESTRATOR_URL` into `index.html` and deploys to S3.
4.  **CDN:** Invalidates CloudFront cache for instant updates.

## License

MIT
