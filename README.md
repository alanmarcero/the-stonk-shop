# The Stonk Shop

![Python 3.12](https://img.shields.io/badge/Python-3.12-blue)
![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-orange)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)

Serverless weekly scanner and web dashboard for detecting 5-period EMA crossovers/crossdowns across ~4,300 US equities and ETFs.

## Architecture

EventBridge (Scheduler) → Orchestrator Lambda → SQS → Worker Lambda → S3 → CloudFront

**Cost goal: under $1/month.** Free-tier-eligible resources, no provisioned capacity.

### Key Features

- **EMA Analysis** — 5-week EMA crossovers/crossdowns and consecutive period tracking (Daily/Weekly/Monthly/Quarterly).
- **Performance Metrics** — YTD performance, distance from 3-year highs and 52-week lows.
- **Breadth Stats** — Percentage of market above 200-day and 200-week SMAs.
- **VIX Spike Correlation** — Returns since major market volatility events.
- **Swing Levels** — Breakout/breakdown price levels with historical date tracking.
- **Interactive Dashboard** — Responsive web UI with real-time scan status, sorting, and multi-source filtering.
- **On-Demand Scans** — Secure "Run Now" trigger protected by a `DEV_KEY` and origin-restricted CORS.

## Project Structure

```
src/
  orchestrator/    # Fan-out with SQS batching
  worker/          # Analysis engine: EMA, RSI, swing, quarterly, VIX, stats
tests/             # 340 tests (100% pass)
web/               # Single-page dashboard (index.html)
terraform/         # IaC: S3 OAC, CloudFront, Lambda URLs, IAM
scripts/           # Diagnostic and maintenance utilities
symbols/           # Curated US equity/ETF ticker lists
```

## Development

```bash
# Run tests
python3 -m pytest tests/ -v

# Manual trigger (requires profile configuration)
aws lambda invoke --function-name ema-scanner-orchestrator /dev/stdout
```

## Deployment

**GitHub Actions:** Automatically triggered on push to `main`.
1.  **Test:** Executes 340 unit tests via `pytest`.
2.  **Infrastructure:** Applies Terraform changes (IAM, Lambda, SQS, S3).
3.  **App:** Injects `ORCHESTRATOR_URL` and `DEV_KEY` into `index.html` and deploys to S3.
4.  **CDN:** Invalidates CloudFront cache for instant updates.

## License

MIT
