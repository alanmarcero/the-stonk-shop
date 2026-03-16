# The Stonk Shop

Serverless weekly scanner: detects 5-week EMA crossovers/crossdowns and counts days/weeks above EMA across ~10,000 US equities/ETFs.

## Verification

```bash
python3 -m pytest tests/ -v
```

## Architecture

EventBridge (Friday 2 PM ET) → Orchestrator Lambda → SQS → Worker Lambda (reserved concurrency 1) → S3 → CloudFront.

**Cost goal: under $1/month.** All AWS infrastructure decisions must prioritize minimal cost. Prefer free-tier-eligible resources, avoid provisioned capacity, and keep Lambda memory/timeout as low as practical.

## Project Structure

- `src/orchestrator/app.py`: EventBridge handler, fans out symbol batches to SQS
- `src/worker/app.py`: SQS consumer, orchestrates all analysis modules
- `src/worker/ema.py`: 5-week EMA crossover/crossdown detection
- `src/worker/quarterly.py`: Quarterly performance calculations
- `src/worker/rsi.py`: RSI(14) computation
- `src/worker/swing.py`: Swing level (breakout/breakdown) analysis
- `src/worker/vix.py`: VIX spike event detection
- `src/worker/stats.py`: Aggregate statistics computation
- `src/worker/yahoo.py`: Yahoo Finance API client
- `web/index.html`: Single-page dashboard served via CloudFront
- `terraform/`: Full AWS infrastructure (Lambda, SQS, S3, CloudFront, EventBridge, IAM)
- `scripts/prune_symbols.py`: Symbol list maintenance
- `symbols/us-equities.txt`: ~10K US equity/ETF symbol list

## Deployment

GitHub Actions on push to `main`. Tests → Terraform plan/apply → S3 dashboard deploy → CloudFront invalidation.

**AWS CLI:** Profile `scanner` configured locally for the `github-actions-scanner` IAM user.

**Required GitHub secrets:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `TF_STATE_BUCKET`, `TF_LOCK_TABLE`

## Related Repos

- **[StockTicker-macOS](https://github.com/alanmarcero/StockTicker-macOS)** — macOS menu bar app that consumes this scanner's CloudFront API via `ScannerService`.
