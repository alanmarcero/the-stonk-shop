# The Stonk Shop

![Python 3.12](https://img.shields.io/badge/Python-3.12-blue)
![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-orange)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)

Serverless weekly scanner and web dashboard for detecting 5-week EMA crossovers/crossdowns across ~10,000 US equities and ETFs.

## Architecture

EventBridge (Friday 2 PM ET) → Orchestrator Lambda → SQS → Worker Lambda → S3 → CloudFront

**Cost goal: under $1/month.** Free-tier-eligible resources, no provisioned capacity.

### What it scans

- **EMA crossovers/crossdowns** — 5-week EMA signal detection
- **Days/weeks above EMA** — Consecutive closes above/below 5-period EMA
- **Quarterly performance** — Since-quarter and during-quarter percent changes
- **RSI(14)** — Relative strength index
- **Swing levels** — Breakout/breakdown price levels with dates
- **VIX spike analysis** — Market volatility event detection

## Project Structure

```
src/
  orchestrator/    # EventBridge → SQS fan-out
  worker/          # SQS consumer: EMA, RSI, swing, quarterly, VIX, stats
tests/             # 307 tests
web/               # Single-page dashboard (index.html)
terraform/         # Full AWS infrastructure
scripts/           # Symbol list maintenance
symbols/           # ~10K US equity/ETF symbol list
```

## Development

```bash
# Run tests
python3 -m pytest tests/ -v

# Manual invoke
aws lambda invoke --function-name ema-scanner-orchestrator /dev/stdout
```

**AWS CLI:** Profile `scanner` configured for the `github-actions-scanner` IAM user.

## Deployment

GitHub Actions on push to `main`. Runs tests, then applies Terraform and deploys the web dashboard to S3/CloudFront.

Manual deployment requires these GitHub secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `TF_STATE_BUCKET`, `TF_LOCK_TABLE`.

## Related Repos

- **[StockTicker-macOS](https://github.com/alanmarcero/StockTicker-macOS)** — macOS menu bar app that consumes this scanner's CloudFront API via `ScannerService`.

## License

MIT
