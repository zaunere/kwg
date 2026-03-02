# kwg - Keyword Generator (web-first, no-login)

`kwg` is a Python CLI that discovers, expands, scores, and clusters keyword ideas from public web signals.

## Highlights

- No login, no API keys
- Python stdlib only
- Configurable sources: `autocomplete`, `trends`, `reddit`, `serp`
- Deterministic variants engine
- Optional clustering + report output

## Requirements

- Python 3.10+ recommended (works on many 3.9 environments as well)

## Quick start

```bash
python kwg.py --help
python kwg.py pop "wireframe browser" --top 100
python kwg.py pop "wireframe browser" --top 200 --clusters 8 --format report --out ./kwg_out
```

## Commands

- `pop`: fetch + score + output
- `expand`: fetch/generate candidates only
- `score`: score existing keywords from stdin/file

Examples:

```bash
python kwg.py pop "vps hosting" --top 100
python kwg.py expand "vps hosting" --limit 400 --format json
cat terms.txt | python kwg.py score --from stdin --top 150
```

## Source controls

```bash
--sources autocomplete,trends,reddit,serp
--geo US
--hl en
--kl us-en
--cache-dir ~/.kwg/cache
--cache-ttl 21600
--rate-limit 0.6
```

## SERP feature toggles

```bash
--serp-related / --no-serp-related
--serp-snippets / --no-serp-snippets
--serp-freshness / --no-serp-freshness
--serp-ugc / --no-serp-ugc
--serp-weakness / --no-serp-weakness
--serp-ngrams / --no-serp-ngrams
```

## Variants engine

Modes:

```bash
--variants none|basic|all|custom
--variants-packs question,commercial,compare,local,platform,opensource,howto
```

Controls:

```bash
--variants-max-per-term N
--variants-max-total N
--variants-seed-only
--variants-to-sources
--variants-only
--variants-include-original / --no-variants-include-original
```

## Output formats

```bash
--format txt|json|csv|md|report
--out ./kwg_out
```

When `--out` is set, `kwg` writes:

- `keywords.json`
- `keywords.csv`
- `clusters.json` (if clustering enabled)
- `report.md` (if report/clusters generated)

## Fault tolerance and network behavior

`kwg` uses public endpoints that may fail, rate-limit, or block requests.

Current behavior:

- Request retries with exponential backoff for transient errors
- Source-level failures are non-fatal (warnings are printed; run continues)
- A single blocked source (for example Reddit HTTP 403) does not crash the CLI

If your environment has restricted DNS/network access, expect warnings like URL errors and limited/no fetched keywords.

## Troubleshooting

If a source is unreliable in your environment:

```bash
python kwg.py pop "vps hosting" --sources autocomplete,trends,serp
python kwg.py pop "vps hosting" --sources autocomplete --rate-limit 1.0
python kwg.py pop "vps hosting" --variants all --variants-only
```

Notes:

- `--variants-only` performs no network requests
- `--no-serp-snippets` can improve SERP speed/robustness
- Public endpoints can change at any time

## Caveats

- Popularity is inferred heuristically (not official search volume)
- SERP parsing is best-effort and may change as upstream HTML changes
- Results are directional inputs for research, not absolute truth
