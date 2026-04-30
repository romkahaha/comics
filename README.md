# Seed monitoring

Long-running Steam `/render/` monitor for paint-seed opportunities.

The active runtime is in `monitoring/`:

- `monitoring/config.json` - paths, Steam timing, filters, Telegram behavior.
- `monitoring/run_monitoring.py` - scans one `seed_target_table.csv` row at a time, reports matching opportunities immediately, then moves to the next row.
- `monitoring_runtime/` - state, dedupe, latest matches, latest opportunities, progress CSVs.

The monitor reads `data/seed_target_table.csv`, expands each `query_name` through `lists/screening_super_full.py`, fetches exact Steam market names, applies the analysis-notebook-style filter (`edge_ratio >= 0.7` by default), and sends one Telegram message per new listing that passes.

Manual local dry run:

```powershell
python monitoring\run_monitoring.py --dry-run
```

Short test without Telegram:

```powershell
python monitoring\run_monitoring.py --duration-minutes 5 --max-batches 1 --telegram-mode off
```

GitHub Actions:

1. Add repository secrets `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
2. Optional but recommended: add `STEAM_COOKIES` if anonymous Steam requests get throttled.
3. In repository Actions settings, make sure workflow permissions allow writing contents so `monitoring_runtime/` can be committed.
4. Run **Seed monitoring** from the Actions tab. Defaults are 300 minutes, batch size 1 target row, real Telegram alerts.
