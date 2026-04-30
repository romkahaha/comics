# `steam_seed_scan`

Collect Steam `/render/` listings for items matched by query terms and keep only rows with selected `paint_seed`.

Files:

- `seed_listing_scan.py`
- `seed_listing_scan_runtime.json`
- `seed_listing_scan_runner.ipynb`

CLI examples:

```powershell
python steam_seed_scan\seed_listing_scan.py --query "AK, redline" --query-mode AND --seeds 661,321 --out steam_seed_scan\data\ak_redline_seeds.csv
python steam_seed_scan\seed_listing_scan.py --query "AK, redline" --query-mode OR --seeds 661,321 --out steam_seed_scan\data\ak_or_redline_seeds.csv
```
