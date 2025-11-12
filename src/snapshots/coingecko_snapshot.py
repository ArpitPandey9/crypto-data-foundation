# src/snapshots/coingecko_snapshot.py
import os, json, hashlib, subprocess, datetime, time, random
from pathlib import Path
import numpy as np
import requests
import pandas as pd

# ---------- Determinism (best effort) ----------
SEED = 42
random.seed(SEED); np.random.seed(SEED)

# ---------- Time labels ----------
UTC_NOW = datetime.datetime.now(datetime.timezone.utc)
CUT_DATE = UTC_NOW.strftime("%Y-%m-%d")  # daily cut label (UTC)

# ---------- Paths ----------
BASE = Path(__file__).resolve().parents[2]  # project root
RAW_DIR = BASE / f"data/raw/{CUT_DATE}/coingecko"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Source (FREE endpoint) ----------
URL = "https://api.coingecko.com/api/v3/coins/markets"
params = {"vs_currency": "usd",
          "ids": "bitcoin,ethereum",
          "price_change_percentage": "24h"}

# ---------- VCR-lite: keep request ----------
(RAW_DIR / "request.json").write_text(json.dumps({"url": URL, "params": params}, indent=2))

# ---------- Call API ----------
resp = requests.get(URL, params=params, timeout=30)
resp.raise_for_status()
data = resp.json()

# ---------- VCR-lite: keep raw response ----------
(RAW_DIR / "response.json").write_text(json.dumps(data, indent=2))

# ---------- Into a stable table ----------
df = pd.DataFrame(data)
cols = ["id", "symbol", "name", "current_price", "market_cap", "total_volume", "last_updated"]
# enforce column order & stable sort so file bytes are stable-ish
df = df[cols].copy().sort_values("id").reset_index(drop=True)

# quick sanity: shape and presence
assert set(cols) == set(df.columns)
assert len(df) >= 2

# ---------- Save Parquet (zstd compression) ----------
out_path = RAW_DIR / "crypto_markets.parquet.zstd"
df.to_parquet(out_path, compression="zstd", engine="pyarrow", index=False)

# ---------- sha256 fingerprint ----------
h = hashlib.sha256()
with open(out_path, "rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
        h.update(chunk)
file_sha256 = h.hexdigest()

# ---------- Which code generated this? ----------
try:
    git_sha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=BASE).decode().strip()
except Exception:
    git_sha = None

manifest = {
    "source": "coingecko",
    "endpoint": "/api/v3/coins/markets",
    "params": params,
    "rows": int(len(df)),
    "columns": cols,
    "file": str(out_path.relative_to(BASE)),
    "sha256": file_sha256,
    "git_sha": git_sha,
    "created_at_utc": UTC_NOW.isoformat().replace("+00:00", "Z"),
}
(RAW_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2))

print("Saved:", out_path)
print("sha256:", file_sha256)
print("rows:", len(df))
