# Crypto Data Spine v1 (PIT Snapshots + Manifest)

**What this is:** A tiny, reproducible **Point-in-Time (PIT)** crypto data spine.  
We fetch BTC/ETH from **CoinGecko**, save a compressed **Parquet (zstd)** snapshot, and write a **manifest.json** with:
- `rows`, ordered `columns`
- file `sha256` (content fingerprint)
- `git_sha` (which code produced it)
- `created_at_utc` (ISO, UTC)

This is the foundation for auditability, governance, and S&P-style research.

---

## ☑️ Why this matters (in one minute)
- **PIT = photo now.** We keep exactly what we saw at the cut time.
- **Manifest = receipt.** Proves **what** file was produced, **when**, and **by which code**.
- **Parquet (zstd)** = fast + small, better than CSV for tables.
- **Tests** ensure the manifest’s `sha256` matches the actual file and the schema hasn’t drifted.

---

## 📦 Tech (FREE-first)
- Python 3.11, pandas, pyarrow, requests, pandera, pytest
- Determinism: seeds fixed where applicable
- Reproducible installs via **pip-tools** (`pip-compile` + `--require-hashes`)

---

## 🧰 Prereqs
- **Windows 10/11** with **PowerShell**
- **Python 3.11** and **Git**
- (Optional) **GitHub CLI** `gh` 

Check:
```powershell
python --version
pip --version
git --version
