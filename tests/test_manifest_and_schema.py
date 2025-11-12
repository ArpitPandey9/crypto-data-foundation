# tests/test_manifest_and_schema.py
import json, hashlib
from pathlib import Path
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema

def test_manifest_and_schema():
    BASE = Path(__file__).resolve().parents[1]
    raw_root = BASE / "data" / "raw"
    dates = [p for p in raw_root.iterdir() if p.is_dir()]
    assert dates, "No snapshot folders found under data/raw/"
    latest = sorted(dates)[-1]
    cg = latest / "coingecko"
    assert cg.exists(), f"Missing folder: {cg}"

    manifest_path = cg / "manifest.json"
    assert manifest_path.exists(), f"Missing manifest: {manifest_path}"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    parquet_path = BASE / manifest["file"]
    assert parquet_path.exists(), f"Missing parquet: {parquet_path}"

    h = hashlib.sha256()
    with open(parquet_path, "rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""):
            h.update(chunk)
    assert h.hexdigest() == manifest["sha256"], "sha256 mismatch vs manifest"

    df = pd.read_parquet(parquet_path)
    schema = DataFrameSchema({
        "id": Column(str),
        "symbol": Column(str),
        "name": Column(str),
        "current_price": Column(float),
        "market_cap": Column(float, nullable=True),
        "total_volume": Column(float, nullable=True),
        "last_updated": Column(str),
    }, coerce=True)
    schema.validate(df)

    assert df.shape[0] >= 2, "Expected >=2 rows (BTC, ETH)"
    assert list(df.columns) == manifest["columns"], "Column order/content mismatch"
