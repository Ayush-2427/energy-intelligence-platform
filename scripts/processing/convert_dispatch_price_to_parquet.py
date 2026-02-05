import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/clean/dispatch_price/dispatch_price_clean.csv")
OUTPUT_FILE = Path("data/curated/dispatch_price/dispatch_price.parquet")

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(INPUT_FILE)

df["settlement_date"] = pd.to_datetime(df["settlement_date"])

df.to_parquet(OUTPUT_FILE, engine="pyarrow", index=False)

print(f"Wrote parquet file to {OUTPUT_FILE}")
print(f"Rows: {len(df)}")
