import csv
from pathlib import Path

RAW_FILE = Path("data/raw/dispatch_sample")
OUTPUT_FILE = Path("data/clean/dispatch_price/dispatch_price_clean.csv")

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

rows = []

for file in RAW_FILE.glob("*.csv"):
    with open(file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = []

        for row in reader:
            if row[0] == "I" and row[1] == "DREGION":
                headers = row[4:]
            elif row[0] == "D" and row[1] == "DREGION":
                data = dict(zip(headers, row[4:]))
                rows.append({
                    "settlement_date": data["SETTLEMENTDATE"],
                    "region_id": data["REGIONID"],
                    "rrp": data["RRP"],
                    "intervention": data["INTERVENTION"],
                    "run_no": data["RUNNO"],
                })

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["settlement_date", "region_id", "rrp", "intervention", "run_no"]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Processed {len(rows)} rows into {OUTPUT_FILE}")
