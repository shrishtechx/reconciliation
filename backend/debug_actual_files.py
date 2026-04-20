"""Debug with actual Excel files to diagnose matching failure."""
import sys
sys.path.insert(0, '.')

import pandas as pd
from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
from reconciliation.matcher import ReconciliationEngine

BOOKS_FILE = r"e:\GIT\Reconciliation-2\Issue Files\ICICI 27741 - Books.xlsx"
BANK_FILE  = r"e:\GIT\Reconciliation-2\Issue Files\ICICI 27741 - Bank Statement .xlsx"

cfg = ReconciliationConfig()
print(f"date_tolerance={cfg.date_tolerance_days} days")

norm = DataNormalizer(cfg)

# --- Sheet selection ---
print("\n=== SHEET SELECTION ===")
best_a = norm._select_best_excel_sheet(BOOKS_FILE)
best_b = norm._select_best_excel_sheet(BANK_FILE)
print(f"Books: selected sheet = '{best_a}'")
print(f"Bank:  selected sheet = '{best_b}'")

# --- Show all available sheets ---
try:
    xf_a = pd.ExcelFile(BOOKS_FILE, engine='openpyxl')
    xf_b = pd.ExcelFile(BANK_FILE, engine='openpyxl')
    print(f"Books sheets: {xf_a.sheet_names}")
    print(f"Bank sheets:  {xf_b.sheet_names}")
    for sname in xf_b.sheet_names:
        df_s = xf_b.parse(sname, nrows=5, header=None)
        print(f"  Bank sheet '{sname}': {df_s.shape} | cols: {list(df_s.columns)}")
except Exception as e:
    print(f"Sheet info error: {e}")

raw_a = norm.load_file(BOOKS_FILE)
raw_b = norm.load_file(BANK_FILE)
print(f"\nRaw A: {len(raw_a)} rows x {len(raw_a.columns)} cols")
print(f"Raw A cols: {list(raw_a.columns)}")
print(f"\nRaw B: {len(raw_b)} rows x {len(raw_b.columns)} cols")
print(f"Raw B cols: {list(raw_b.columns)}")
print(f"Raw B sample (first 3 rows):")
print(raw_b.head(3).to_string())

print(f"\nColumn mapping A: {norm.detect_columns(raw_a)}")
print(f"Column mapping B: {norm.detect_columns(raw_b)}")

df_a = norm.normalize(raw_a, company_label="A")
df_b = norm.normalize(raw_b, company_label="B")
print(f"\nNormalized A: {len(df_a)} rows | Normalized B: {len(df_b)} rows")
if len(df_b) > 0:
    print(f"B sample net amounts: {list(df_b['net_amount'].head(5))}")
    print(f"B date range: {df_b['transaction_date'].min()} to {df_b['transaction_date'].max()}")
else:
    print("WARNING: Company B has 0 rows after normalization!")

engine = ReconciliationEngine(cfg)
results = engine.reconcile(df_a, df_b)

exceptions = results["exceptions"]
print(f"\n=== EXCEPTIONS ({len(exceptions)}) ===")
for e in exceptions:
    print(f"  [{e['Company']}] {e['Transaction_Date'][:10]} | {e['Description'][:40]} | "
          f"Debit={e['Debit']} Credit={e['Credit']} Net={e['Net_Amount']}")

print(f"\n=== CHECKING NET AMOUNTS OF EXCEPTION TRANSACTIONS ===")
exc_ids_a = {e["Row_ID"] for e in exceptions if e["Company"] == "A"}
exc_ids_b = {e["Row_ID"] for e in exceptions if e["Company"] == "B"}

df_a_exc = df_a[df_a["row_id"].isin(exc_ids_a)]
df_b_exc = df_b[df_b["row_id"].isin(exc_ids_b)]

print("\nCompany A exception net_amounts:")
for _, r in df_a_exc.iterrows():
    print(f"  {r['row_id']} | {str(r['transaction_date'])[:10]} | "
          f"net={r['net_amount']:.4f} | debit={r['debit_amount']} credit={r['credit_amount']}")

print("\nCompany B exception net_amounts:")
for _, r in df_b_exc.iterrows():
    print(f"  {r['row_id']} | {str(r['transaction_date'])[:10]} | "
          f"net={r['net_amount']:.4f} | debit={r['debit_amount']} credit={r['credit_amount']}")

print("\n=== CHECKING IF OPPOSING NETS EXIST IN EACH INDEX ===")
from collections import defaultdict

idx_b_net = defaultdict(list)
for idx, row in df_b.iterrows():
    net = round(row['net_amount'], 2)
    idx_b_net[net].append((idx, row['row_id']))

for _, ra in df_a_exc.iterrows():
    target = round(-ra['net_amount'], 2)
    found = idx_b_net.get(target, [])
    available = [(ib, rid) for ib, rid in found if rid in exc_ids_b or rid not in {e["Row_ID"] for e in exceptions if e["Company"] == "B"}]
    print(f"  A net={ra['net_amount']:.2f} -> looking for B net={target:.2f} -> found {len(found)} in index, exc_b matches: {[rid for _, rid in found if rid in exc_ids_b]}")

print(f"\nSummary: Matched A={results['summary']['Matched (Company A side)']} | "
      f"Matched B={results['summary']['Matched (Company B side)']} | "
      f"Balance Variance={results['summary']['Net Balance Variance']}")
