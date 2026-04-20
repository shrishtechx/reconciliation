import sys; sys.path.insert(0,'.')
from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
import pandas as pd

cfg = ReconciliationConfig()
norm = DataNormalizer(cfg)
BANK = r'e:\GIT\Reconciliation-2\Issue Files\ICICI 27741 - Bank Statement .xlsx'

raw_b = norm.load_file(BANK)
print(f"ALL B cols ({len(raw_b.columns)}): {list(raw_b.columns)}")
print(f"Raw B rows: {len(raw_b)}")
print(f"\nFirst 3 rows:")
print(raw_b.head(3).to_string())
print(f"\nColumn mapping: {norm.detect_columns(raw_b)}")

df_b = norm.normalize(raw_b, company_label='B')
print(f"\nNorm B rows: {len(df_b)}")
if len(df_b) > 0:
    print(f"\nSample B net amounts (first 10): {list(df_b['net_amount'].head(10).round(2))}")
    print(f"B positive nets: {(df_b['net_amount'] > 0).sum()} rows")
    print(f"B negative nets: {(df_b['net_amount'] < 0).sum()} rows")
    print(f"B zero nets:     {(df_b['net_amount'] == 0).sum()} rows")
    print(f"B date range: {df_b['transaction_date'].min()} to {df_b['transaction_date'].max()}")
