"""Debug test: simulate the exact exception transactions and verify Layer 7 matches them."""
import pandas as pd
import sys
sys.path.insert(0, '.')

from reconciliation.config import ReconciliationConfig
from reconciliation.matcher import ReconciliationEngine

# Exact transactions from the exception screenshot
df_a = pd.DataFrame([
    {"row_id": "A_000001", "company": "A", "transaction_date": pd.Timestamp("2025-10-07"),
     "debit_amount": 0.0, "credit_amount": 9018.0, "net_amount": -9018.0, "abs_amount": 9018.0,
     "description": "By TDS -PURCHASE 206AB", "voucher_number": "8",
     "reference_number": "", "description_normalized": "tds purchase 206ab", "reference_normalized": ""},
    {"row_id": "A_000002", "company": "A", "transaction_date": pd.Timestamp("2026-02-07"),
     "debit_amount": 0.0, "credit_amount": 38291.0, "net_amount": -38291.0, "abs_amount": 38291.0,
     "description": "By TDS CONTRACT - 194C", "voucher_number": "30",
     "reference_number": "", "description_normalized": "tds contract 194c", "reference_normalized": ""},
    {"row_id": "A_000003", "company": "A", "transaction_date": pd.Timestamp("2026-03-21"),
     "debit_amount": 0.0, "credit_amount": 375000.0, "net_amount": -375000.0, "abs_amount": 375000.0,
     "description": "By KARTHIKEYAN.S (RENT)", "voucher_number": "58",
     "reference_number": "", "description_normalized": "karthikeyan rent", "reference_normalized": ""},
])

df_b = pd.DataFrame([
    {"row_id": "B_000001", "company": "B", "transaction_date": pd.Timestamp("2025-09-06"),
     "debit_amount": 9018.0, "credit_amount": 0.0, "net_amount": 9018.0, "abs_amount": 9018.0,
     "description": "To Icici Bank Current Account", "voucher_number": "36.0",
     "reference_number": "", "description_normalized": "icici bank current account", "reference_normalized": ""},
    {"row_id": "B_000002", "company": "B", "transaction_date": pd.Timestamp("2025-12-13"),
     "debit_amount": 100000.0, "credit_amount": 0.0, "net_amount": 100000.0, "abs_amount": 100000.0,
     "description": "To Icici Bank Current Account", "voucher_number": "67.0",
     "reference_number": "", "description_normalized": "icici bank current account", "reference_normalized": ""},
    {"row_id": "B_000003", "company": "B", "transaction_date": pd.Timestamp("2026-02-27"),
     "debit_amount": 38291.0, "credit_amount": 0.0, "net_amount": 38291.0, "abs_amount": 38291.0,
     "description": "To Icici Bank Current Account", "voucher_number": "92.0",
     "reference_number": "", "description_normalized": "icici bank current account", "reference_normalized": ""},
    {"row_id": "B_000004", "company": "B", "transaction_date": pd.Timestamp("2026-03-06"),
     "debit_amount": 375000.0, "credit_amount": 0.0, "net_amount": 375000.0, "abs_amount": 375000.0,
     "description": "To Icici Bank Current Account", "voucher_number": "219.0",
     "reference_number": "", "description_normalized": "icici bank current account", "reference_normalized": ""},
    {"row_id": "B_000005", "company": "B", "transaction_date": pd.Timestamp("2026-03-31"),
     "debit_amount": 15000.0, "credit_amount": 0.0, "net_amount": 15000.0, "abs_amount": 15000.0,
     "description": "To Icici Bank Current Account", "voucher_number": "108.0",
     "reference_number": "", "description_normalized": "icici bank current account", "reference_normalized": ""},
])

cfg = ReconciliationConfig()
print(f"Config: date_tolerance={cfg.date_tolerance_days} days")

engine = ReconciliationEngine(cfg)
results = engine.reconcile(df_a, df_b)

print(f"\n=== MATCHES ({len(results['matched'])}) ===")
for m in results['matched']:
    print(f"  [{m['Matching_Layer']}] {m['Transaction_ID_A']} <-> {m['Transaction_ID_B']} | {m['Match_Type']} | {m['Details']}")

print(f"\n=== EXCEPTIONS ({len(results['exceptions'])}) ===")
for e in results['exceptions']:
    print(f"  [{e['Company']}] {e['Transaction_Date'][:10]} | {e['Description']} | Net={e['Net_Amount']}")

print(f"\n=== SUMMARY ===")
s = results['summary']
print(f"  Matched A: {s['Matched (Company A side)']} / {s['Total Transactions Company A']}")
print(f"  Matched B: {s['Matched (Company B side)']} / {s['Total Transactions Company B']}")
print(f"  Balance Variance: {s['Net Balance Variance']}")
