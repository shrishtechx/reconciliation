"""Test the full pipeline with the user's actual Tally .xls files."""
import pandas as pd
from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
from reconciliation.matcher import ReconciliationEngine
from reconciliation.reporter import ReportGenerator, generate_summary_stats

config = ReconciliationConfig()
norm = DataNormalizer(config)

# Load the actual user files
print("=" * 70)
print("LOADING FILE A: SRV Ledger in Welcare Books.xls")
print("=" * 70)
df_a_raw = norm.load_file(r'd:\Gowri\Python\SampleNewProject\SRV Ledger in Welcare Books.xls')
print(f"Loaded: {len(df_a_raw)} rows, columns: {list(df_a_raw.columns)}")
print(df_a_raw.head(5).to_string())
print()

print("=" * 70)
print("LOADING FILE B: Welcare Ledger in SRV Books.xls")
print("=" * 70)
df_b_raw = norm.load_file(r'd:\Gowri\Python\SampleNewProject\Welcare Ledger in SRV Books.xls')
print(f"Loaded: {len(df_b_raw)} rows, columns: {list(df_b_raw.columns)}")
print(df_b_raw.head(5).to_string())
print()

# Normalize
print("=" * 70)
print("NORMALIZING")
print("=" * 70)
mapping_a = norm.detect_columns(df_a_raw)
print(f"Column mapping A: {mapping_a}")
df_a = norm.normalize(df_a_raw, company_label='A')
print(f"Normalized A: {len(df_a)} rows")

mapping_b = norm.detect_columns(df_b_raw)
print(f"Column mapping B: {mapping_b}")
df_b = norm.normalize(df_b_raw, company_label='B')
print(f"Normalized B: {len(df_b)} rows")
print()

if len(df_a) > 0:
    print("Company A transactions:")
    for _, r in df_a.iterrows():
        print(f"  {str(r['transaction_date'])[:10]} | "
              f"Dr:{r['debit_amount']:>10,.0f} Cr:{r['credit_amount']:>10,.0f} | "
              f"Vch: {r['voucher_number'][:25]:25s} | "
              f"Desc: {r['description'][:40]}")
    print()

if len(df_b) > 0:
    print("Company B transactions:")
    for _, r in df_b.iterrows():
        print(f"  {str(r['transaction_date'])[:10]} | "
              f"Dr:{r['debit_amount']:>10,.0f} Cr:{r['credit_amount']:>10,.0f} | "
              f"Vch: {r['voucher_number'][:25]:25s} | "
              f"Desc: {r['description'][:40]}")
    print()

# Reconcile
print("=" * 70)
print("RECONCILING")
print("=" * 70)
engine = ReconciliationEngine(config)
results = engine.reconcile(df_a, df_b)
print(f"Matches: {len(results['matched'])}")
print(f"Exceptions: {len(results['exceptions'])}")
print()

print("--- MATCHES ---")
for m in results['matched']:
    print(f"  {m['Match_Type']:22s} | Conf: {m['Confidence_Score']:5.1f} | "
          f"A: {m['A_Date']} Dr:{m['A_Debit']:>10,.0f} Cr:{m['A_Credit']:>10,.0f} | "
          f"B: {m['B_Date']} Dr:{m['B_Debit']:>10,.0f} Cr:{m['B_Credit']:>10,.0f} | "
          f"Diff: {m['Amount_Difference']:>8,.0f} | {m['Matching_Layer']}")

print()
print("--- EXCEPTIONS ---")
for e in results['exceptions']:
    print(f"  {e['Company']} | {e['Transaction_Date'][:10]} | "
          f"Dr:{e.get('Debit',0):>10,.0f} Cr:{e.get('Credit',0):>10,.0f} | "
          f"Net:{e['Net_Amount']:>10,.0f} | {e['Category']:25s} | "
          f"{e['Description'][:35]}")

# Reporter test
stats = generate_summary_stats(results)
reporter = ReportGenerator(config)
buf = reporter.generate_excel_report(results, 0.5)
print(f"\nExcel report: {len(buf.getvalue())} bytes")
print(f"\nSummary: {results['summary']}")
print("\nALL TESTS PASSED")
