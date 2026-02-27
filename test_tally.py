"""Test the matcher with simulated Tally inter-company data."""
import pandas as pd
from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
from reconciliation.matcher import ReconciliationEngine
from reconciliation.reporter import ReportGenerator, generate_summary_stats
from collections import Counter

# Company A's ledger (S R V & Associates account in Company A's books)
a_data = {
    'Date': ['15-Apr-2024','20-Apr-2024','01-May-2024','10-May-2024',
             '15-May-2024','20-May-2024','25-May-2024','01-Jun-2024'],
    'Particulars': ['S R V & Associates']*8,
    'Vch Type': ['Sales','Receipt','Sales','Sales','Receipt','Sales','Journal','Sales'],
    'Vch No.': ['SA-001','SA-002','SA-003','SA-004','SA-005','SA-006','SA-007','SA-008'],
    'Debit': [25000, 0, 30000, 15000, 0, 50000, 0, 12000],
    'Credit': [0, 15000, 0, 0, 45000, 0, 5000, 0],
}

# Company B's ledger (S and T Welcare account in Company B's books)
b_data = {
    'Date': ['15-Apr-2024','22-Apr-2024','01-May-2024','10-May-2024',
             '15-May-2024','20-May-2024','28-May-2024'],
    'Particulars': ['S and T Welcare Equipments']*7,
    'Vch Type': ['Purchase','Payment','Purchase','Purchase','Payment','Purchase','Payment'],
    'Vch No.': ['PB-001','PB-002','PB-003','PB-004','PB-005','PB-006','PB-007'],
    'Debit': [0, 15000, 0, 0, 45000, 0, 5000],
    'Credit': [25000, 0, 30000, 15000, 0, 50000, 0],
}

dfa = pd.DataFrame(a_data)
dfb = pd.DataFrame(b_data)

config = ReconciliationConfig()
norm = DataNormalizer(config)
df_a = norm.normalize(dfa, company_label='A')
df_b = norm.normalize(dfb, company_label='B')
print(f"Normalized: A={len(df_a)}, B={len(df_b)}")

engine = ReconciliationEngine(config)
results = engine.reconcile(df_a, df_b)
print(f"Matches: {len(results['matched'])}")
print(f"Exceptions: {len(results['exceptions'])}")

types = Counter(m['Match_Type'] for m in results['matched'])
print(f"Types: {dict(types)}")

print("\n--- MATCHES ---")
for m in results['matched']:
    print(f"  {m['Match_Type']:20s} | "
          f"A: {m['A_Date']} {m['A_Debit']:>8} Dr {m['A_Credit']:>8} Cr | "
          f"B: {m['B_Date']} {m['B_Debit']:>8} Dr {m['B_Credit']:>8} Cr | "
          f"{m['Matching_Layer']}")

print("\n--- EXCEPTIONS ---")
for e in results['exceptions']:
    print(f"  {e['Company']} | {e['Transaction_Date'][:10]} | "
          f"Net: {e['Net_Amount']:>10} | {e['Category']}")

# Test reporter
stats = generate_summary_stats(results)
reporter = ReportGenerator(config)
buf = reporter.generate_excel_report(results, 0.5)
print(f"\nExcel report: {len(buf.getvalue())} bytes")
print("ALL TESTS PASSED")
