import sys
sys.path.insert(0, '.')
from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
from reconciliation.matcher import ReconciliationEngine

cfg = ReconciliationConfig()
norm = DataNormalizer(cfg)
BOOKS = r'e:\GIT\Reconciliation-2\Issue Files\ICICI 27741 - Books.xlsx'
BANK  = r'e:\GIT\Reconciliation-2\Issue Files\ICICI 27741 - Bank Statement .xlsx'

raw_a = norm.load_file(BOOKS)
raw_b = norm.load_file(BANK)
print(f"Raw A: {len(raw_a)} rows | Raw B: {len(raw_b)} rows")
print(f"B cols: {list(raw_b.columns[:6])}")

df_a = norm.normalize(raw_a, company_label='A')
df_b = norm.normalize(raw_b, company_label='B')
print(f"Norm A: {len(df_a)} rows | Norm B: {len(df_b)} rows")

if len(df_b) == 0:
    print("ERROR: B still 0 rows!")
    sys.exit(1)

print(f"B sample nets: {list(df_b['net_amount'].head(5).round(2))}")

engine = ReconciliationEngine(cfg)
results = engine.reconcile(df_a, df_b)
s = results['summary']
print(f"Matched A: {s['Matched (Company A side)']} / {s['Total Transactions Company A']}")
print(f"Matched B: {s['Matched (Company B side)']} / {s['Total Transactions Company B']}")
print(f"Exceptions: {len(results['exceptions'])}")
print(f"Balance Variance: {s['Net Balance Variance']}")
print("--- Top 15 exceptions ---")
for ex in results['exceptions'][:15]:
    print(f"  [{ex['Company']}] net={ex['Net_Amount']} | {str(ex['Transaction_Date'])[:10]} | {str(ex['Description'])[:45]}")
