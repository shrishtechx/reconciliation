"""Quick timing test — runs normalization + reconciliation and reports elapsed time."""
import sys, time
sys.path.insert(0, '.')
from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
from reconciliation.matcher import ReconciliationEngine

BOOKS = r'e:\GIT\Reconciliation-2\Issue Files\ICICI 27741 - Books.xlsx'
BANK  = r'e:\GIT\Reconciliation-2\Issue Files\ICICI 27741 - Bank Statement .xlsx'

cfg = ReconciliationConfig()
norm = DataNormalizer(cfg)

t0 = time.time()
raw_a = norm.load_file(BOOKS)
raw_b = norm.load_file(BANK)
t1 = time.time()
print(f"Load:        {t1-t0:.2f}s  | A={len(raw_a)} rows  B={len(raw_b)} rows")

df_a = norm.normalize(raw_a, company_label='A')
df_b = norm.normalize(raw_b, company_label='B')
t2 = time.time()
print(f"Normalize:   {t2-t1:.2f}s  | A={len(df_a)} rows  B={len(df_b)} rows")

if len(df_b) == 0:
    print("ERROR: B has 0 rows after normalization!")
    sys.exit(1)

engine = ReconciliationEngine(cfg)
results = engine.reconcile(df_a, df_b)
t3 = time.time()
print(f"Reconcile:   {t3-t2:.2f}s")
print(f"TOTAL:       {t3-t0:.2f}s")

s = results['summary']
print(f"\nMatched A:   {s['Matched (Company A side)']} / {s['Total Transactions Company A']}")
print(f"Matched B:   {s['Matched (Company B side)']} / {s['Total Transactions Company B']}")
print(f"Exceptions:  {len(results['exceptions'])}")
print(f"Variance:    {s['Net Balance Variance']}")
print("\nTop 5 exceptions:")
for ex in results['exceptions'][:5]:
    print(f"  [{ex['Company']}] {str(ex['Transaction_Date'])[:10]}  net={ex['Net_Amount']}  {str(ex['Description'])[:40]}")
