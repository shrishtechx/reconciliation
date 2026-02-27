"""Quick test to verify reconciliation engine behavior."""
import sys
import traceback

try:
    from reconciliation.config import ReconciliationConfig
    from reconciliation.normalizer import DataNormalizer
    from reconciliation.matcher import ReconciliationEngine
    from reconciliation.sample_data import generate_sample_data

    config = ReconciliationConfig()
    normalizer = DataNormalizer(config)

    df_a_raw, df_b_raw = generate_sample_data()
    print(f"Raw data - A: {len(df_a_raw)} rows, B: {len(df_b_raw)} rows")

    df_a = normalizer.normalize(df_a_raw, company_label="A")
    df_b = normalizer.normalize(df_b_raw, company_label="B")
    print(f"Normalized - A: {len(df_a)} rows, B: {len(df_b)} rows")
    print(f"A columns: {list(df_a.columns)}")
    print(f"A net_amount range: {df_a['net_amount'].min():.2f} to {df_a['net_amount'].max():.2f}")
    print(f"B net_amount range: {df_b['net_amount'].min():.2f} to {df_b['net_amount'].max():.2f}")
    print()

    engine = ReconciliationEngine(config)
    results = engine.reconcile(df_a, df_b)

    print(f"=== RESULTS ===")
    print(f"Total matches: {len(results['matched'])}")
    print(f"Total exceptions: {len(results['exceptions'])}")
    print()

    # Count by match type
    from collections import Counter
    match_types = Counter(m['Match_Type'] for m in results['matched'])
    print("Match type breakdown:")
    for mt, count in sorted(match_types.items()):
        print(f"  {mt}: {count}")
    print()

    # Count by layer
    layers = Counter(m['Matching_Layer'] for m in results['matched'])
    print("Layer breakdown:")
    for layer, count in sorted(layers.items()):
        print(f"  {layer}: {count}")
    print()

    # Show all matches
    print("All matches:")
    for i, m in enumerate(results['matched']):
        print(f"  {i+1}. {m['Transaction_ID_A']} <-> {m['Transaction_ID_B']} | "
              f"{m['Match_Type']} | Conf: {m['Confidence_Score']} | "
              f"AmtDiff: {m['Amount_Difference']} | Layer: {m['Matching_Layer']}")
    print()

    # Show exceptions
    exc_cats = Counter(e['Category'] for e in results['exceptions'])
    print(f"Exception breakdown:")
    for cat, count in sorted(exc_cats.items()):
        print(f"  {cat}: {count}")
    print()

    # Show summary
    print("Summary:")
    for k, v in results['summary'].items():
        print(f"  {k}: {v}")

    # Test reporter
    print("\n--- Testing Reporter ---")
    from reconciliation.reporter import ReportGenerator, generate_summary_stats
    stats = generate_summary_stats(results)
    print(f"Stats keys: {list(stats.keys())}")
    print(f"Match types: {stats['match_types']}")

    reporter = ReportGenerator(config)
    buf = reporter.generate_excel_report(results, 1.23)
    print(f"Excel report size: {len(buf.getvalue())} bytes")
    print("Reporter: OK")

except Exception as e:
    print(f"ERROR: {e}")
    traceback.print_exc()
