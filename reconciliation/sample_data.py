"""
Sample Data Generator.
Creates realistic test datasets demonstrating all matching scenarios.
Each scenario is designed to be captured by the correct matching layer.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import io


def generate_sample_data() -> tuple:
    """Generate two sample company ledgers with various matching scenarios.

    Scenarios:
        1. Exact Match          – same amount, date, reference
        2. Timing Difference    – same amount & ref, different date
        3. Fuzzy Text Match     – same amount & date, different descriptions, NO shared ref
        4. Rounding Difference  – amount off by ≤ ₹5, same date & ref
        5. Tax Deduction (TDS)  – B records net-of-TDS, same ref
        6. Forex Difference     – different INR amounts due to FX rate, currency ≠ INR
        7. Partial Settlement   – 1 A entry = sum of multiple B entries
        8. Aggregated (Many→1)  – multiple A entries = 1 B entry (structural)
        9. Missing Entries      – entries only in A or only in B
       10. Duplicate Entries    – same entry twice in A, once in B
    """
    random.seed(42)
    np.random.seed(42)

    base_date = datetime(2025, 3, 1)
    records_a = []
    records_b = []
    ref_counter = 1000

    desc_pool = [
        "Consulting Services", "IT Support", "Software License",
        "Hardware Purchase", "Annual Maintenance", "Training Program",
        "Marketing Campaign", "Legal Advisory", "Audit Services",
        "Cloud Hosting", "Data Migration", "Security Assessment",
        "Project Management", "Quality Assurance", "Technical Support",
    ]

    # ────────────────────────────────────────────
    # 1. EXACT MATCH (15 pairs)
    #    Same amount, same date, same reference
    # ────────────────────────────────────────────
    for i in range(15):
        ref = f"INV-{ref_counter}"; ref_counter += 1
        dt = base_date + timedelta(days=random.randint(0, 28))
        amt = round(random.uniform(5000, 200000), 2)
        desc = random.choice(desc_pool)
        records_a.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} - {ref}",
            "Debit Amount": amt, "Credit Amount": 0, "Currency": "INR",
        })
        records_b.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} - {ref}",
            "Debit Amount": 0, "Credit Amount": amt, "Currency": "INR",
        })

    # ────────────────────────────────────────────
    # 2. TIMING DIFFERENCE (10 pairs)
    #    Same amount & reference, date differs 1-5 days
    # ────────────────────────────────────────────
    for i in range(10):
        ref = f"INV-{ref_counter}"; ref_counter += 1
        dt_a = base_date + timedelta(days=random.randint(0, 25))
        dt_b = dt_a + timedelta(days=random.randint(1, 5))
        amt = round(random.uniform(10000, 150000), 2)
        desc = random.choice(desc_pool)
        records_a.append({
            "Transaction Date": dt_a.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} - {ref}",
            "Debit Amount": amt, "Credit Amount": 0, "Currency": "INR",
        })
        records_b.append({
            "Transaction Date": dt_b.strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} Ref {ref}",
            "Debit Amount": 0, "Credit Amount": amt, "Currency": "INR",
        })

    # ────────────────────────────────────────────
    # 3. FUZZY TEXT MATCH (6 pairs)
    #    Same amount & date, different descriptions,
    #    references are DIFFERENT so Layer 1 won't grab them
    # ────────────────────────────────────────────
    fuzzy_pairs = [
        ("Invoice 458 March Consulting Fees",    "Consulting Inv 458 for March"),
        ("Payment for Software License Q1 2025", "Q1 2025 Software License Payment"),
        ("Annual Maintenance Contract 2025",     "AMC 2025 Annual Maintenance"),
        ("Cloud Hosting Charges Jan to Mar",     "Jan Mar Cloud Hosting Charges"),
        ("IT Support Retainer Monthly Fee",      "Monthly IT Support Retainer Fee"),
        ("Security Audit Phase 1 Services",      "Phase 1 Security Audit Services"),
    ]
    # Use completely UNRELATED reference numbers so Layer 1 can't match on ref
    fuzzy_ref_a_pool = ["CONS-458", "SWL-9021", "AMC-3347", "CHOST-776", "ITS-5592", "SECA-2210"]
    fuzzy_ref_b_pool = ["VND-8174", "PO-6305",  "MAINT-112", "HOST-4490", "SUPP-3381", "AUD-9978"]
    for i, (desc_a, desc_b) in enumerate(fuzzy_pairs):
        ref_a = fuzzy_ref_a_pool[i]
        ref_b = fuzzy_ref_b_pool[i]
        dt = base_date + timedelta(days=random.randint(0, 28))
        amt = round(random.uniform(20000, 150000), 2)
        records_a.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref_a,
            "Description": desc_a,
            "Debit Amount": amt, "Credit Amount": 0, "Currency": "INR",
        })
        records_b.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": ref_b,
            "Description": desc_b,
            "Debit Amount": 0, "Credit Amount": amt, "Currency": "INR",
        })

    # ────────────────────────────────────────────
    # 4. ROUNDING DIFFERENCE (8 pairs)
    #    Amount differs by ≤ ₹5, same date & reference
    # ────────────────────────────────────────────
    for i in range(8):
        ref = f"INV-{ref_counter}"; ref_counter += 1
        dt = base_date + timedelta(days=random.randint(0, 28))
        amt_a = round(random.uniform(10000, 100000), 2)
        rounding = round(random.choice([-1, 1]) * random.uniform(0.50, 4.99), 2)
        amt_b = round(amt_a + rounding, 2)
        desc = random.choice(desc_pool)
        records_a.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} - {ref}",
            "Debit Amount": amt_a, "Credit Amount": 0, "Currency": "INR",
        })
        records_b.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} - {ref}",
            "Debit Amount": 0, "Credit Amount": amt_b, "Currency": "INR",
        })

    # ────────────────────────────────────────────
    # 5. TAX DEDUCTION / TDS (8 pairs)
    #    A records gross, B records net-of-TDS
    # ────────────────────────────────────────────
    tax_rates = [10.0, 5.0, 2.0, 1.0]
    for i in range(8):
        ref = f"INV-{ref_counter}"; ref_counter += 1
        dt = base_date + timedelta(days=random.randint(0, 28))
        gross = round(random.uniform(50000, 500000), 2)
        rate = random.choice(tax_rates)
        tds = round(gross * rate / 100, 2)
        net = round(gross - tds, 2)
        desc = random.choice(desc_pool)
        records_a.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} - {ref}",
            "Debit Amount": gross, "Credit Amount": 0,
            "TDS": 0, "Currency": "INR",
        })
        records_b.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": ref,
            "Description": f"{desc} - {ref} (Net of TDS @{rate:.0f}%)",
            "Debit Amount": 0, "Credit Amount": net,
            "TDS": tds, "Currency": "INR",
        })

    # ────────────────────────────────────────────
    # 6. FOREX DIFFERENCE (4 pairs)
    #    USD transactions, different exchange rates → different INR amounts
    # ────────────────────────────────────────────
    for i in range(4):
        ref = f"FX-{ref_counter}"; ref_counter += 1
        dt = base_date + timedelta(days=random.randint(0, 28))
        usd = round(random.uniform(1000, 50000), 2)
        rate_a = 83.50
        rate_b = rate_a + round(random.uniform(-1.50, 1.50), 2)
        inr_a = round(usd * rate_a, 2)
        inr_b = round(usd * rate_b, 2)
        records_a.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref,
            "Description": f"USD Payment {ref}",
            "Debit Amount": inr_a, "Credit Amount": 0,
            "Currency": "USD", "Exchange Rate": rate_a,
        })
        records_b.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": ref,
            "Description": f"USD Payment {ref}",
            "Debit Amount": 0, "Credit Amount": inr_b,
            "Currency": "USD", "Exchange Rate": rate_b,
        })

    # ────────────────────────────────────────────
    # 7. PARTIAL SETTLEMENT — 1→Many (2 sets)
    # ────────────────────────────────────────────
    # Set A: 1 invoice ₹1,73,500 → 2 payments ₹1,08,500 + ₹65,000
    ref = f"PS-{ref_counter}"; ref_counter += 1
    dt = base_date + timedelta(days=5)
    records_a.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VA-{len(records_a)+1:04d}",
        "Reference Number": ref,
        "Description": f"Invoice {ref} - Full Amount",
        "Debit Amount": 173500, "Credit Amount": 0, "Currency": "INR",
    })
    records_b.append({
        "Transaction Date": (dt + timedelta(days=2)).strftime("%d-%m-%Y"),
        "Voucher Number": f"VB-{len(records_b)+1:04d}",
        "Reference Number": f"{ref}-P1",
        "Description": f"Part Payment 1 - {ref}",
        "Debit Amount": 0, "Credit Amount": 108500, "Currency": "INR",
    })
    records_b.append({
        "Transaction Date": (dt + timedelta(days=5)).strftime("%d-%m-%Y"),
        "Voucher Number": f"VB-{len(records_b)+1:04d}",
        "Reference Number": f"{ref}-P2",
        "Description": f"Part Payment 2 - {ref}",
        "Debit Amount": 0, "Credit Amount": 65000, "Currency": "INR",
    })

    # Set B: 1 invoice ₹2,87,300 → 3 tranches ₹1,22,300 + ₹95,000 + ₹70,000
    ref = f"PS-{ref_counter}"; ref_counter += 1
    dt = base_date + timedelta(days=10)
    records_a.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VA-{len(records_a)+1:04d}",
        "Reference Number": ref,
        "Description": f"Project Billing {ref}",
        "Debit Amount": 287300, "Credit Amount": 0, "Currency": "INR",
    })
    for j, (frac, label) in enumerate([(122300, "A"), (95000, "B"), (70000, "C")]):
        records_b.append({
            "Transaction Date": (dt + timedelta(days=j*2+1)).strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": f"{ref}-{label}",
            "Description": f"Tranche {label} - {ref}",
            "Debit Amount": 0, "Credit Amount": frac, "Currency": "INR",
        })

    # ────────────────────────────────────────────
    # 8. AGGREGATED / STRUCTURAL — Many→1
    #    A books base + GST separately, B books combined
    # ────────────────────────────────────────────
    ref = f"STR-{ref_counter}"; ref_counter += 1
    dt = base_date + timedelta(days=20)
    records_a.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VA-{len(records_a)+1:04d}",
        "Reference Number": f"{ref}-BASE",
        "Description": f"Base Amount - {ref}",
        "Debit Amount": 147200, "Credit Amount": 0, "Currency": "INR",
    })
    records_a.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VA-{len(records_a)+1:04d}",
        "Reference Number": f"{ref}-GST",
        "Description": f"GST on {ref}",
        "Debit Amount": 26496, "Credit Amount": 0,
        "Currency": "INR", "GST": 26496,
    })
    records_b.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VB-{len(records_b)+1:04d}",
        "Reference Number": ref,
        "Description": f"Invoice {ref} inclusive GST",
        "Debit Amount": 0, "Credit Amount": 173696, "Currency": "INR",
    })

    # Second structural set
    ref = f"STR-{ref_counter}"; ref_counter += 1
    dt = base_date + timedelta(days=22)
    records_a.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VA-{len(records_a)+1:04d}",
        "Reference Number": f"{ref}-SVC",
        "Description": f"Service Fee - {ref}",
        "Debit Amount": 83500, "Credit Amount": 0, "Currency": "INR",
    })
    records_a.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VA-{len(records_a)+1:04d}",
        "Reference Number": f"{ref}-TAX",
        "Description": f"Service Tax - {ref}",
        "Debit Amount": 15030, "Credit Amount": 0, "Currency": "INR",
    })
    records_b.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VB-{len(records_b)+1:04d}",
        "Reference Number": ref,
        "Description": f"Service invoice {ref} with tax",
        "Debit Amount": 0, "Credit Amount": 98530, "Currency": "INR",
    })

    # ────────────────────────────────────────────
    # 9. MISSING ENTRIES (5 only in A, 5 only in B)
    # ────────────────────────────────────────────
    missing_a_refs = ["ONLYA-7701", "ONLYA-7702", "ONLYA-7703", "ONLYA-7704", "ONLYA-7705"]
    for i in range(5):
        ref = missing_a_refs[i]; ref_counter += 1
        dt = base_date + timedelta(days=random.randint(0, 28))
        amt = round(random.uniform(5000, 80000), 2)
        records_a.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref,
            "Description": f"{random.choice(desc_pool)} - {ref}",
            "Debit Amount": amt, "Credit Amount": 0, "Currency": "INR",
        })
    missing_b_refs = ["XONLY-8801", "XONLY-8802", "XONLY-8803", "XONLY-8804", "XONLY-8805"]
    for i in range(5):
        ref = missing_b_refs[i]; ref_counter += 1
        dt = base_date + timedelta(days=random.randint(0, 28))
        amt = round(random.uniform(5000, 80000), 2)
        records_b.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VB-{len(records_b)+1:04d}",
            "Reference Number": ref,
            "Description": f"{random.choice(desc_pool)} - {ref}",
            "Debit Amount": 0, "Credit Amount": amt, "Currency": "INR",
        })

    # ────────────────────────────────────────────
    # 10. DUPLICATE ENTRIES (2 identical in A, 1 in B)
    # ────────────────────────────────────────────
    ref = f"DUP-{ref_counter}"; ref_counter += 1
    dt = base_date + timedelta(days=12)
    for _ in range(2):
        records_a.append({
            "Transaction Date": dt.strftime("%d-%m-%Y"),
            "Voucher Number": f"VA-{len(records_a)+1:04d}",
            "Reference Number": ref,
            "Description": f"Duplicate Test - {ref}",
            "Debit Amount": 45000, "Credit Amount": 0, "Currency": "INR",
        })
    records_b.append({
        "Transaction Date": dt.strftime("%d-%m-%Y"),
        "Voucher Number": f"VB-{len(records_b)+1:04d}",
        "Reference Number": ref,
        "Description": f"Payment for {ref}",
        "Debit Amount": 0, "Credit Amount": 45000, "Currency": "INR",
    })

    # ── Build DataFrames & fill optional columns ──
    df_a = pd.DataFrame(records_a)
    df_b = pd.DataFrame(records_b)

    for col in ['TDS', 'GST', 'Exchange Rate']:
        if col not in df_a.columns:
            df_a[col] = 0
        if col not in df_b.columns:
            df_b[col] = 0
        df_a[col] = df_a[col].fillna(0)
        df_b[col] = df_b[col].fillna(0)
    for df in [df_a, df_b]:
        if 'Currency' not in df.columns:
            df['Currency'] = 'INR'

    return df_a, df_b


def save_sample_to_excel() -> tuple:
    """Save sample data to in-memory Excel buffers and return them."""
    df_a, df_b = generate_sample_data()
    buf_a = io.BytesIO()
    buf_b = io.BytesIO()
    df_a.to_excel(buf_a, index=False, engine='openpyxl')
    df_b.to_excel(buf_b, index=False, engine='openpyxl')
    buf_a.seek(0)
    buf_b.seek(0)
    return buf_a, buf_b, df_a, df_b
