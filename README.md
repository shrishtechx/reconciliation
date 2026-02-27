# Inter-Company Ledger Reconciliation System

Enterprise-grade transaction matching engine for reconciling inter-company ledgers with multi-layer analysis and audit-ready reporting.

## Features

- **10 Matching Scenarios**: Exact, Timing, Tax Deduction, Rounding, Missing, Fuzzy Text, Partial Settlement, Many-to-One, Forex, Structural, and Duplicate detection
- **5-Layer Matching Engine**: Strict → Relaxed → Tolerance → Pattern → Partial Settlement
- **Weighted Scoring Model**: Configurable weights for amount, date, reference, and narration similarity
- **Professional Dashboard**: Interactive charts, KPI cards, filterable data views
- **Audit-Ready Reports**: Excel output with Summary, Matched, Exceptions, and Audit Log sheets
- **High Performance**: Amount/date bucketing and hash indexing for O(n) lookups
- **Fully Configurable**: All tolerances, thresholds, and weights adjustable via sidebar

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Application
```bash
streamlit run app.py
```

### 3. Usage
1. Upload Company A and Company B ledger files (Excel/CSV), or click **Load Sample Data**
2. Adjust configuration settings in the sidebar (optional)
3. Click **Run Reconciliation**
4. Explore results across the dashboard tabs
5. Download the audit-ready Excel report

## Project Structure

```
├── app.py                        # Streamlit web application
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── reconciliation/
    ├── __init__.py
    ├── config.py                 # Configuration & tolerances
    ├── normalizer.py             # Data ingestion & normalization
    ├── matcher.py                # Multi-layer matching engine
    ├── reporter.py               # Excel report generation
    └── sample_data.py            # Sample data generator
```

## Input File Format

### Mandatory Fields
- Transaction Date
- Voucher Number / Document ID
- Reference Number
- Description
- Debit Amount
- Credit Amount

### Optional Fields
- TDS (Tax Deducted at Source)
- GST
- Currency
- Exchange Rate
- Document Type

## Matching Layers

| Layer | Description | Confidence |
|-------|------------|------------|
| Layer 1 - Strict | Exact amount + date + reference | 95-100% |
| Layer 2 - Relaxed | Amount match + date tolerance + narration similarity | 85-95% |
| Layer 3 - Tolerance | Weighted scoring across all factors | 75-90% |
| Layer 4 - Pattern | Tax, rounding, forex pattern detection | 78-95% |
| Layer 5 - Partial | One-to-many / many-to-one settlement | 80% |

## Tech Stack

- **Python 3.9+**
- **Streamlit** - Web UI framework
- **Pandas** - Data processing
- **RapidFuzz** - Fuzzy text matching
- **Plotly** - Interactive charts
- **XlsxWriter** - Excel report generation
- **NumPy** - Numerical operations
