"""
Data Normalization Module.
Handles ingestion, cleaning, and standardization of financial datasets.
Specifically designed for Tally inter-company ledger exports (.xls/.xlsx).
"""

import pandas as pd
import numpy as np
import re
from typing import Tuple, Optional
from .config import ReconciliationConfig


class DataNormalizer:
    """Normalizes and prepares financial ledger data for reconciliation."""

    def __init__(self, config: ReconciliationConfig):
        self.config = config

    # Keywords that indicate a row is the actual column header
    _HEADER_KEYWORDS = [
        'date', 'voucher', 'vch', 'debit', 'credit', 'particular',
        'narration', 'description', 'amount', 'reference', 'ref',
        'invoice', 'dr', 'cr', 'balance', 'type',
    ]

    # Rows whose description matches these are non-transaction rows
    _SKIP_DESCRIPTIONS = {
        'opening balance', 'closing balance', 'total', 'grand total',
    }

    def _read_excel_any(self, source, **kwargs):
        """Read Excel file, trying openpyxl first then xlrd as fallback.
        Tally often saves .xlsx content with a .xls extension."""
        # Try openpyxl first (handles both .xlsx and mislabeled .xls-as-xlsx)
        if hasattr(source, 'seek'):
            source.seek(0)
        try:
            return pd.read_excel(source, engine='openpyxl', **kwargs)
        except Exception:
            pass
        # Fallback to xlrd for genuine old .xls files
        if hasattr(source, 'seek'):
            source.seek(0)
        try:
            return pd.read_excel(source, engine='xlrd', **kwargs)
        except Exception:
            pass
        # Last resort: let pandas auto-detect
        if hasattr(source, 'seek'):
            source.seek(0)
        return pd.read_excel(source, **kwargs)

    def load_file(self, file_path_or_buffer, file_type: str = "auto") -> pd.DataFrame:
        """Load data from Excel or CSV file.
        Auto-detects the header row for Tally-style exports that have
        company name / address metadata in the first few rows."""

        is_excel = False
        if isinstance(file_path_or_buffer, str):
            is_excel = file_path_or_buffer.endswith(('.xlsx', '.xls'))
        else:
            name = getattr(file_path_or_buffer, 'name', '')
            is_excel = name.endswith(('.xlsx', '.xls'))

        # --- First pass: read WITHOUT headers to find the real header row ---
        if hasattr(file_path_or_buffer, 'seek'):
            file_path_or_buffer.seek(0)

        if is_excel:
            raw = self._read_excel_any(file_path_or_buffer, header=None)
        else:
            raw = pd.read_csv(file_path_or_buffer, header=None)

        header_row = self._find_header_row(raw)

        # --- Second pass: read with the detected header row ---
        if hasattr(file_path_or_buffer, 'seek'):
            file_path_or_buffer.seek(0)

        if is_excel:
            df = self._read_excel_any(file_path_or_buffer, header=header_row)
        else:
            df = pd.read_csv(file_path_or_buffer, header=header_row)

        # Handle Tally's split "Particulars" column:
        # Tally exports have [Date, "Particulars"(To/By), <unnamed>(actual desc), Vch Type, ...]
        # Merge the To/By prefix column with the unnamed description column
        df = self._merge_tally_particulars(df)

        # Drop completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        df = df.reset_index(drop=True)

        return df

    def _merge_tally_particulars(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle Tally's split Particulars column.
        In Tally exports, Particulars header spans 2 Excel columns:
          col1 = 'Particulars' (contains 'To'/'By' prefix)
          col2 = unnamed/NaN   (contains the actual ledger name / description)
        Merge them into a single 'Particulars' column."""
        cols = list(df.columns)
        part_idx = None
        for i, c in enumerate(cols):
            if str(c).strip().lower() == 'particulars':
                part_idx = i
                break
        if part_idx is None:
            return df

        # Check if next column is unnamed/nan (indicating a merged header)
        if part_idx + 1 < len(cols):
            next_col = str(cols[part_idx + 1]).strip().lower()
            if 'unnamed' in next_col or next_col == 'nan':
                part_col = cols[part_idx]
                desc_col = cols[part_idx + 1]
                # Merge: "To" + "HDFC-CC A/C" → "To HDFC-CC A/C"
                df['Particulars'] = (
                    df[part_col].astype(str).replace('nan', '').str.strip() +
                    ' ' +
                    df[desc_col].astype(str).replace('nan', '').str.strip()
                ).str.strip()
                # Drop the old split columns if they differ from 'Particulars'
                drop_cols = []
                if part_col != 'Particulars':
                    drop_cols.append(part_col)
                if desc_col != 'Particulars':
                    drop_cols.append(desc_col)
                if drop_cols:
                    df = df.drop(columns=drop_cols, errors='ignore')

        return df

    def _find_header_row(self, raw_df: pd.DataFrame) -> int:
        """Scan the first 30 rows to find the one that looks like a column header.
        Returns the 0-based row index, or 0 if no header row is detected."""
        max_scan = min(30, len(raw_df))
        best_row = 0
        best_score = 0

        for i in range(max_scan):
            row_values = raw_df.iloc[i].astype(str).str.strip().str.lower().tolist()
            score = 0
            non_empty = 0
            for val in row_values:
                if val and val != 'nan' and val != 'none':
                    non_empty += 1
                    for kw in self._HEADER_KEYWORDS:
                        if kw in val:
                            score += 1
                            break
            # A good header row has multiple keyword hits AND multiple non-empty cells
            if score >= 2 and non_empty >= 3 and score > best_score:
                best_score = score
                best_row = i

        return best_row

    def detect_columns(self, df: pd.DataFrame) -> dict:
        """Auto-detect column mappings using fuzzy name matching."""
        col_lower = {c: str(c).strip().lower().replace('_', ' ').replace('-', ' ')
                     for c in df.columns}
        mapping = {}
        used_columns = set()

        # Order matters: more specific / important fields first
        patterns = [
            ('debit',        [r'debit.*amount', r'debit.*amt', r'\bdebit\b',
                              r'\bdr\b.*amt', r'\bdr\b']),
            ('credit',       [r'credit.*amount', r'credit.*amt', r'\bcredit\b',
                              r'\bcr\b.*amt', r'\bcr\b']),
            ('date',         [r'trans.*date', r'txn.*date', r'posting.*date',
                              r'value.*date', r'vch.*date', r'\bdate\b']),
            ('voucher',      [r'voucher.*no', r'vch.*no', r'\bvoucher\b',
                              r'document.*id', r'doc.*no']),
            ('reference',    [r'ref.*no', r'ref.*number', r'\breference\b',
                              r'invoice.*no', r'\bref\b']),
            ('description',  [r'particular', r'narration', r'desc',
                              r'detail', r'remark', r'memo']),
            ('vch_type',     [r'vch.*type', r'voucher.*type', r'trans.*type',
                              r'doc.*type']),
            ('tds',          [r'\btds\b', r'tax.*deduct', r'withhold']),
            ('gst',          [r'\bgst\b', r'\bvat\b', r'service.*tax']),
            ('currency',     [r'currency', r'\bcurr\b', r'\bccy\b']),
            ('exchange_rate', [r'exchange.*rate', r'fx.*rate', r'conv.*rate',
                              r'exch.*rate']),
            ('amount',       [r'\bamount\b', r'\bamt\b']),
        ]

        for field_key, regex_list in patterns:
            if field_key in mapping:
                continue
            for col_orig, col_norm in col_lower.items():
                if col_orig in used_columns:
                    continue
                for pattern in regex_list:
                    if re.search(pattern, col_norm):
                        mapping[field_key] = col_orig
                        used_columns.add(col_orig)
                        break
                if field_key in mapping:
                    break

        return mapping

    def normalize(self, df: pd.DataFrame, column_mapping: Optional[dict] = None,
                  company_label: str = "A") -> pd.DataFrame:
        """Normalize a dataset into standard format."""
        df = df.copy()

        if column_mapping is None:
            column_mapping = self.detect_columns(df)

        rename_map = {}
        field_to_standard = {
            'date': 'transaction_date',
            'voucher': 'voucher_number',
            'reference': 'reference_number',
            'description': 'description',
            'debit': 'debit_amount',
            'credit': 'credit_amount',
            'vch_type': 'document_type',
            'tds': 'tds_amount',
            'gst': 'gst_amount',
            'currency': 'currency',
            'exchange_rate': 'exchange_rate',
        }

        for field_key, std_name in field_to_standard.items():
            if field_key in column_mapping:
                rename_map[column_mapping[field_key]] = std_name

        df = df.rename(columns=rename_map)

        # Ensure mandatory columns exist
        for col in ['transaction_date', 'debit_amount', 'credit_amount']:
            if col not in df.columns:
                df[col] = pd.NaT if col == 'transaction_date' else 0.0

        # Optional columns
        for col in ['voucher_number', 'reference_number', 'description',
                     'tds_amount', 'gst_amount', 'currency', 'exchange_rate',
                     'document_type']:
            if col not in df.columns:
                if col in ['tds_amount', 'gst_amount', 'exchange_rate']:
                    df[col] = 0.0
                elif col == 'currency':
                    df[col] = 'INR'
                else:
                    df[col] = ''

        # Parse dates — try ISO format first, then dayfirst for DD-MM-YYYY
        dates = pd.to_datetime(df['transaction_date'], errors='coerce')
        mask_failed = dates.isna() & df['transaction_date'].notna()
        if mask_failed.any():
            dates.loc[mask_failed] = pd.to_datetime(
                df.loc[mask_failed, 'transaction_date'],
                errors='coerce', dayfirst=True)
        df['transaction_date'] = dates

        # Clean amounts
        for col in ['debit_amount', 'credit_amount', 'tds_amount',
                     'gst_amount', 'exchange_rate']:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(',', '')
                .str.replace('₹', '').str.replace('$', '').str.strip(),
                errors='coerce'
            ).fillna(0.0)

        # Compute net amount (debit - credit)
        df['net_amount'] = df['debit_amount'] - df['credit_amount']
        df['abs_amount'] = df['net_amount'].abs()

        # Normalize text fields
        for col in ['voucher_number', 'reference_number', 'description',
                     'document_type']:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '')

        # Normalized description for matching
        df['description_normalized'] = df['description'].apply(
            self._normalize_text)
        df['reference_normalized'] = df['reference_number'].apply(
            self._normalize_text)

        # Add row ID
        df['row_id'] = [f"{company_label}_{i+1:06d}" for i in range(len(df))]
        df['company'] = company_label

        # ── Filter out non-transaction rows ──
        # 1. Drop rows with no valid date (totals, metadata remnants)
        df = df[df['transaction_date'].notna()]
        if df.empty:
            return df.reset_index(drop=True)

        # 2. Drop rows with dates before 2000 (numeric totals parsed as epoch)
        df = df[df['transaction_date'] >= pd.Timestamp('2000-01-01')]
        if df.empty:
            return df.reset_index(drop=True)

        # 3. Drop Opening Balance / Closing Balance rows
        desc_lower = df['description'].astype(str).str.lower().str.strip()
        skip_mask = desc_lower.apply(
            lambda x: any(s in x for s in self._SKIP_DESCRIPTIONS))
        df = df[~skip_mask]

        # 4. Drop rows with zero amounts AND empty description
        if not df.empty:
            df = df[~((df['debit_amount'] == 0) & (df['credit_amount'] == 0) &
                       (df['description'].isin(['', 'nan'])))].copy()

        df = df.reset_index(drop=True)
        return df

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Normalize text for comparison."""
        if not text or text == 'nan':
            return ''
        text = str(text).lower()
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def validate_data(self, df: pd.DataFrame, label: str) -> list:
        """Validate normalized data and return list of warnings."""
        warnings = []
        null_dates = df['transaction_date'].isna().sum()
        if null_dates > 0:
            warnings.append(
                f"{label}: {null_dates} rows have invalid/missing dates")

        zero_amounts = (
            (df['debit_amount'] == 0) & (df['credit_amount'] == 0)).sum()
        if zero_amounts > 0:
            warnings.append(
                f"{label}: {zero_amounts} rows have zero debit and credit")

        empty_refs = (df['reference_number'] == '').sum()
        if empty_refs > 0:
            warnings.append(
                f"{label}: {empty_refs} rows have no reference number")

        return warnings
