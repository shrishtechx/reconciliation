"""
Data Normalization Module.
Handles ingestion, cleaning, and standardization of financial datasets.
Supports multiple formats: Excel (.xls/.xlsx), CSV, PDF, Images (JPG/JPEG/PNG), and SAP reports.
Intelligently detects and normalizes various column structures (Debit/Credit, +/-, Dr/Cr).
Uses OpenAI GPT-4o-mini Vision API for accurate PDF and Image extraction.
"""

import pandas as pd
import numpy as np
import re
import io
import os
import base64
import json
import tempfile
import logging
from typing import Tuple, Optional, List, Union
from pathlib import Path
from .config import ReconciliationConfig

logger = logging.getLogger(__name__)

# OpenAI support for PDF/Image extraction
try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

# PyMuPDF for PDF to image conversion (no Poppler needed)
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

# PIL for image handling
try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# OpenAI API Key - can be overridden via environment variable
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "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")

# Image file extensions
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif', '.webp')


class DataNormalizer:
    """Normalizes and prepares financial ledger data for reconciliation."""

    def __init__(self, config: ReconciliationConfig):
        self.config = config

    # Keywords that indicate a row is the actual column header
    _HEADER_KEYWORDS = [
        'date', 'voucher', 'vch', 'debit', 'credit', 'particular',
        'narration', 'description', 'amount', 'reference', 'ref',
        'invoice', 'dr', 'cr', 'balance', 'type', 'transaction',
        'entry', 'ledger', 'account', 'posting',
    ]

    # Column name variations for intelligent mapping
    _DEBIT_PATTERNS = [
        r'debit', r'\bdr\b', r'\+', r'plus', r'inflow', r'receipt',
        r'received', r'deposit', r'in\b', r'money\s*in',
    ]
    _CREDIT_PATTERNS = [
        r'credit', r'\bcr\b', r'\-', r'minus', r'outflow', r'payment',
        r'paid', r'withdrawal', r'out\b', r'money\s*out',
    ]
    _AMOUNT_SIGN_PATTERNS = {
        'positive_debit': [r'\+.*debit', r'debit.*\+', r'dr.*\+'],
        'negative_credit': [r'\-.*credit', r'credit.*\-', r'cr.*\-'],
        'single_amount_with_sign': [r'amount', r'value', r'sum'],
    }

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

    def _detect_file_type(self, file_path_or_buffer) -> str:
        """Detect file type from path or buffer."""
        if isinstance(file_path_or_buffer, str):
            path = file_path_or_buffer.lower()
        else:
            path = getattr(file_path_or_buffer, 'name', '').lower()
        
        if path.endswith('.pdf'):
            return 'pdf'
        elif path.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif path.endswith('.csv'):
            return 'csv'
        elif path.endswith('.txt'):
            return 'txt'
        elif path.endswith(IMAGE_EXTENSIONS):
            return 'image'
        return 'auto'

    # ── OpenAI GPT-4o-mini Vision extraction (PDF + Images) ──

    _EXTRACTION_PROMPT = """You are a financial data extraction expert. Analyze this image of a financial ledger, statement, or report and extract ALL data into a structured JSON format.

CRITICAL: Extract EVERY single row with 100% accuracy. Do NOT miss any transaction.

The document may be structured or unstructured, in any format (Tally, SAP, bank statement, handwritten, scanned, etc.).

Return a JSON object with this EXACT structure:
{
    "headers": ["column1", "column2", ...],
    "rows": [
        ["value1", "value2", ...],
        ["value1", "value2", ...]
    ]
}

Rules:
1. Identify ALL columns visible in the table/document (Date, Particulars, Voucher Type, Voucher No, Debit, Credit, Balance, etc.)
2. Extract EVERY data row - do not skip any transaction
3. Preserve exact values: numbers (with or without commas), dates, text exactly as shown
4. If a cell is empty, use empty string ""
5. Do NOT include summary/total rows or opening/closing balance rows in the data rows
6. If data spans multiple sections or tables, combine all transaction rows using a unified column set
7. For amounts, keep the original number format - do not modify values
8. If the document has no clear table structure, still extract all financial data into logical columns

Return ONLY the JSON object, no other text or explanation."""

    def _call_openai_vision(self, base64_images: list, page_labels: list = None) -> list:
        """Call OpenAI GPT-4o-mini Vision API with one or more base64 images.
        Returns list of dicts with 'headers' and 'rows'."""
        if not HAS_OPENAI:
            raise ValueError("OpenAI package not installed. Run: pip install openai")
        if not OPENAI_API_KEY:
            raise ValueError("OpenAI API key not configured. Set OPENAI_API_KEY environment variable.")

        client = OpenAI(api_key=OPENAI_API_KEY)
        all_data = []

        for idx, b64_img in enumerate(base64_images):
            label = (page_labels[idx] if page_labels else f"Image {idx + 1}")
            logger.info(f"Sending {label} to OpenAI GPT-4o-mini for extraction...")

            # Detect image MIME type from base64 header or default to png
            mime = "image/png"
            if b64_img.startswith("/9j/"):
                mime = "image/jpeg"
            elif b64_img.startswith("iVBOR"):
                mime = "image/png"

            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": self._EXTRACTION_PROMPT},
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:{mime};base64,{b64_img}",
                                        "detail": "high"
                                    }
                                }
                            ]
                        }
                    ],
                    max_tokens=16384,
                    temperature=0
                )

                result_text = response.choices[0].message.content.strip()
                logger.info(f"{label}: Got response ({len(result_text)} chars)")

                # Strip markdown code fences if present
                if "```" in result_text:
                    result_text = re.sub(r'^```(?:json)?\s*\n?', '', result_text)
                    result_text = re.sub(r'\n?\s*```\s*$', '', result_text)

                data = json.loads(result_text)

                if 'headers' in data and 'rows' in data and len(data['rows']) > 0:
                    all_data.append(data)
                    logger.info(f"{label}: Extracted {len(data['rows'])} rows, {len(data['headers'])} columns")
                else:
                    logger.warning(f"{label}: No valid data in response")

            except json.JSONDecodeError as e:
                logger.error(f"{label}: Failed to parse JSON: {e}")
                logger.error(f"{label}: Response preview: {result_text[:300]}")
            except Exception as e:
                logger.error(f"{label}: OpenAI API error: {e}")

        return all_data

    def _combine_extracted_data(self, all_data: list) -> pd.DataFrame:
        """Combine extracted data from multiple pages/images into a single DataFrame."""
        if not all_data:
            raise ValueError("No data could be extracted from the file.")

        # Use headers from first page
        headers = all_data[0]['headers']
        all_rows = []

        for page_data in all_data:
            page_headers = page_data.get('headers', headers)
            rows = page_data.get('rows', [])

            # If this page has different columns, try to align
            if page_headers != headers:
                # Map page columns to main headers
                col_map = {}
                for i, h in enumerate(page_headers):
                    if h in headers:
                        col_map[i] = headers.index(h)
                    else:
                        # Add new column
                        headers.append(h)
                        col_map[i] = len(headers) - 1

                for row in rows:
                    aligned_row = [''] * len(headers)
                    for src_idx, dst_idx in col_map.items():
                        if src_idx < len(row):
                            aligned_row[dst_idx] = row[src_idx]
                    all_rows.append(aligned_row)
            else:
                all_rows.extend(rows)

        # Pad rows to match header length
        num_cols = len(headers)
        padded_rows = []
        for row in all_rows:
            if len(row) < num_cols:
                row = row + [''] * (num_cols - len(row))
            elif len(row) > num_cols:
                row = row[:num_cols]
            padded_rows.append(row)

        df = pd.DataFrame(padded_rows, columns=headers)
        logger.info(f"Combined result: {len(df)} rows, columns: {list(df.columns)}")
        return df

    def _pdf_to_base64_images(self, file_path_or_buffer) -> list:
        """Convert PDF pages to base64-encoded PNG images using PyMuPDF."""
        if not HAS_PYMUPDF:
            raise ValueError("PyMuPDF not installed. Run: pip install PyMuPDF")

        # Read PDF bytes
        if hasattr(file_path_or_buffer, 'read'):
            file_path_or_buffer.seek(0)
            pdf_bytes = file_path_or_buffer.read()
        else:
            with open(file_path_or_buffer, 'rb') as f:
                pdf_bytes = f.read()

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        base64_images = []

        for page_num in range(len(doc)):
            page = doc[page_num]
            # Render at 200 DPI for good quality
            pix = page.get_pixmap(dpi=200)
            img_bytes = pix.tobytes("png")
            b64 = base64.b64encode(img_bytes).decode('utf-8')
            base64_images.append(b64)
            logger.info(f"PDF page {page_num + 1}/{len(doc)}: converted to image ({pix.width}x{pix.height})")

        doc.close()
        logger.info(f"Converted PDF to {len(base64_images)} images")
        return base64_images

    def _image_to_base64(self, file_path_or_buffer) -> str:
        """Convert an image file to base64-encoded string."""
        if hasattr(file_path_or_buffer, 'read'):
            file_path_or_buffer.seek(0)
            img_bytes = file_path_or_buffer.read()
        else:
            with open(file_path_or_buffer, 'rb') as f:
                img_bytes = f.read()

        return base64.b64encode(img_bytes).decode('utf-8')

    def _extract_pdf_tables(self, file_path_or_buffer) -> pd.DataFrame:
        """Extract tables from PDF using OpenAI GPT-4o-mini Vision API via PyMuPDF."""
        logger.info("Extracting PDF data using OpenAI GPT-4o-mini + PyMuPDF...")

        # Convert PDF pages to images
        base64_images = self._pdf_to_base64_images(file_path_or_buffer)
        page_labels = [f"PDF Page {i+1}" for i in range(len(base64_images))]

        # Send to OpenAI for extraction
        all_data = self._call_openai_vision(base64_images, page_labels)

        # Combine all pages
        return self._combine_extracted_data(all_data)

    def _extract_image_tables(self, file_path_or_buffer) -> pd.DataFrame:
        """Extract tables from image files (JPG, JPEG, PNG, etc.) using OpenAI GPT-4o-mini."""
        logger.info("Extracting image data using OpenAI GPT-4o-mini...")

        b64 = self._image_to_base64(file_path_or_buffer)

        # Send to OpenAI for extraction
        all_data = self._call_openai_vision([b64], ["Image"])

        return self._combine_extracted_data(all_data)

    def _parse_sap_report(self, file_path_or_buffer) -> pd.DataFrame:
        """Parse SAP-style fixed-width or delimited reports."""
        if hasattr(file_path_or_buffer, 'read'):
            file_path_or_buffer.seek(0)
            content = file_path_or_buffer.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8', errors='ignore')
        else:
            with open(file_path_or_buffer, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        
        lines = content.strip().split('\n')
        
        # Try to detect delimiter
        delimiters = ['|', '\t', ';', ',']
        best_delim = None
        max_cols = 0
        
        for delim in delimiters:
            cols = len(lines[0].split(delim)) if lines else 0
            if cols > max_cols:
                max_cols = cols
                best_delim = delim
        
        if best_delim and max_cols > 2:
            # Parse as delimited
            data = [line.split(best_delim) for line in lines]
            df = pd.DataFrame(data)
            # Clean up whitespace
            df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
            return df
        
        # Try fixed-width parsing
        return pd.read_fwf(io.StringIO(content))

    def load_file(self, file_path_or_buffer, file_type: str = "auto") -> pd.DataFrame:
        """Load data from Excel, CSV, PDF, Image, or SAP report file.
        Auto-detects the header row for Tally-style exports that have
        company name / address metadata in the first few rows."""

        detected_type = self._detect_file_type(file_path_or_buffer) if file_type == "auto" else file_type
        
        # Handle PDF files via OpenAI GPT-4o-mini
        if detected_type == 'pdf':
            df = self._extract_pdf_tables(file_path_or_buffer)
            df = self._post_process_extracted(df)
            return df
        
        # Handle Image files (JPG, JPEG, PNG, etc.) via OpenAI GPT-4o-mini
        if detected_type == 'image':
            df = self._extract_image_tables(file_path_or_buffer)
            df = self._post_process_extracted(df)
            return df
        
        # Handle SAP/TXT files
        if detected_type == 'txt':
            df = self._parse_sap_report(file_path_or_buffer)
            df = self._post_process_extracted(df)
            return df

        is_excel = detected_type == 'excel'

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

    def _post_process_extracted(self, df: pd.DataFrame) -> pd.DataFrame:
        """Post-process extracted data (from PDF/SAP) to find headers and clean up."""
        if df.empty:
            return df
        
        # Try to find header row
        header_row = self._find_header_row(df)
        
        if header_row > 0:
            # Use detected row as header
            new_header = df.iloc[header_row].astype(str).str.strip()
            df = df.iloc[header_row + 1:].reset_index(drop=True)
            df.columns = new_header
        
        # Drop empty rows/columns
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
        """Auto-detect column mappings using fuzzy name matching.
        Handles various naming conventions: Debit/Credit, Dr/Cr, +/-, In/Out, etc."""
        col_lower = {c: str(c).strip().lower().replace('_', ' ').replace('-', ' ')
                     for c in df.columns}
        mapping = {}
        used_columns = set()

        # Order matters: more specific / important fields first
        patterns = [
            ('debit',        [r'debit.*amount', r'debit.*amt', r'\bdebit\b',
                              r'\bdr\b.*amt', r'\bdr\b', r'\+\s*amount', r'money.*in',
                              r'receipt', r'inflow', r'received', r'deposit']),
            ('credit',       [r'credit.*amount', r'credit.*amt', r'\bcredit\b',
                              r'\bcr\b.*amt', r'\bcr\b', r'\-\s*amount', r'money.*out',
                              r'payment', r'outflow', r'paid', r'withdrawal']),
            ('date',         [r'trans.*date', r'txn.*date', r'posting.*date',
                              r'value.*date', r'vch.*date', r'\bdate\b', r'entry.*date']),
            ('voucher',      [r'voucher.*no', r'vch.*no', r'\bvoucher\b',
                              r'document.*id', r'doc.*no', r'entry.*no']),
            ('reference',    [r'ref.*no', r'ref.*number', r'\breference\b',
                              r'invoice.*no', r'\bref\b', r'bill.*no', r'cheque.*no']),
            ('description',  [r'particular', r'narration', r'desc',
                              r'detail', r'remark', r'memo', r'note']),
            ('vch_type',     [r'vch.*type', r'voucher.*type', r'trans.*type',
                              r'doc.*type', r'entry.*type']),
            ('tds',          [r'\btds\b', r'tax.*deduct', r'withhold']),
            ('gst',          [r'\bgst\b', r'\bvat\b', r'service.*tax']),
            ('currency',     [r'currency', r'\bcurr\b', r'\bccy\b']),
            ('exchange_rate', [r'exchange.*rate', r'fx.*rate', r'conv.*rate',
                              r'exch.*rate']),
            ('amount',       [r'\bamount\b', r'\bamt\b', r'\bvalue\b', r'\bsum\b']),
            ('balance',      [r'\bbalance\b', r'running.*bal', r'closing.*bal']),
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

        # Handle single "Amount" column with sign indicators
        if 'amount' in mapping and 'debit' not in mapping and 'credit' not in mapping:
            mapping = self._handle_single_amount_column(df, mapping)

        return mapping

    def _handle_single_amount_column(self, df: pd.DataFrame, mapping: dict) -> dict:
        """Handle ledgers with single Amount column and sign indicators (+/-, Dr/Cr)."""
        amount_col = mapping.get('amount')
        if not amount_col:
            return mapping
        
        # Look for a sign/type indicator column
        for col in df.columns:
            col_lower = str(col).lower().strip()
            sample_vals = df[col].dropna().head(20).astype(str).str.lower().tolist()
            
            # Check if column contains Dr/Cr or +/- indicators
            has_dr_cr = any('dr' in v or 'cr' in v for v in sample_vals)
            has_plus_minus = any(v.strip() in ['+', '-'] for v in sample_vals)
            
            if has_dr_cr or has_plus_minus:
                mapping['_sign_column'] = col
                mapping['_sign_type'] = 'dr_cr' if has_dr_cr else 'plus_minus'
                break
        
        # Check if amount values themselves contain signs
        if amount_col in df.columns:
            sample_amounts = df[amount_col].dropna().head(20).astype(str).tolist()
            has_embedded_signs = any(
                v.strip().startswith('-') or v.strip().startswith('+') or
                v.strip().endswith('Dr') or v.strip().endswith('Cr') or
                v.strip().endswith('DR') or v.strip().endswith('CR')
                for v in sample_amounts
            )
            if has_embedded_signs:
                mapping['_embedded_signs'] = True
        
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

        # Handle single amount column with sign indicators
        if '_sign_column' in column_mapping or '_embedded_signs' in column_mapping:
            df = self._split_amount_by_sign(df, column_mapping)

        # Clean amounts - handle various formats
        for col in ['debit_amount', 'credit_amount', 'tds_amount',
                     'gst_amount', 'exchange_rate']:
            df[col] = self._clean_amount_column(df[col])

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

    def _clean_amount_column(self, series: pd.Series) -> pd.Series:
        """Clean amount column handling various formats and symbols."""
        def clean_value(v):
            if pd.isna(v):
                return 0.0
            s = str(v).strip()
            # Remove currency symbols and formatting
            s = re.sub(r'[₹$€£¥,\s]', '', s)
            # Handle parentheses as negative (accounting format)
            if s.startswith('(') and s.endswith(')'):
                s = '-' + s[1:-1]
            # Remove Dr/Cr suffixes
            s = re.sub(r'\s*(Dr|Cr|DR|CR)$', '', s, flags=re.IGNORECASE)
            try:
                return float(s) if s else 0.0
            except ValueError:
                return 0.0
        
        return series.apply(clean_value)

    def _split_amount_by_sign(self, df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
        """Split single amount column into debit/credit based on sign indicators."""
        if 'amount' not in column_mapping:
            return df
        
        amount_col = column_mapping['amount']
        if amount_col not in df.columns:
            return df
        
        # Initialize debit/credit columns
        df['debit_amount'] = 0.0
        df['credit_amount'] = 0.0
        
        if '_sign_column' in column_mapping:
            sign_col = column_mapping['_sign_column']
            sign_type = column_mapping.get('_sign_type', 'dr_cr')
            
            for idx, row in df.iterrows():
                amount = self._clean_amount_column(pd.Series([row[amount_col]]))[0]
                sign_val = str(row.get(sign_col, '')).strip().lower()
                
                if sign_type == 'dr_cr':
                    if 'dr' in sign_val:
                        df.at[idx, 'debit_amount'] = abs(amount)
                    elif 'cr' in sign_val:
                        df.at[idx, 'credit_amount'] = abs(amount)
                else:  # plus_minus
                    if sign_val == '+' or amount > 0:
                        df.at[idx, 'debit_amount'] = abs(amount)
                    else:
                        df.at[idx, 'credit_amount'] = abs(amount)
        
        elif '_embedded_signs' in column_mapping:
            for idx, row in df.iterrows():
                val = str(row.get(amount_col, '')).strip()
                
                # Check for Dr/Cr suffix
                if val.upper().endswith('DR'):
                    amount = self._clean_amount_column(pd.Series([val[:-2]]))[0]
                    df.at[idx, 'debit_amount'] = abs(amount)
                elif val.upper().endswith('CR'):
                    amount = self._clean_amount_column(pd.Series([val[:-2]]))[0]
                    df.at[idx, 'credit_amount'] = abs(amount)
                else:
                    amount = self._clean_amount_column(pd.Series([val]))[0]
                    if amount >= 0:
                        df.at[idx, 'debit_amount'] = abs(amount)
                    else:
                        df.at[idx, 'credit_amount'] = abs(amount)
        
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
