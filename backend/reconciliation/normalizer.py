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
import asyncio
import concurrent.futures
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

# pdfplumber for PDF table extraction (fallback)
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# tabula-py for PDF table extraction (fallback)
try:
    import tabula
    HAS_TABULA = True
except ImportError:
    HAS_TABULA = False

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

# OpenAI API Key and Prompt - loaded from database at runtime
# These are set dynamically via set_openai_config() before extraction
_OPENAI_API_KEY = None
_EXTRACTION_PROMPT = None
_OPENAI_MODEL = "gpt-4o-mini"


def set_openai_config(api_key: str = None, prompt: str = None, model: str = None):
    """Set OpenAI configuration from database settings."""
    global _OPENAI_API_KEY, _EXTRACTION_PROMPT, _OPENAI_MODEL
    if api_key is not None:
        _OPENAI_API_KEY = api_key
    if prompt is not None:
        _EXTRACTION_PROMPT = prompt
    if model is not None:
        _OPENAI_MODEL = model


def get_openai_api_key() -> str:
    """Get the current OpenAI API key."""
    return _OPENAI_API_KEY or os.environ.get("OPENAI_API_KEY", "")


def get_extraction_prompt() -> str:
    """Get the current extraction prompt."""
    return _EXTRACTION_PROMPT or DEFAULT_EXTRACTION_PROMPT


def get_openai_model() -> str:
    """Get the current OpenAI model."""
    return _OPENAI_MODEL or "gpt-4o-mini"


# Default extraction prompt (used if not set from DB)
DEFAULT_EXTRACTION_PROMPT = """You are a financial data extraction expert. Analyze this image of a financial ledger, statement, or report and extract ALL data into a structured JSON format.

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

    def _call_openai_vision_single(self, client, model: str, prompt: str, b64_img: str, label: str) -> dict:
        """Call OpenAI API for a single image. Used for parallel processing."""
        # Detect image MIME type from base64 header or default to png
        mime = "image/png"
        if b64_img.startswith("/9j/"):
            mime = "image/jpeg"
        elif b64_img.startswith("iVBOR"):
            mime = "image/png"

        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
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
                logger.info(f"{label}: Extracted {len(data['rows'])} rows, {len(data['headers'])} columns")
                return {"success": True, "data": data, "label": label}
            else:
                logger.warning(f"{label}: No valid data in response")
                return {"success": False, "label": label}

        except json.JSONDecodeError as e:
            logger.error(f"{label}: Failed to parse JSON: {e}")
            return {"success": False, "label": label, "error": str(e)}
        except Exception as e:
            logger.error(f"{label}: OpenAI API error: {e}")
            return {"success": False, "label": label, "error": str(e)}

    def _call_openai_vision_batch(self, client, model: str, prompt: str, base64_images: list, page_labels: list) -> list:
        """Send multiple images in a single API call (batched). More efficient for multi-page PDFs."""
        if len(base64_images) == 0:
            return []
        
        # Build content with all images
        content = [{"type": "text", "text": prompt + "\n\nExtract data from ALL pages/images below. Return a single JSON with combined data."}]
        
        for idx, b64_img in enumerate(base64_images):
            mime = "image/png"
            if b64_img.startswith("/9j/"):
                mime = "image/jpeg"
            elif b64_img.startswith("iVBOR"):
                mime = "image/png"
            
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{mime};base64,{b64_img}",
                    "detail": "high"
                }
            })
        
        logger.info(f"Sending {len(base64_images)} images in single batch to OpenAI {model}...")
        
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": content}],
                max_tokens=16384,
                temperature=0
            )

            result_text = response.choices[0].message.content.strip()
            logger.info(f"Batch: Got response ({len(result_text)} chars)")

            # Strip markdown code fences if present
            if "```" in result_text:
                result_text = re.sub(r'^```(?:json)?\s*\n?', '', result_text)
                result_text = re.sub(r'\n?\s*```\s*$', '', result_text)

            data = json.loads(result_text)

            if 'headers' in data and 'rows' in data and len(data['rows']) > 0:
                logger.info(f"Batch: Extracted {len(data['rows'])} rows, {len(data['headers'])} columns")
                return [data]
            else:
                logger.warning("Batch: No valid data in response")
                return []

        except Exception as e:
            logger.error(f"Batch API call failed: {e}, falling back to parallel individual calls")
            return None  # Signal to use parallel fallback

    def _call_openai_vision(self, base64_images: list, page_labels: list = None) -> list:
        """Call OpenAI GPT-4o-mini Vision API with one or more base64 images.
        Uses batching for efficiency (single API call for multiple pages).
        Falls back to parallel processing if batching fails.
        Returns list of dicts with 'headers' and 'rows'."""
        if not HAS_OPENAI:
            raise ValueError("OpenAI package not installed. Run: pip install openai")
        
        api_key = get_openai_api_key()
        if not api_key:
            raise ValueError("OpenAI API key not configured. Set it in Admin Settings or OPENAI_API_KEY environment variable.")

        prompt = get_extraction_prompt()
        model = get_openai_model()
        
        client = OpenAI(api_key=api_key)
        
        if page_labels is None:
            page_labels = [f"Image {i + 1}" for i in range(len(base64_images))]
        
        # Always use parallel processing - more reliable than batching with large images
        logger.info(f"Processing {len(base64_images)} images in parallel...")
        all_data = []
        
        # Use ThreadPoolExecutor for parallel API calls
        max_workers = min(len(base64_images), 5)  # Limit concurrent requests
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._call_openai_vision_single, 
                    client, model, prompt, b64_img, page_labels[idx]
                ): idx 
                for idx, b64_img in enumerate(base64_images)
            }
            
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result.get("success") and result.get("data"):
                    all_data.append(result["data"])
        
        return all_data

    def _extract_pdf_with_pdfplumber(self, file_path_or_buffer) -> pd.DataFrame:
        """Extract tables from PDF using pdfplumber (no API key needed)."""
        if not HAS_PDFPLUMBER:
            raise ValueError("pdfplumber not installed. Run: pip install pdfplumber")
        
        # Save buffer to temp file if needed
        if hasattr(file_path_or_buffer, 'read'):
            file_path_or_buffer.seek(0)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                tmp.write(file_path_or_buffer.read())
                tmp_path = tmp.name
            use_temp = True
        else:
            tmp_path = file_path_or_buffer
            use_temp = False
        
        try:
            all_rows = []
            headers = None
            
            with pdfplumber.open(tmp_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    tables = page.extract_tables()
                    
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        
                        # First row is likely headers
                        if headers is None:
                            headers = [str(h).strip() if h else f"Column_{i}" for i, h in enumerate(table[0])]
                        
                        # Add data rows
                        for row in table[1:]:
                            if row and any(cell for cell in row):  # Skip empty rows
                                all_rows.append([str(cell).strip() if cell else '' for cell in row])
            
            if not all_rows:
                raise ValueError("No tables found in PDF")
            
            # Create DataFrame
            df = pd.DataFrame(all_rows, columns=headers if headers else None)
            logger.info(f"pdfplumber extracted {len(df)} rows, {len(df.columns)} columns")
            return df
            
        finally:
            if use_temp and os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def _extract_pdf_with_tabula(self, file_path_or_buffer) -> pd.DataFrame:
        """Extract tables from PDF using tabula-py (no API key needed)."""
        if not HAS_TABULA:
            raise ValueError("tabula-py not installed. Run: pip install tabula-py")
        
        # Save buffer to temp file if needed
        if hasattr(file_path_or_buffer, 'read'):
            file_path_or_buffer.seek(0)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                tmp.write(file_path_or_buffer.read())
                tmp_path = tmp.name
            use_temp = True
        else:
            tmp_path = file_path_or_buffer
            use_temp = False
        
        try:
            # Extract all tables from all pages
            dfs = tabula.read_pdf(tmp_path, pages='all', multiple_tables=True)
            
            if not dfs:
                raise ValueError("No tables found in PDF")
            
            # Combine all tables
            combined = pd.concat(dfs, ignore_index=True)
            logger.info(f"tabula extracted {len(combined)} rows, {len(combined.columns)} columns")
            return combined
            
        finally:
            if use_temp and os.path.exists(tmp_path):
                os.unlink(tmp_path)

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
            # Render at 150 DPI - balance between quality and speed
            pix = page.get_pixmap(dpi=150)
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
        """Extract tables from PDF using available methods (pdfplumber, tabula, or OpenAI)."""
        
        # Try pdfplumber first (no API key needed)
        if HAS_PDFPLUMBER:
            try:
                logger.info("Extracting PDF data using pdfplumber...")
                return self._extract_pdf_with_pdfplumber(file_path_or_buffer)
            except Exception as e:
                logger.warning(f"pdfplumber extraction failed: {e}")
        
        # Try tabula as fallback
        if HAS_TABULA:
            try:
                logger.info("Extracting PDF data using tabula-py...")
                return self._extract_pdf_with_tabula(file_path_or_buffer)
            except Exception as e:
                logger.warning(f"tabula extraction failed: {e}")
        
        # Try OpenAI as last resort (requires API key)
        if HAS_OPENAI and get_openai_api_key():
            try:
                logger.info("Extracting PDF data using OpenAI GPT-4o-mini + PyMuPDF...")
                base64_images = self._pdf_to_base64_images(file_path_or_buffer)
                page_labels = [f"PDF Page {i+1}" for i in range(len(base64_images))]
                all_data = self._call_openai_vision(base64_images, page_labels)
                return self._combine_extracted_data(all_data)
            except Exception as e:
                logger.error(f"OpenAI extraction failed: {e}")
        
        raise ValueError("No PDF extraction method available. Install pdfplumber or tabula-py, or configure OpenAI API key.")

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

    # Sheet names that indicate summary/non-transaction data — heavily penalised
    _SUMMARY_SHEET_KEYWORDS = {
        'brs', 'summary', 'index', 'cover', 'toc', 'report', 'reconcil',
        'consolidated', 'master', 'pivot', 'dashboard',
    }

    def _select_best_excel_sheet(self, file_path_or_buffer) -> str:
        """Pick the Excel sheet most likely to contain transaction data.
        Scores each sheet by (a) number of rows with a date AND a non-zero
        number, minus a heavy penalty for known summary sheet names."""
        try:
            if hasattr(file_path_or_buffer, 'seek'):
                file_path_or_buffer.seek(0)
            xf = pd.ExcelFile(file_path_or_buffer, engine='openpyxl')
            sheet_names = xf.sheet_names
            if len(sheet_names) == 1:
                return sheet_names[0]

            best_sheet, best_score = sheet_names[0], -1
            for sname in sheet_names:
                try:
                    # Name-based penalty: summary sheets score -1000
                    sname_lower = str(sname).strip().lower()
                    name_penalty = -1000 if any(
                        kw in sname_lower for kw in self._SUMMARY_SHEET_KEYWORDS
                    ) else 0

                    df_raw = xf.parse(sname, header=None, nrows=300)
                    score = name_penalty
                    for _, row in df_raw.iterrows():
                        vals = row.tolist()
                        has_date = any(
                            isinstance(v, pd.Timestamp) or
                            (isinstance(v, str) and
                             re.search(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}', v))
                            for v in vals
                        )
                        has_number = any(
                            isinstance(v, (int, float)) and
                            not pd.isna(v) and v != 0
                            for v in vals
                        )
                        if has_date and has_number:
                            score += 1
                    if score > best_score:
                        best_score = score
                        best_sheet = sname
                except Exception:
                    continue
            logger.info(f"Selected sheet '{best_sheet}' (score={best_score}) "
                        f"from {sheet_names}")
            return best_sheet
        except Exception as e:
            logger.warning(f"Sheet selection failed: {e}, using first sheet")
            return 0

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

        # --- For Excel: pick the sheet with the most transaction data ---
        sheet_name = 0  # default: first sheet
        if is_excel:
            sheet_name = self._select_best_excel_sheet(file_path_or_buffer)
            if hasattr(file_path_or_buffer, 'seek'):
                file_path_or_buffer.seek(0)

        # --- First pass: read WITHOUT headers to find the real header row ---
        if hasattr(file_path_or_buffer, 'seek'):
            file_path_or_buffer.seek(0)

        if is_excel:
            raw = self._read_excel_any(file_path_or_buffer, header=None,
                                       sheet_name=sheet_name)
        else:
            raw = pd.read_csv(file_path_or_buffer, header=None)

        header_row = self._find_header_row(raw)

        # --- Second pass: read with the detected header row ---
        if hasattr(file_path_or_buffer, 'seek'):
            file_path_or_buffer.seek(0)

        if is_excel:
            df = self._read_excel_any(file_path_or_buffer, header=header_row,
                                      sheet_name=sheet_name)
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
            if 'unnamed' in str(next_col) or next_col == 'nan':
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
            row_values = [str(x).strip().lower() for x in raw_df.iloc[i].tolist()]
            score = 0
            non_empty = 0
            for val in row_values:
                val = str(val)  # Ensure it's a string
                if val and val != 'nan' and val != 'none':
                    non_empty += 1
                    for kw in self._HEADER_KEYWORDS:
                        if kw in str(val):
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
                              r'receipt', r'inflow', r'received', r'withdrawal']),
            ('credit',       [r'credit.*amount', r'credit.*amt', r'\bcredit\b',
                              r'\bcr\b.*amt', r'\bcr\b', r'\-\s*amount', r'money.*out',
                              r'payment', r'outflow', r'paid', r'deposit']),
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
            sample_vals = [str(x).lower() for x in df[col].dropna().head(20).tolist()]
            
            # Check if column contains Dr/Cr or +/- indicators
            has_dr_cr = any('dr' in str(v) or 'cr' in str(v) for v in sample_vals)
            has_plus_minus = any(str(v).strip() in ['+', '-'] for v in sample_vals)
            
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
