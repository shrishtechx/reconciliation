"""
FastAPI backend for Inter-Company Ledger Reconciliation.
Exposes the reconciliation engine as REST APIs.
Serves the built React frontend when available.
Integrates with MySQL database for user management, credits, and settings.
"""

import io
import os
import sys
import time
import logging
import tempfile
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, List
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer, set_openai_config
from reconciliation.matcher import ReconciliationEngine
from reconciliation.reporter import ReportGenerator, generate_summary_stats
from reconciliation.sample_data import save_sample_to_excel

# Database imports
from database import get_db, init_db, User, get_setting, SessionLocal
from credit_service import CreditService, get_openai_key, get_extraction_prompt, get_openai_model
from admin_routes import router as admin_router
from user_routes import router as user_router


def _get_base_dir() -> Path:
    """Get the base directory - handles both normal and PyInstaller frozen mode."""
    if getattr(sys, 'frozen', False):
        return Path(sys._MEIPASS)
    return Path(__file__).parent


BASE_DIR = _get_base_dir()
FRONTEND_DIR = BASE_DIR.parent / "frontend" / "dist"

app = FastAPI(title="Ledger Reconciliation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include admin and user routers
app.include_router(admin_router)
app.include_router(user_router)


# ── Health Check ───────────────────────────────────────────
@app.get("/api/health")
async def health_check():
    """Health check endpoint for Docker/Kubernetes."""
    return {"status": "healthy", "service": "ledger-reconciliation-api"}


# ── Startup Event ───────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    """Initialize database and load settings on startup."""
    try:
        init_db()
        logger.info("Database initialized successfully")
        
        # Load OpenAI config from database
        _load_openai_config()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        logger.warning("Running without database - some features will be unavailable")


def _load_openai_config():
    """Load OpenAI configuration from database."""
    try:
        db = SessionLocal()
        api_key = get_openai_key(db)
        prompt = get_extraction_prompt(db)
        model = get_openai_model(db)
        db.close()
        
        set_openai_config(api_key=api_key, prompt=prompt, model=model)
        logger.info("OpenAI configuration loaded from database")
    except Exception as e:
        logger.warning(f"Could not load OpenAI config from database: {e}")


def _get_current_user_id(x_user_id: Optional[str] = Header(None)) -> Optional[int]:
    """Extract user ID from request header."""
    if x_user_id:
        try:
            return int(x_user_id)
        except ValueError:
            pass
    return None


# ── In-memory state ──────────────────────────────────────────
state = {
    "config": ReconciliationConfig(),
    "df_a_raw": None,
    "df_b_raw": None,
    "df_a_norm": None,
    "df_b_norm": None,
    "results": None,
    "execution_time": None,
    "file_a_name": None,
    "file_b_name": None,
    # Multiple file support
    "files_a": [],  # List of {name, rows, df} for Company A
    "files_b": [],  # List of {name, rows, df} for Company B
    "opening_balance_a": 0.0,
    "opening_balance_b": 0.0,
    "closing_balance_a": 0.0,
    "closing_balance_b": 0.0,
}


# ── Helpers ──────────────────────────────────────────────────
def _clean_value(v):
    """Recursively convert a value to a JSON-safe Python native type."""
    if v is None:
        return None
    # Handle pandas NaT
    if isinstance(v, pd.NaT.__class__) or (hasattr(pd, 'NaT') and v is pd.NaT):
        return None
    # Handle numpy / float NaN / Inf
    if isinstance(v, float):
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    if isinstance(v, (np.floating,)):
        fv = float(v)
        if np.isnan(fv) or np.isinf(fv):
            return None
        return fv
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, np.bool_):
        return bool(v)
    # Handle pandas Timestamp
    if isinstance(v, pd.Timestamp):
        if pd.isna(v):
            return None
        return v.strftime("%Y-%m-%d")
    # Handle dicts recursively
    if isinstance(v, dict):
        return {k: _clean_value(val) for k, val in v.items()}
    # Handle lists/tuples recursively
    if isinstance(v, (list, tuple)):
        return [_clean_value(item) for item in v]
    # Handle numpy arrays
    if isinstance(v, np.ndarray):
        return [_clean_value(item) for item in v.tolist()]
    # str passthrough
    if isinstance(v, str):
        # Catch stringified NaT
        if v in ('NaT', 'nan', 'None'):
            return None
        return v
    return v


def _clean(obj):
    """Deep-clean any nested dict/list structure for JSON serialization."""
    return _clean_value(obj)


def _sanitize(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to JSON-safe list of dicts."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
    # Convert to dicts first, THEN deep-clean each record.
    # This avoids pandas converting None back to NaN in float columns.
    records = df.to_dict(orient="records")
    return [_clean_value(r) for r in records]


def _generate_balance_summary(results: dict, df_a: pd.DataFrame, df_b: pd.DataFrame) -> dict:
    """Compute balance totals, differences, and breakdown from reconciliation results.
    
    Closing Balance = Opening Balance + Total Debits - Total Credits
    The calculated closing balance is compared with the ledger closing balance
    and the difference is displayed clearly.
    """
    # ── Company A totals from normalized DataFrames ──
    a_total_debit = round(float(df_a['debit_amount'].sum()), 2)
    a_total_credit = round(float(df_a['credit_amount'].sum()), 2)
    a_net = round(a_total_debit - a_total_credit, 2)

    # ── Company B totals from normalized DataFrames ──
    b_total_debit = round(float(df_b['debit_amount'].sum()), 2)
    b_total_credit = round(float(df_b['credit_amount'].sum()), 2)
    b_net = round(b_total_debit - b_total_credit, 2)

    # ── Opening balance: net of each ledger before reconciliation ──
    opening_diff = round(a_net + b_net, 2)

    # ── Matched transaction totals ──
    matched = results.get('matched', [])
    m_a_debit = round(sum(float(m.get('A_Debit', 0) or 0) for m in matched), 2)
    m_a_credit = round(sum(float(m.get('A_Credit', 0) or 0) for m in matched), 2)
    m_b_debit = round(sum(float(m.get('B_Debit', 0) or 0) for m in matched), 2)
    m_b_credit = round(sum(float(m.get('B_Credit', 0) or 0) for m in matched), 2)
    m_total_diff = round(sum(abs(float(m.get('Amount_Difference', 0) or 0)) for m in matched), 2)

    # ── Unmatched (exceptions) totals by company ──
    exceptions = results.get('exceptions', [])
    exc_a = [e for e in exceptions if str(e.get('Company', '')).upper() == 'A']
    exc_b = [e for e in exceptions if str(e.get('Company', '')).upper() == 'B']

    exc_a_debit = round(sum(float(e.get('Debit', 0) or 0) for e in exc_a), 2)
    exc_a_credit = round(sum(float(e.get('Credit', 0) or 0) for e in exc_a), 2)
    exc_a_net = round(exc_a_debit - exc_a_credit, 2)
    exc_b_debit = round(sum(float(e.get('Debit', 0) or 0) for e in exc_b), 2)
    exc_b_credit = round(sum(float(e.get('Credit', 0) or 0) for e in exc_b), 2)
    exc_b_net = round(exc_b_debit - exc_b_credit, 2)

    # ── Closing balance calculation ──
    # Closing Balance = Opening Balance + Total Debits - Total Credits
    # For Company A: closing_a = a_net (since a_net = a_total_debit - a_total_credit)
    # After reconciliation, closing = unmatched portion only
    closing_a = round(exc_a_net, 2) if len(exc_a) > 0 else 0.0
    closing_b = round(exc_b_net, 2) if len(exc_b) > 0 else 0.0
    closing_diff = round(closing_a + closing_b, 2)

    # ── Calculated closing balance (derived from totals) ──
    # This should match: Opening + matched_removed = closing (unmatched)
    calc_closing_a = round(a_net - (m_a_debit - m_a_credit), 2)
    calc_closing_b = round(b_net - (m_b_debit - m_b_credit), 2)
    calc_closing_diff = round(calc_closing_a + calc_closing_b, 2)

    # Verify: calculated closing should equal actual closing
    closing_verified = abs(calc_closing_diff - closing_diff) < 0.01

    # ── Difference breakdown — explain where the gap comes from ──
    breakdown = []
    if m_total_diff != 0:
        breakdown.append({
            "label": "Rounding / tolerance differences in matched transactions",
            "amount": m_total_diff,
        })
    if exc_a_net != 0:
        breakdown.append({
            "label": f"Unmatched Company A transactions ({len(exc_a)} items)",
            "amount": exc_a_net,
        })
    if exc_b_net != 0:
        breakdown.append({
            "label": f"Unmatched Company B transactions ({len(exc_b)} items)",
            "amount": exc_b_net,
        })
    breakdown.append({
        "label": "Net unexplained difference",
        "amount": opening_diff,
    })

    return {
        "opening_balance": {
            "company_a": a_net,
            "company_b": b_net,
            "a_debit": a_total_debit,
            "a_credit": a_total_credit,
            "b_debit": b_total_debit,
            "b_credit": b_total_credit,
            "difference": opening_diff,
            "a_count": len(df_a),
            "b_count": len(df_b),
        },
        "closing_balance": {
            "company_a": closing_a,
            "company_b": closing_b,
            "difference": closing_diff,
            "a_debit": exc_a_debit,
            "a_credit": exc_a_credit,
            "b_debit": exc_b_debit,
            "b_credit": exc_b_credit,
            "calculated_a": calc_closing_a,
            "calculated_b": calc_closing_b,
            "calculated_difference": calc_closing_diff,
            "verified": closing_verified,
        },
        "balance_difference": opening_diff,
        "matched_summary": {
            "count": len(matched),
            "a_total_debit": m_a_debit,
            "a_total_credit": m_a_credit,
            "b_total_debit": m_b_debit,
            "b_total_credit": m_b_credit,
            "total_amount_diff": m_total_diff,
        },
        "unmatched_summary": {
            "count_a": len(exc_a),
            "count_b": len(exc_b),
            "a_debit": exc_a_debit,
            "a_credit": exc_a_credit,
            "a_net": exc_a_net,
            "b_debit": exc_b_debit,
            "b_credit": exc_b_credit,
            "b_net": exc_b_net,
        },
        "breakdown": breakdown,
    }


def _save_upload(upload: UploadFile) -> str:
    """Save uploaded file to temp path and return path."""
    suffix = os.path.splitext(upload.filename or "file.xlsx")[1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(upload.file.read())
    tmp.close()
    return tmp.name


# ── Pydantic models ─────────────────────────────────────────
class ConfigUpdate(BaseModel):
    date_tolerance_days: Optional[int] = None
    rounding_tolerance: Optional[float] = None
    amount_match_tolerance_pct: Optional[float] = None
    tax_tolerance_pct: Optional[float] = None
    forex_tolerance_pct: Optional[float] = None
    fuzzy_match_threshold: Optional[float] = None
    reference_match_threshold: Optional[float] = None
    weight_amount: Optional[float] = None
    weight_date: Optional[float] = None
    weight_reference: Optional[float] = None
    weight_narration: Optional[float] = None
    overall_match_threshold: Optional[float] = None
    max_group_size: Optional[int] = None
    partial_settlement_tolerance: Optional[float] = None


# ── API Endpoints ────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/upload")
async def upload_files(
    file_a: UploadFile = File(...),
    file_b: UploadFile = File(...),
):
    """Upload two ledger files and return preview data."""
    try:
        # Reload OpenAI config from database before processing
        _load_openai_config()
        
        norm = DataNormalizer(state["config"])

        path_a = _save_upload(file_a)
        path_b = _save_upload(file_b)

        try:
            df_a_raw = norm.load_file(path_a)
            df_b_raw = norm.load_file(path_b)
        finally:
            os.unlink(path_a)
            os.unlink(path_b)

        state["df_a_raw"] = df_a_raw
        state["df_b_raw"] = df_b_raw
        state["df_a_norm"] = None
        state["df_b_norm"] = None
        state["file_a_name"] = file_a.filename
        state["file_b_name"] = file_b.filename
        state["results"] = None
        state["execution_time"] = None
        state["files_a"] = [{"name": file_a.filename, "rows": len(df_a_raw)}]
        state["files_b"] = [{"name": file_b.filename, "rows": len(df_b_raw)}]

        logger.info(f"Uploaded: A={file_a.filename} ({len(df_a_raw)} rows), "
                    f"B={file_b.filename} ({len(df_b_raw)} rows)")

        return _clean({
            "file_a": file_a.filename,
            "file_b": file_b.filename,
            "rows_a": len(df_a_raw),
            "rows_b": len(df_b_raw),
            "columns_a": list(df_a_raw.columns),
            "columns_b": list(df_b_raw.columns),
            "preview_a": _sanitize(df_a_raw.head(10)),
            "preview_b": _sanitize(df_b_raw.head(10)),
            "files_a": state["files_a"],
            "files_b": state["files_b"],
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/upload/add")
async def add_file(
    company: str,
    file: UploadFile = File(...),
):
    """Add an additional ledger file to Company A or B."""
    if company.upper() not in ['A', 'B']:
        raise HTTPException(status_code=400, detail="Company must be 'A' or 'B'")
    
    try:
        # Reload OpenAI config from database before processing
        _load_openai_config()
        
        norm = DataNormalizer(state["config"])
        path = _save_upload(file)
        
        try:
            df_new = norm.load_file(path)
        finally:
            os.unlink(path)
        
        if company.upper() == 'A':
            if state["df_a_raw"] is None:
                state["df_a_raw"] = df_new
                state["files_a"] = [{"name": file.filename, "rows": len(df_new)}]
            else:
                state["df_a_raw"] = pd.concat([state["df_a_raw"], df_new], ignore_index=True)
                state["files_a"].append({"name": file.filename, "rows": len(df_new)})
            state["file_a_name"] = f"{len(state['files_a'])} files"
            total_rows = len(state["df_a_raw"])
        else:
            if state["df_b_raw"] is None:
                state["df_b_raw"] = df_new
                state["files_b"] = [{"name": file.filename, "rows": len(df_new)}]
            else:
                state["df_b_raw"] = pd.concat([state["df_b_raw"], df_new], ignore_index=True)
                state["files_b"].append({"name": file.filename, "rows": len(df_new)})
            state["file_b_name"] = f"{len(state['files_b'])} files"
            total_rows = len(state["df_b_raw"])
        
        # Reset results since data changed
        state["df_a_norm"] = None
        state["df_b_norm"] = None
        state["results"] = None
        
        logger.info(f"Added file to Company {company}: {file.filename} ({len(df_new)} rows)")
        
        return _clean({
            "company": company.upper(),
            "file_added": file.filename,
            "rows_added": len(df_new),
            "total_rows": total_rows,
            "files_a": state["files_a"],
            "files_b": state["files_b"],
            "total_a": len(state["df_a_raw"]) if state["df_a_raw"] is not None else 0,
            "total_b": len(state["df_b_raw"]) if state["df_b_raw"] is not None else 0,
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/upload/multiple")
async def upload_multiple_files(
    files_a: List[UploadFile] = File(default=[]),
    files_b: List[UploadFile] = File(default=[]),
):
    """Upload multiple ledger files for both companies at once."""
    if not files_a and not files_b:
        raise HTTPException(status_code=400, detail="At least one file required")
    
    try:
        # Reload OpenAI config from database before processing
        _load_openai_config()
        
        norm = DataNormalizer(state["config"])
        
        # Process Company A files
        dfs_a = []
        files_a_info = []
        for f in files_a:
            path = _save_upload(f)
            try:
                df = norm.load_file(path)
                dfs_a.append(df)
                files_a_info.append({"name": f.filename, "rows": len(df)})
            finally:
                os.unlink(path)
        
        # Process Company B files
        dfs_b = []
        files_b_info = []
        for f in files_b:
            path = _save_upload(f)
            try:
                df = norm.load_file(path)
                dfs_b.append(df)
                files_b_info.append({"name": f.filename, "rows": len(df)})
            finally:
                os.unlink(path)
        
        # Combine dataframes
        if dfs_a:
            state["df_a_raw"] = pd.concat(dfs_a, ignore_index=True) if len(dfs_a) > 1 else dfs_a[0]
            state["files_a"] = files_a_info
            state["file_a_name"] = files_a_info[0]["name"] if len(files_a_info) == 1 else f"{len(files_a_info)} files"
        
        if dfs_b:
            state["df_b_raw"] = pd.concat(dfs_b, ignore_index=True) if len(dfs_b) > 1 else dfs_b[0]
            state["files_b"] = files_b_info
            state["file_b_name"] = files_b_info[0]["name"] if len(files_b_info) == 1 else f"{len(files_b_info)} files"
        
        state["df_a_norm"] = None
        state["df_b_norm"] = None
        state["results"] = None
        state["execution_time"] = None
        
        rows_a = len(state["df_a_raw"]) if state["df_a_raw"] is not None else 0
        rows_b = len(state["df_b_raw"]) if state["df_b_raw"] is not None else 0
        
        logger.info(f"Uploaded multiple files: A={len(files_a_info)} files ({rows_a} rows), "
                    f"B={len(files_b_info)} files ({rows_b} rows)")
        
        return _clean({
            "files_a": files_a_info,
            "files_b": files_b_info,
            "total_rows_a": rows_a,
            "total_rows_b": rows_b,
            "columns_a": list(state["df_a_raw"].columns) if state["df_a_raw"] is not None else [],
            "columns_b": list(state["df_b_raw"].columns) if state["df_b_raw"] is not None else [],
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/sample")
def load_sample():
    """Load built-in sample data."""
    try:
        _, _, df_a, df_b = save_sample_to_excel()
        state["df_a_raw"] = df_a
        state["df_b_raw"] = df_b
        state["df_a_norm"] = None
        state["df_b_norm"] = None
        state["file_a_name"] = "Sample Company A"
        state["file_b_name"] = "Sample Company B"
        state["results"] = None
        state["execution_time"] = None
        return _clean({
            "file_a": "Sample Company A",
            "file_b": "Sample Company B",
            "rows_a": len(df_a),
            "rows_b": len(df_b),
            "columns_a": list(df_a.columns),
            "columns_b": list(df_b.columns),
            "preview_a": _sanitize(df_a.head(10)),
            "preview_b": _sanitize(df_b.head(10)),
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/reconcile")
def reconcile(x_user_id: Optional[str] = Header(None)):
    """Run the reconciliation engine on uploaded data."""
    if state["df_a_raw"] is None or state["df_b_raw"] is None:
        raise HTTPException(status_code=400, detail="No data uploaded. Upload files first.")

    # Get user ID for credit tracking
    user_id = None
    if x_user_id:
        try:
            user_id = int(x_user_id)
        except ValueError:
            pass

    # Check credits if user is logged in
    if user_id:
        try:
            db = SessionLocal()
            credit_service = CreditService(db)
            credits_required = credit_service.get_credits_per_reconciliation()
            
            if not credit_service.check_credits(user_id, credits_required):
                db.close()
                raise HTTPException(
                    status_code=402,
                    detail=f"Insufficient credits. Required: {credits_required}, Available: Check your balance."
                )
            db.close()
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Credit check failed: {e}")

    try:
        # Reload OpenAI config before processing (in case it was updated)
        _load_openai_config()
        
        config = state["config"]
        norm = DataNormalizer(config)
        engine = ReconciliationEngine(config)

        t0 = time.time()
        df_a = norm.normalize(state["df_a_raw"], company_label="A")
        df_b = norm.normalize(state["df_b_raw"], company_label="B")

        if len(df_a) == 0 or len(df_b) == 0:
            # Log raw data info for debugging
            raw_a_cols = list(state["df_a_raw"].columns) if state["df_a_raw"] is not None else []
            raw_b_cols = list(state["df_b_raw"].columns) if state["df_b_raw"] is not None else []
            raw_a_rows = len(state["df_a_raw"]) if state["df_a_raw"] is not None else 0
            raw_b_rows = len(state["df_b_raw"]) if state["df_b_raw"] is not None else 0
            logger.error(f"Normalization failed: A raw={raw_a_rows} rows, cols={raw_a_cols[:5]}")
            logger.error(f"Normalization failed: B raw={raw_b_rows} rows, cols={raw_b_cols[:5]}")
            raise HTTPException(
                status_code=400,
                detail=f"Normalization produced 0 rows (A={len(df_a)}, B={len(df_b)}). "
                       f"Raw data: A={raw_a_rows} rows, B={raw_b_rows} rows. "
                       f"A columns: {raw_a_cols[:5]}. Check file format.",
            )

        state["df_a_norm"] = df_a
        state["df_b_norm"] = df_b

        results = engine.reconcile(df_a, df_b)
        elapsed = time.time() - t0

        state["results"] = results
        state["execution_time"] = elapsed

        stats = generate_summary_stats(results)

        # Deduct credits and log usage
        if user_id:
            try:
                db = SessionLocal()
                credit_service = CreditService(db)
                credit_service.log_reconciliation_usage(
                    user_id=user_id,
                    file_a_name=state.get("file_a_name"),
                    file_b_name=state.get("file_b_name"),
                    rows_a=len(df_a),
                    rows_b=len(df_b),
                    matched_count=len(results["matched"]),
                    exception_count=len(results["exceptions"]),
                )
                db.close()
                logger.info(f"Credits deducted for user {user_id}")
            except Exception as e:
                logger.error(f"Failed to log usage: {e}")

        logger.info(f"Reconciliation done in {elapsed:.2f}s: "
                    f"{len(results['matched'])} matched, "
                    f"{len(results['exceptions'])} exceptions")

        return _clean({
            "summary": results["summary"],
            "stats": stats,
            "execution_time": round(elapsed, 2),
            "matched_count": len(results["matched"]),
            "exception_count": len(results["exceptions"]),
        })
    except HTTPException:
        raise
    except Exception as e:
        import traceback as _tb
        tb_str = _tb.format_exc()
        logger.error(f"Reconciliation failed:\n{tb_str}")
        raise HTTPException(status_code=500, detail=f"{e}\n---TRACEBACK---\n{tb_str}")


@app.get("/api/results")
def get_results():
    """Get full reconciliation results."""
    if state["results"] is None:
        raise HTTPException(status_code=400, detail="No results. Run reconciliation first.")

    results = state["results"]
    stats = generate_summary_stats(results)

    # Compute balance summary from normalized DataFrames
    balance_summary = {}
    if state["df_a_norm"] is not None and state["df_b_norm"] is not None:
        balance_summary = _generate_balance_summary(
            results, state["df_a_norm"], state["df_b_norm"]
        )

    return _clean({
        "summary": results["summary"],
        "stats": stats,
        "matched": results["matched"],
        "exceptions": results["exceptions"],
        "execution_time": state["execution_time"],
        "balance_summary": balance_summary,
    })


@app.get("/api/preview")
def get_preview():
    """Get data preview. Returns normalized data if available, otherwise raw uploaded data."""
    # Try normalized data first, fall back to raw
    df_a = state["df_a_norm"] if state["df_a_norm"] is not None else state["df_a_raw"]
    df_b = state["df_b_norm"] if state["df_b_norm"] is not None else state["df_b_raw"]

    if df_a is None or df_b is None:
        raise HTTPException(status_code=400, detail="No data uploaded. Upload files first.")

    label = "Normalized" if state["df_a_norm"] is not None else "Raw"

    return _clean({
        "company_a": {
            "name": f"{state['file_a_name']} ({label})",
            "rows": len(df_a),
            "data": _sanitize(df_a),
        },
        "company_b": {
            "name": f"{state['file_b_name']} ({label})",
            "rows": len(df_b),
            "data": _sanitize(df_b),
        },
    })


@app.get("/api/report")
def download_report():
    """Download Excel reconciliation report."""
    if state["results"] is None:
        raise HTTPException(status_code=400, detail="No results. Run reconciliation first.")

    reporter = ReportGenerator(state["config"])
    buf = reporter.generate_excel_report(state["results"], state["execution_time"] or 0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=reconciliation_report.xlsx"},
    )


@app.get("/api/config")
def get_config():
    """Get current engine configuration."""
    return state["config"].to_dict()


@app.put("/api/config")
def update_config(update: ConfigUpdate):
    """Update engine configuration."""
    config = state["config"]
    for field, value in update.model_dump(exclude_none=True).items():
        if hasattr(config, field):
            setattr(config, field, value)
    state["config"] = config
    return config.to_dict()


@app.get("/api/debug-exceptions")
def debug_exceptions():
    """Debug: show normalized data for unmatched transactions."""
    if state["results"] is None:
        raise HTTPException(status_code=400, detail="No results yet.")
    results = state["results"]
    exceptions = results.get("exceptions", [])
    df_a = state.get("df_a_norm")
    df_b = state.get("df_b_norm")
    debug = {"exceptions": exceptions}
    if df_a is not None:
        exc_ids_a = {e["Row_ID"] for e in exceptions if e["Company"] == "A"}
        debug["df_a_exceptions"] = df_a[df_a["row_id"].isin(exc_ids_a)][
            ["row_id","transaction_date","debit_amount","credit_amount","net_amount","description"]
        ].to_dict(orient="records")
    if df_b is not None:
        exc_ids_b = {e["Row_ID"] for e in exceptions if e["Company"] == "B"}
        debug["df_b_exceptions"] = df_b[df_b["row_id"].isin(exc_ids_b)][
            ["row_id","transaction_date","debit_amount","credit_amount","net_amount","description"]
        ].to_dict(orient="records")
    return debug


@app.post("/api/reset")
def reset():
    """Reset all data and results."""
    state["df_a_raw"] = None
    state["df_b_raw"] = None
    state["df_a_norm"] = None
    state["df_b_norm"] = None
    state["results"] = None
    state["execution_time"] = None
    state["file_a_name"] = None
    state["file_b_name"] = None
    state["files_a"] = []
    state["files_b"] = []
    state["opening_balance_a"] = 0.0
    state["opening_balance_b"] = 0.0
    state["closing_balance_a"] = 0.0
    state["closing_balance_b"] = 0.0
    state["config"] = ReconciliationConfig()
    return {"status": "reset"}


@app.get("/api/files")
def get_files():
    """Get list of uploaded files for both companies."""
    return _clean({
        "files_a": state["files_a"],
        "files_b": state["files_b"],
        "total_rows_a": len(state["df_a_raw"]) if state["df_a_raw"] is not None else 0,
        "total_rows_b": len(state["df_b_raw"]) if state["df_b_raw"] is not None else 0,
    })


@app.delete("/api/files/{company}/{index}")
def remove_file(company: str, index: int):
    """Remove a specific file from Company A or B by index."""
    if company.upper() not in ['A', 'B']:
        raise HTTPException(status_code=400, detail="Company must be 'A' or 'B'")
    
    files_key = "files_a" if company.upper() == 'A' else "files_b"
    
    if index < 0 or index >= len(state[files_key]):
        raise HTTPException(status_code=400, detail="Invalid file index")
    
    # For simplicity, we need to reload all files except the removed one
    # In production, you'd want to track individual dataframes
    removed = state[files_key].pop(index)
    
    # Reset the combined dataframe - user needs to re-upload
    if company.upper() == 'A':
        state["df_a_raw"] = None
        state["df_a_norm"] = None
    else:
        state["df_b_raw"] = None
        state["df_b_norm"] = None
    
    state["results"] = None
    
    return _clean({
        "removed": removed,
        "files_a": state["files_a"],
        "files_b": state["files_b"],
    })


# ── Login Logging ───────────────────────────────────────────
class LoginLogRequest(BaseModel):
    username: str
    password: str
    ip: str
    macAddress: str
    machineId: str
    createdDate: str

# Google Sheets configuration using Service Account
GOOGLE_CREDENTIALS_FILE = Path(__file__).parent / "google_credentials.json"
GOOGLE_SHEET_ID = "1gftoCW1ucm7GvWv1Y0x-ozbgBG2WLgx1GTxXGOK0Lc0"

def log_to_google_sheets(username: str, password: str, ip: str, mac_address: str, machine_id: str, created_date: str):
    """Log login data to Google Sheets using service account."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        
        logger.info(f"Attempting to log to Google Sheets...")
        logger.info(f"Credentials file: {GOOGLE_CREDENTIALS_FILE}")
        logger.info(f"Sheet ID: {GOOGLE_SHEET_ID}")
        
        # Define scopes
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Authenticate using service account
        creds = Credentials.from_service_account_file(str(GOOGLE_CREDENTIALS_FILE), scopes=scopes)
        logger.info("Credentials loaded successfully")
        
        client = gspread.authorize(creds)
        logger.info("gspread client authorized")
        
        # Open the spreadsheet by ID
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        logger.info(f"Spreadsheet opened: {spreadsheet.title}")
        
        worksheet = spreadsheet.sheet1  # First sheet
        logger.info(f"Worksheet selected: {worksheet.title}")
        
        # Check if headers exist, if not add them
        try:
            first_row = worksheet.row_values(1)
            if not first_row or first_row[0] != "UserName":
                worksheet.insert_row(["UserName", "Password", "IP", "MACAddress", "MachineId", "CreatedDate"], 1)
                logger.info("Headers added to sheet")
        except Exception as header_err:
            logger.warning(f"Header check failed: {header_err}")
            worksheet.insert_row(["UserName", "Password", "IP", "MACAddress", "MachineId", "CreatedDate"], 1)
        
        # Append the login data
        worksheet.append_row([username, password, ip, mac_address, machine_id, created_date])
        logger.info(f"Login logged to Google Sheets: {username} from {ip}")
        return True
    except Exception as e:
        import traceback
        logger.error(f"Failed to log to Google Sheets: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

@app.get("/api/log-login-test")
async def log_login_test():
    """Test endpoint to verify login logging route is registered."""
    return {"status": "ok", "message": "Login logging endpoint is working"}

@app.post("/api/log-login")
async def log_login(data: LoginLogRequest):
    """Log login attempt to CSV file and Google Sheets."""
    import csv
    
    # Log to local CSV file
    log_file = Path("login_logs.csv")
    file_exists = log_file.exists()
    
    try:
        with open(log_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["UserName", "Password", "IP", "MACAddress", "MachineId", "CreatedDate"])
            writer.writerow([data.username, data.password, data.ip, data.macAddress, data.machineId, data.createdDate])
        logger.info(f"Login logged to CSV: {data.username} from {data.ip}")
    except Exception as e:
        logger.error(f"Failed to log to CSV: {e}")
    
    # Log to Google Sheets using service account
    sheets_logged = False
    if GOOGLE_CREDENTIALS_FILE.exists():
        sheets_logged = log_to_google_sheets(
            data.username, data.password, data.ip, 
            data.macAddress, data.machineId, data.createdDate
        )
    else:
        logger.warning("Google credentials file not found, skipping Google Sheets logging")
    
    return {
        "status": "logged", 
        "message": "Login recorded",
        "google_sheets": sheets_logged
    }


# ── Serve built frontend (SPA) ──────────────────────────────
if FRONTEND_DIR.is_dir():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR / "assets")), name="static-assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the React SPA — any non-API route returns index.html."""
        file_path = FRONTEND_DIR / full_path
        if full_path and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(FRONTEND_DIR / "index.html"))


if __name__ == "__main__":
    import webbrowser
    import threading
    import uvicorn

    port = 8000
    def open_browser():
        webbrowser.open(f"http://localhost:{port}")

    # Open browser after a short delay
    threading.Timer(1.5, open_browser).start()

    logger.info(f"Starting Ledger Reconciliation at http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
