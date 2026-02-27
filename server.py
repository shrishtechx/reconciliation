"""
FastAPI backend for Inter-Company Ledger Reconciliation.
Exposes the reconciliation engine as REST APIs.
Serves the built React frontend when available.
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
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
from reconciliation.matcher import ReconciliationEngine
from reconciliation.reporter import ReportGenerator, generate_summary_stats
from reconciliation.sample_data import save_sample_to_excel


def _get_base_dir() -> Path:
    """Get the base directory - handles both normal and PyInstaller frozen mode."""
    if getattr(sys, 'frozen', False):
        return Path(sys._MEIPASS)
    return Path(__file__).parent


BASE_DIR = _get_base_dir()
FRONTEND_DIR = BASE_DIR / "frontend" / "dist"

app = FastAPI(title="Ledger Reconciliation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    """Compute balance totals, differences, and breakdown from reconciliation results."""
    # Company totals from normalized DataFrames
    a_total_debit = round(float(df_a['debit_amount'].sum()), 2)
    a_total_credit = round(float(df_a['credit_amount'].sum()), 2)
    a_net = round(a_total_debit - a_total_credit, 2)

    b_total_debit = round(float(df_b['debit_amount'].sum()), 2)
    b_total_credit = round(float(df_b['credit_amount'].sum()), 2)
    b_net = round(b_total_debit - b_total_credit, 2)

    # Matched transaction totals
    matched = results.get('matched', [])
    m_a_debit = round(sum(float(m.get('A_Debit', 0) or 0) for m in matched), 2)
    m_a_credit = round(sum(float(m.get('A_Credit', 0) or 0) for m in matched), 2)
    m_b_debit = round(sum(float(m.get('B_Debit', 0) or 0) for m in matched), 2)
    m_b_credit = round(sum(float(m.get('B_Credit', 0) or 0) for m in matched), 2)
    m_total_diff = round(sum(abs(float(m.get('Amount_Difference', 0) or 0)) for m in matched), 2)

    # Unmatched (exceptions) totals by company
    exceptions = results.get('exceptions', [])
    exc_a = [e for e in exceptions if str(e.get('Company', '')).upper() == 'A']
    exc_b = [e for e in exceptions if str(e.get('Company', '')).upper() == 'B']

    exc_a_debit = round(sum(float(e.get('Debit', 0) or 0) for e in exc_a), 2)
    exc_a_credit = round(sum(float(e.get('Credit', 0) or 0) for e in exc_a), 2)
    exc_a_net = round(exc_a_debit - exc_a_credit, 2)
    exc_b_debit = round(sum(float(e.get('Debit', 0) or 0) for e in exc_b), 2)
    exc_b_credit = round(sum(float(e.get('Credit', 0) or 0) for e in exc_b), 2)
    exc_b_net = round(exc_b_debit - exc_b_credit, 2)

    # Opening balance difference (total ledger balances before reconciliation)
    opening_diff = round(a_net + b_net, 2)

    # Closing balance (unmatched amounts remaining after reconciliation)
    closing_a = round(exc_a_debit - exc_a_credit, 2) if (len(exc_a) > 0) else 0.0
    closing_b = round(exc_b_debit - exc_b_credit, 2) if (len(exc_b) > 0) else 0.0
    closing_diff = round(closing_a + closing_b, 2)

    # Difference breakdown — explain where the gap comes from
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
def reconcile():
    """Run the reconciliation engine on uploaded data."""
    if state["df_a_raw"] is None or state["df_b_raw"] is None:
        raise HTTPException(status_code=400, detail="No data uploaded. Upload files first.")

    try:
        config = state["config"]
        norm = DataNormalizer(config)
        engine = ReconciliationEngine(config)

        t0 = time.time()
        df_a = norm.normalize(state["df_a_raw"], company_label="A")
        df_b = norm.normalize(state["df_b_raw"], company_label="B")

        if len(df_a) == 0 or len(df_b) == 0:
            raise HTTPException(
                status_code=400,
                detail=f"Normalization produced 0 rows (A={len(df_a)}, B={len(df_b)}). "
                       f"Check file format.",
            )

        state["df_a_norm"] = df_a
        state["df_b_norm"] = df_b

        results = engine.reconcile(df_a, df_b)
        elapsed = time.time() - t0

        state["results"] = results
        state["execution_time"] = elapsed

        stats = generate_summary_stats(results)

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
        logger.exception("Reconciliation failed")
        raise HTTPException(status_code=500, detail=str(e))


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
    state["config"] = ReconciliationConfig()
    return {"status": "reset"}


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
