"""
Configuration module for the Inter-Company Ledger Reconciliation System.
All tolerances, thresholds, and weights are defined here.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class ReconciliationConfig:
    """Central configuration for the reconciliation engine."""

    # --- Date Tolerance ---
    date_tolerance_days: int = 7

    # --- Amount Tolerances ---
    rounding_tolerance: float = 5.0
    amount_match_tolerance_pct: float = 0.5  # percentage

    # --- Tax Detection ---
    # Includes effective TDS+GST combined rates (e.g. 10% TDS on base with 18% GST → 8.47%)
    tax_rates: List[float] = field(default_factory=lambda: [
        1.0, 2.0, 5.0, 7.5, 8.0, 8.5, 10.0, 15.0, 18.0, 20.0])
    tax_tolerance_pct: float = 1.0  # allowed deviation from exact tax rate

    # --- Forex ---
    forex_tolerance_pct: float = 5.0

    # --- Text Similarity ---
    fuzzy_match_threshold: float = 75.0  # percent similarity for narration
    reference_match_threshold: float = 85.0

    # --- Weighted Scoring Model ---
    # Weights tuned for inter-company / Tally data where ref & description
    # are always different between the two companies.
    weight_amount: float = 0.50
    weight_date: float = 0.30
    weight_reference: float = 0.10
    weight_narration: float = 0.10
    overall_match_threshold: float = 55.0  # lower because ref/desc won't match

    # --- Partial Settlement ---
    max_group_size: int = 5  # max entries to combine for partial settlement
    partial_settlement_tolerance: float = 1.0  # amount tolerance for grouped matches

    # --- Duplicate Detection ---
    duplicate_date_tolerance_days: int = 1
    duplicate_amount_tolerance: float = 0.01

    # --- Performance ---
    amount_bucket_size: float = 100.0  # bucket width for amount indexing

    # --- Column Mappings (defaults) ---
    col_date: str = "Transaction Date"
    col_voucher: str = "Voucher Number"
    col_reference: str = "Reference Number"
    col_description: str = "Description"
    col_debit: str = "Debit Amount"
    col_credit: str = "Credit Amount"
    col_tds: str = "TDS"
    col_gst: str = "GST"
    col_currency: str = "Currency"
    col_exchange_rate: str = "Exchange Rate"
    col_doc_type: str = "Document Type"

    # --- Execution Metadata ---
    algorithm_version: str = "1.0.0"

    def to_dict(self) -> Dict:
        """Convert config to dictionary for audit logging."""
        return {
            "date_tolerance_days": self.date_tolerance_days,
            "rounding_tolerance": self.rounding_tolerance,
            "amount_match_tolerance_pct": self.amount_match_tolerance_pct,
            "tax_rates": str(self.tax_rates),
            "tax_tolerance_pct": self.tax_tolerance_pct,
            "forex_tolerance_pct": self.forex_tolerance_pct,
            "fuzzy_match_threshold": self.fuzzy_match_threshold,
            "reference_match_threshold": self.reference_match_threshold,
            "weight_amount": self.weight_amount,
            "weight_date": self.weight_date,
            "weight_reference": self.weight_reference,
            "weight_narration": self.weight_narration,
            "overall_match_threshold": self.overall_match_threshold,
            "max_group_size": self.max_group_size,
            "partial_settlement_tolerance": self.partial_settlement_tolerance,
            "duplicate_date_tolerance_days": self.duplicate_date_tolerance_days,
            "duplicate_amount_tolerance": self.duplicate_amount_tolerance,
            "amount_bucket_size": self.amount_bucket_size,
            "algorithm_version": self.algorithm_version,
        }
