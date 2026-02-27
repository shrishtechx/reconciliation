"""
Core Matching Engine Module.
Implements multi-layer matching with weighted scoring for transaction reconciliation.
Designed to work with real-world Tally inter-company ledger exports where
voucher numbers and descriptions differ between companies.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict
from itertools import combinations
from rapidfuzz import fuzz
from .config import ReconciliationConfig


class MatchResult:
    """Represents a single match result between transactions."""

    def __init__(self, ids_a: List[str], ids_b: List[str], match_type: str,
                 confidence: float, amount_diff: float, date_diff_days: Optional[int],
                 matching_layer: str, details: str = "",
                 row_data_a: Optional[List[Dict]] = None,
                 row_data_b: Optional[List[Dict]] = None):
        self.ids_a = ids_a
        self.ids_b = ids_b
        self.match_type = match_type
        self.confidence = round(confidence, 2)
        self.amount_diff = round(amount_diff, 2)
        self.date_diff_days = date_diff_days
        self.matching_layer = matching_layer
        self.details = details
        self.row_data_a = row_data_a or []
        self.row_data_b = row_data_b or []

    @staticmethod
    def _extract_row(row) -> Dict:
        """Pull key fields from a DataFrame row for display."""
        return {
            "date": str(row.get('transaction_date', ''))[:10],
            "description": str(row.get('description', '')),
            "voucher": str(row.get('voucher_number', '')),
            "reference": str(row.get('reference_number', '')),
            "debit": float(row.get('debit_amount', 0)),
            "credit": float(row.get('credit_amount', 0)),
            "net": float(row.get('net_amount', 0)),
        }

    def to_dict(self) -> Dict:
        # Flatten row data for the first (or only) pair
        da = self.row_data_a[0] if self.row_data_a else {}
        db = self.row_data_b[0] if self.row_data_b else {}
        return {
            "Transaction_ID_A": ", ".join(self.ids_a),
            "Transaction_ID_B": ", ".join(self.ids_b),
            "Match_Type": self.match_type,
            "Confidence_Score": self.confidence,
            "Amount_Difference": self.amount_diff,
            "Date_Difference_Days": self.date_diff_days,
            "Matching_Layer": self.matching_layer,
            "Details": self.details,
            # Company A details
            "A_Date": da.get("date", ""),
            "A_Description": da.get("description", ""),
            "A_Voucher": da.get("voucher", ""),
            "A_Debit": da.get("debit", 0),
            "A_Credit": da.get("credit", 0),
            # Company B details
            "B_Date": db.get("date", ""),
            "B_Description": db.get("description", ""),
            "B_Voucher": db.get("voucher", ""),
            "B_Debit": db.get("debit", 0),
            "B_Credit": db.get("credit", 0),
        }


class ReconciliationEngine:
    """Multi-layer reconciliation engine with weighted scoring.

    Matching strategy (designed for inter-company / Tally data):
      - Primary criteria: AMOUNT (opposing direction) + DATE
      - Secondary (tiebreaker only): reference / narration similarity
      - Reference & description are NOT hard requirements because
        each company uses its own voucher numbers and the Particulars
        column in Tally shows the counter-party name (always different).
    """

    def __init__(self, config: ReconciliationConfig):
        self.config = config
        self.matches: List[MatchResult] = []
        self.matched_a: Set[str] = set()
        self.matched_b: Set[str] = set()
        self.exceptions: List[Dict] = []
        self.duplicates_a: List[Dict] = []
        self.duplicates_b: List[Dict] = []

    def reconcile(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> Dict:
        """Run full reconciliation pipeline."""
        self.matches = []
        self.matched_a = set()
        self.matched_b = set()
        self.exceptions = []
        self.duplicates_a = []
        self.duplicates_b = []

        # Build indexes for performance
        idx_a = self._build_index(df_a)
        idx_b = self._build_index(df_b)

        # Detect duplicates first (informational only)
        self._detect_duplicates(df_a, "A")
        self._detect_duplicates(df_b, "B")

        # Layer 1: Exact match (amount + date, reference as tiebreaker)
        self._layer1_exact(df_a, df_b, idx_b)

        # Layer 2: Date tolerance match (amount + date window)
        self._layer2_date_tolerance(df_a, df_b, idx_b)

        # Layer 3: Rounding / small-amount difference
        self._layer3_rounding(df_a, df_b, idx_b)

        # Layer 4: Pattern detection — tax deduction & forex
        self._layer4_patterns(df_a, df_b, idx_b)

        # Layer 5: Weighted-score tolerance match
        self._layer5_weighted(df_a, df_b, idx_b)

        # Layer 6: Partial settlement (one-to-many, many-to-one)
        self._layer6_partial(df_a, df_b)

        # Classify remaining unmatched as exceptions
        self._classify_exceptions(df_a, df_b)

        return self._compile_results(df_a, df_b)

    # ──────────────────────────────────────────────────────────
    # INDEX BUILDING
    # ──────────────────────────────────────────────────────────
    def _build_index(self, df: pd.DataFrame) -> Dict:
        """Build hash indexes for efficient lookup."""
        index = {
            'by_net': defaultdict(list),
            'by_abs': defaultdict(list),
            'by_ref': defaultdict(list),
        }
        for idx, row in df.iterrows():
            net = round(row['net_amount'], 2)
            abs_amt = round(row['abs_amount'], 2)
            index['by_net'][net].append(idx)
            index['by_abs'][abs_amt].append(idx)
            ref = str(row.get('reference_normalized', ''))
            if ref and ref != 'nan' and ref.strip():
                index['by_ref'][ref].append(idx)
        return index

    # ──────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────
    def _avail_a(self, rid: str) -> bool:
        return rid not in self.matched_a

    def _avail_b(self, rid: str) -> bool:
        return rid not in self.matched_b

    def _mark(self, ids_a: List[str], ids_b: List[str]):
        self.matched_a.update(ids_a)
        self.matched_b.update(ids_b)

    @staticmethod
    def _date_diff(row_a, row_b) -> Optional[int]:
        d_a, d_b = row_a['transaction_date'], row_b['transaction_date']
        if pd.isna(d_a) or pd.isna(d_b):
            return None
        return (d_a - d_b).days

    @staticmethod
    def _text_sim(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return fuzz.token_sort_ratio(str(a), str(b))

    def _opposing_net(self, net_a: float) -> float:
        """In inter-company, A's debit = B's credit → opposite signs."""
        return round(-net_a, 2)

    @staticmethod
    def _is_opposing(ra, rb) -> bool:
        """True when one row is debit and the other is credit."""
        return (ra['net_amount'] > 0 and rb['net_amount'] < 0) or \
               (ra['net_amount'] < 0 and rb['net_amount'] > 0)

    def _tiebreak_score(self, ra, rb) -> float:
        """Compute a tiebreaker score using reference + description similarity.
        Used to pick the BEST candidate when multiple rows match on amount+date."""
        r_sc = self._text_sim(ra.get('reference_normalized', ''),
                               rb.get('reference_normalized', ''))
        n_sc = self._text_sim(ra.get('description_normalized', ''),
                               rb.get('description_normalized', ''))
        # Also prefer closer dates
        dd = self._date_diff(ra, rb)
        d_bonus = 0 if dd is None else max(0, 10 - abs(dd))
        return r_sc * 0.4 + n_sc * 0.3 + d_bonus * 3

    def _nearby_abs(self, idx_b, target_abs, tolerance) -> List[int]:
        """Return index positions in B whose abs_amount is within tolerance."""
        out = []
        for amt, positions in idx_b['by_abs'].items():
            if abs(amt - target_abs) <= tolerance:
                out.extend(positions)
        return out

    def _row_data(self, row) -> Dict:
        return MatchResult._extract_row(row)

    # ──────────────────────────────────────────────────────────
    # LAYER 1 — EXACT MATCH
    #   Exact opposing amount + same date.
    #   Reference/description used ONLY as tiebreaker when
    #   multiple B candidates match.
    # ──────────────────────────────────────────────────────────
    def _layer1_exact(self, df_a, df_b, idx_b):
        for _, ra in df_a.iterrows():
            if not self._avail_a(ra['row_id']):
                continue
            target = self._opposing_net(round(ra['net_amount'], 2))
            candidates = []
            for ib in idx_b['by_net'].get(target, []):
                rb = df_b.loc[ib]
                if not self._avail_b(rb['row_id']):
                    continue
                dd = self._date_diff(ra, rb)
                if dd is None or dd != 0:
                    continue
                candidates.append((ib, rb))

            if not candidates:
                continue

            # Pick best candidate by tiebreaker
            if len(candidates) == 1:
                ib, rb = candidates[0]
            else:
                candidates.sort(key=lambda x: self._tiebreak_score(ra, x[1]),
                                reverse=True)
                ib, rb = candidates[0]

            ref_sim = self._text_sim(ra.get('reference_normalized', ''),
                                      rb.get('reference_normalized', ''))
            self.matches.append(MatchResult(
                [ra['row_id']], [rb['row_id']],
                "Exact Match", 100.0, 0.0, 0,
                "Layer 1 - Exact",
                f"Exact amount & date (ref sim {ref_sim:.0f}%)",
                [self._row_data(ra)], [self._row_data(rb)],
            ))
            self._mark([ra['row_id']], [rb['row_id']])

    # ──────────────────────────────────────────────────────────
    # LAYER 2 — DATE TOLERANCE MATCH
    #   Exact opposing amount + date within tolerance.
    #   If date differs, classified as "Timing Difference".
    # ──────────────────────────────────────────────────────────
    def _layer2_date_tolerance(self, df_a, df_b, idx_b):
        cfg = self.config
        for _, ra in df_a.iterrows():
            if not self._avail_a(ra['row_id']):
                continue
            target = self._opposing_net(round(ra['net_amount'], 2))
            candidates = []
            for ib in idx_b['by_net'].get(target, []):
                rb = df_b.loc[ib]
                if not self._avail_b(rb['row_id']):
                    continue
                dd = self._date_diff(ra, rb)
                if dd is None:
                    # Allow match if dates are missing
                    candidates.append((ib, rb, 0))
                elif abs(dd) <= cfg.date_tolerance_days:
                    candidates.append((ib, rb, dd))

            if not candidates:
                continue

            # Pick best: smallest date diff, then tiebreaker
            candidates.sort(key=lambda x: (abs(x[2]),
                            -self._tiebreak_score(ra, x[1])))
            ib, rb, dd = candidates[0]

            conf = 95.0 - abs(dd) * 0.8
            mtype = "Timing Difference" if abs(dd) > 0 else "Exact Match"
            self.matches.append(MatchResult(
                [ra['row_id']], [rb['row_id']],
                mtype, conf, 0.0, dd,
                "Layer 2 - Date Tolerance",
                f"Exact amount | Date diff: {dd}d",
                [self._row_data(ra)], [self._row_data(rb)],
            ))
            self._mark([ra['row_id']], [rb['row_id']])

    # ──────────────────────────────────────────────────────────
    # LAYER 3 — ROUNDING DIFFERENCE
    #   |amount_diff| ≤ rounding_tolerance, date within window.
    #   No ref/description requirement.
    # ──────────────────────────────────────────────────────────
    def _layer3_rounding(self, df_a, df_b, idx_b):
        cfg = self.config
        for _, ra in df_a.iterrows():
            if not self._avail_a(ra['row_id']):
                continue
            abs_a = ra['abs_amount']
            if abs_a == 0:
                continue
            candidates = self._nearby_abs(idx_b, abs_a, cfg.rounding_tolerance)
            best, best_key = None, (999999, 0)
            for ib in candidates:
                rb = df_b.loc[ib]
                if not self._avail_b(rb['row_id']):
                    continue
                if not self._is_opposing(ra, rb):
                    continue
                dd = self._date_diff(ra, rb)
                if dd is not None and abs(dd) > cfg.date_tolerance_days:
                    continue
                diff = abs(abs_a - rb['abs_amount'])
                if diff > cfg.rounding_tolerance or diff == 0:
                    continue
                tb = self._tiebreak_score(ra, rb)
                key = (diff, -tb)  # prefer smallest diff, then best tiebreak
                if key < best_key:
                    best_key = key
                    best = (rb, dd, diff)
            if best:
                rb, dd, diff = best
                self.matches.append(MatchResult(
                    [ra['row_id']], [rb['row_id']],
                    "Rounding Difference", 88.0, round(diff, 2), dd,
                    "Layer 3 - Rounding",
                    f"Amt diff: {diff:.2f} | Date diff: {dd}d",
                    [self._row_data(ra)], [self._row_data(rb)],
                ))
                self._mark([ra['row_id']], [rb['row_id']])

    # ──────────────────────────────────────────────────────────
    # LAYER 4 — PATTERN DETECTION (Tax / Forex)
    #   Amount differs by a known tax rate or forex %.
    #   No ref/description hard requirement.
    # ──────────────────────────────────────────────────────────
    def _layer4_patterns(self, df_a, df_b, idx_b):
        cfg = self.config
        for _, ra in df_a.iterrows():
            if not self._avail_a(ra['row_id']):
                continue
            abs_a = ra['abs_amount']
            if abs_a == 0:
                continue
            # Candidate set: amounts within 25% (covers largest tax rate)
            candidates = []
            for amt, positions in idx_b['by_abs'].items():
                if abs_a > 0 and abs(amt - abs_a) / abs_a <= 0.25:
                    candidates.extend(positions)

            best_match = None
            best_conf = 0
            for ib in candidates:
                rb = df_b.loc[ib]
                if not self._avail_b(rb['row_id']):
                    continue
                if not self._is_opposing(ra, rb):
                    continue
                abs_b = rb['abs_amount']
                if abs_b == 0:
                    continue
                dd = self._date_diff(ra, rb)
                if dd is not None and abs(dd) > cfg.date_tolerance_days * 2:
                    continue

                # Forex check (clear signal: currency ≠ INR)
                fx = self._check_forex(ra, rb)
                if fx:
                    desc, conf = fx
                    if conf > best_conf:
                        best_conf = conf
                        best_match = ("Forex Difference", conf,
                                      round(abs_a - abs_b, 2), dd, rb, desc)
                    continue

                # Tax deduction check
                tax = self._check_tax(ra, rb)
                if tax:
                    rate, conf = tax
                    if conf > best_conf:
                        best_conf = conf
                        best_match = ("Tax Deduction (TDS)", conf,
                                      round(abs_a - abs_b, 2), dd, rb,
                                      f"TDS ~{rate:.1f}% | Diff: {abs(abs_a - abs_b):.2f}")

            if best_match:
                mtype, conf, adiff, dd, rb, desc = best_match
                self.matches.append(MatchResult(
                    [ra['row_id']], [rb['row_id']],
                    mtype, conf, adiff, dd,
                    "Layer 4 - Pattern", desc,
                    [self._row_data(ra)], [self._row_data(rb)],
                ))
                self._mark([ra['row_id']], [rb['row_id']])

    # ──────────────────────────────────────────────────────────
    # LAYER 5 — WEIGHTED SCORE TOLERANCE MATCH
    #   Wider amount bucket, weighted scoring with emphasis on
    #   amount + date over reference + description.
    # ──────────────────────────────────────────────────────────
    def _layer5_weighted(self, df_a, df_b, idx_b):
        cfg = self.config
        for _, ra in df_a.iterrows():
            if not self._avail_a(ra['row_id']):
                continue
            abs_a = ra['abs_amount']
            if abs_a == 0:
                continue
            tol = max(cfg.amount_bucket_size, abs_a * 0.20)
            candidates = self._nearby_abs(idx_b, abs_a, tol)
            best, best_score = None, 0.0
            for ib in candidates:
                rb = df_b.loc[ib]
                if not self._avail_b(rb['row_id']):
                    continue
                if not self._is_opposing(ra, rb):
                    continue
                sc, det = self._weighted_score(ra, rb)
                if sc > best_score and sc >= cfg.overall_match_threshold:
                    best_score = sc
                    best = (rb, sc, det)
            if best:
                rb, sc, det = best
                dd = self._date_diff(ra, rb)
                amt_diff = round(ra['net_amount'] + rb['net_amount'], 2)
                self.matches.append(MatchResult(
                    [ra['row_id']], [rb['row_id']],
                    "Weighted Score Match", sc, amt_diff, dd,
                    "Layer 5 - Weighted", det,
                    [self._row_data(ra)], [self._row_data(rb)],
                ))
                self._mark([ra['row_id']], [rb['row_id']])

    # ──────────────────────────────────────────────────────────
    # LAYER 6 — PARTIAL SETTLEMENT / STRUCTURAL
    # ──────────────────────────────────────────────────────────
    def _layer6_partial(self, df_a, df_b):
        self._group_match(df_a, df_b, "A", "B")
        self._group_match(df_b, df_a, "B", "A")

    def _group_match(self, df_single, df_multi, lbl_s, lbl_m):
        cfg = self.config
        set_s = self.matched_a if lbl_s == "A" else self.matched_b
        set_m = self.matched_a if lbl_m == "A" else self.matched_b
        un_s = df_single[~df_single['row_id'].isin(set_s)]
        un_m = df_multi[~df_multi['row_id'].isin(set_m)]
        if un_s.empty or un_m.empty:
            return

        for _, rs in un_s.iterrows():
            avail_s = self._avail_a if lbl_s == "A" else self._avail_b
            avail_m = self._avail_a if lbl_m == "A" else self._avail_b
            if not avail_s(rs['row_id']):
                continue
            target = rs['abs_amount']
            if target == 0:
                continue
            cands = []
            for _, rm in un_m.iterrows():
                if not avail_m(rm['row_id']):
                    continue
                dd = self._date_diff(rs, rm)
                if dd is not None and abs(dd) > cfg.date_tolerance_days * 4:
                    continue
                if rm['abs_amount'] > target + cfg.partial_settlement_tolerance:
                    continue
                cands.append(rm)
            if len(cands) < 2 or len(cands) > 25:
                continue
            cands.sort(key=lambda r: r['abs_amount'], reverse=True)
            max_sz = min(cfg.max_group_size, len(cands))
            found = False
            for sz in range(2, max_sz + 1):
                if found:
                    break
                for combo in combinations(range(len(cands)), sz):
                    rows = [cands[c] for c in combo]
                    total = sum(r['abs_amount'] for r in rows)
                    diff = abs(total - target)
                    if diff <= cfg.partial_settlement_tolerance:
                        ids_m = [r['row_id'] for r in rows]
                        if not all(avail_m(rid) for rid in ids_m):
                            continue
                        ids_a = [rs['row_id']] if lbl_s == "A" else ids_m
                        ids_b = ids_m if lbl_s == "A" else [rs['row_id']]
                        mtype = "Partial Settlement (1→Many)" if lbl_s == "A" \
                                else "Aggregated Match (Many→1)"
                        rd_s = [self._row_data(rs)]
                        rd_m = [self._row_data(r) for r in rows]
                        rda = rd_s if lbl_s == "A" else rd_m
                        rdb = rd_m if lbl_s == "A" else rd_s
                        self.matches.append(MatchResult(
                            ids_a, ids_b, mtype, 82.0, round(diff, 2), None,
                            "Layer 6 - Partial Settlement",
                            f"{len(rows)} entries sum {total:,.2f} vs target {target:,.2f}",
                            rda, rdb,
                        ))
                        self._mark(ids_a, ids_b)
                        found = True
                        break

    # ──────────────────────────────────────────────────────────
    # DUPLICATE DETECTION
    # ──────────────────────────────────────────────────────────
    def _detect_duplicates(self, df, label):
        dupes, seen = [], set()
        cfg = self.config
        for i in range(len(df)):
            if i in seen:
                continue
            ri = df.iloc[i]
            for j in range(i + 1, len(df)):
                if j in seen:
                    continue
                rj = df.iloc[j]
                if abs(ri['net_amount'] - rj['net_amount']) > cfg.duplicate_amount_tolerance:
                    continue
                dd = self._date_diff(ri, rj)
                if dd is not None and abs(dd) > cfg.duplicate_date_tolerance_days:
                    continue
                # For duplicates: same company, so ref/desc SHOULD match
                rsim = self._text_sim(ri['reference_normalized'],
                                      rj['reference_normalized'])
                nsim = self._text_sim(ri['description_normalized'],
                                      rj['description_normalized'])
                if rsim >= 85 or (nsim >= 90 and abs(ri['net_amount'] - rj['net_amount']) < 0.01):
                    dupes.append({
                        "Row_ID_1": ri['row_id'], "Row_ID_2": rj['row_id'],
                        "Amount": ri['net_amount'],
                        "Date_1": str(ri['transaction_date']),
                        "Date_2": str(rj['transaction_date']),
                        "Company": label, "Category": "Duplicate Entry",
                    })
                    seen.add(j)
        if label == "A":
            self.duplicates_a = dupes
        else:
            self.duplicates_b = dupes

    # ──────────────────────────────────────────────────────────
    # EXCEPTION CLASSIFICATION
    # ──────────────────────────────────────────────────────────
    def _classify_exceptions(self, df_a, df_b):
        for _, r in df_a.iterrows():
            if r['row_id'] not in self.matched_a:
                self.exceptions.append({
                    "Row_ID": r['row_id'], "Company": "A",
                    "Transaction_Date": str(r['transaction_date']),
                    "Net_Amount": r['net_amount'],
                    "Description": r['description'],
                    "Voucher": r.get('voucher_number', ''),
                    "Reference": r.get('reference_number', ''),
                    "Debit": float(r.get('debit_amount', 0)),
                    "Credit": float(r.get('credit_amount', 0)),
                    "Category": "Missing in Company B",
                })
        for _, r in df_b.iterrows():
            if r['row_id'] not in self.matched_b:
                self.exceptions.append({
                    "Row_ID": r['row_id'], "Company": "B",
                    "Transaction_Date": str(r['transaction_date']),
                    "Net_Amount": r['net_amount'],
                    "Description": r['description'],
                    "Voucher": r.get('voucher_number', ''),
                    "Reference": r.get('reference_number', ''),
                    "Debit": float(r.get('debit_amount', 0)),
                    "Credit": float(r.get('credit_amount', 0)),
                    "Category": "Missing in Company A",
                })

    # ──────────────────────────────────────────────────────────
    # SCORING HELPERS
    # ──────────────────────────────────────────────────────────
    def _weighted_score(self, ra, rb) -> Tuple[float, str]:
        cfg = self.config
        parts = []
        abs_a, abs_b = ra['abs_amount'], rb['abs_amount']
        mx = max(abs_a, abs_b, 1)
        pct = abs(abs_a - abs_b) / mx * 100
        if pct <= cfg.amount_match_tolerance_pct:
            a_sc = 100.0
        elif pct <= 5:
            a_sc = max(0, 100 - pct * 10)
        else:
            a_sc = max(0, 100 - pct * 5)
        parts.append(f"Amt:{a_sc:.0f}")

        dd = self._date_diff(ra, rb)
        if dd is not None:
            abd = abs(dd)
            if abd == 0:
                d_sc = 100.0
            elif abd <= cfg.date_tolerance_days:
                d_sc = max(0, 100 - abd / max(cfg.date_tolerance_days, 1) * 50)
            else:
                d_sc = max(0, 50 - abd * 5)
        else:
            d_sc = 30.0
        parts.append(f"Date:{d_sc:.0f}")

        r_sc = self._text_sim(ra.get('reference_normalized', ''),
                               rb.get('reference_normalized', ''))
        parts.append(f"Ref:{r_sc:.0f}")

        n_sc = self._text_sim(ra.get('description_normalized', ''),
                               rb.get('description_normalized', ''))
        parts.append(f"Narr:{n_sc:.0f}")

        # For inter-company: amount+date carry most weight
        total = (a_sc * cfg.weight_amount + d_sc * cfg.weight_date +
                 r_sc * cfg.weight_reference + n_sc * cfg.weight_narration)
        return total, f"Scores — {', '.join(parts)} → Total: {total:.1f}"

    def _check_tax(self, ra, rb) -> Optional[Tuple[float, float]]:
        abs_a, abs_b = ra['abs_amount'], rb['abs_amount']
        if abs_a == 0 or abs_b == 0:
            return None
        larger, smaller = max(abs_a, abs_b), min(abs_a, abs_b)
        implied = (larger - smaller) / larger * 100
        for rate in self.config.tax_rates:
            if abs(implied - rate) <= self.config.tax_tolerance_pct:
                # Date proximity boosts confidence
                dd = self._date_diff(ra, rb)
                date_bonus = 5 if dd is not None and abs(dd) <= 3 else 0
                conf = min(80.0 + date_bonus, 92.0)
                return (rate, conf)
        return None

    def _check_forex(self, ra, rb) -> Optional[Tuple[str, float]]:
        ca = str(ra.get('currency', 'INR'))
        cb = str(rb.get('currency', 'INR'))
        if ca == cb == 'INR':
            return None
        abs_a, abs_b = ra['abs_amount'], rb['abs_amount']
        if abs_a == 0 or abs_b == 0:
            return None
        pct = abs(abs_a - abs_b) / max(abs_a, abs_b) * 100
        if pct <= self.config.forex_tolerance_pct:
            conf = min(82.0, 92.0)
            return (f"Forex diff {pct:.2f}% | {ca}/{cb}", conf)
        return None

    # ──────────────────────────────────────────────────────────
    # RESULT COMPILATION
    # ──────────────────────────────────────────────────────────
    def _compile_results(self, df_a, df_b) -> Dict:
        mt_counts = defaultdict(int)
        for m in self.matches:
            mt_counts[m.match_type] += 1

        ta, tb = len(df_a), len(df_b)
        ma, mb = len(self.matched_a), len(self.matched_b)
        bal_a = df_a['net_amount'].sum()
        bal_b = df_b['net_amount'].sum()

        summary = {
            "Total Transactions Company A": ta,
            "Total Transactions Company B": tb,
            "Matched (Company A side)": ma,
            "Matched (Company B side)": mb,
            "Unmatched Company A": ta - ma,
            "Unmatched Company B": tb - mb,
            "Net Balance Company A": round(bal_a, 2),
            "Net Balance Company B": round(bal_b, 2),
            "Net Balance Variance": round(bal_a + bal_b, 2),
            "Match Rate A (%)": round(ma / max(ta, 1) * 100, 1),
            "Match Rate B (%)": round(mb / max(tb, 1) * 100, 1),
        }
        summary.update({f"Count - {k}": v for k, v in mt_counts.items()})

        matched_records = [m.to_dict() for m in self.matches]
        exception_records = list(self.exceptions)
        dup_records = self.duplicates_a + self.duplicates_b

        for d in dup_records:
            exception_records.append({
                "Row_ID": d["Row_ID_1"], "Company": d["Company"],
                "Transaction_Date": d["Date_1"], "Net_Amount": d["Amount"],
                "Description": "", "Reference": "",
                "Voucher": "", "Debit": 0, "Credit": 0,
                "Category": "Duplicate Entry",
            })

        return {
            "summary": summary,
            "matched": matched_records,
            "exceptions": exception_records,
            "duplicates": dup_records,
            "df_a": df_a,
            "df_b": df_b,
        }
