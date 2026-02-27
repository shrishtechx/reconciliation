export interface UploadResponse {
  file_a: string;
  file_b: string;
  rows_a: number;
  rows_b: number;
  columns_a: string[];
  columns_b: string[];
  preview_a: Record<string, unknown>[];
  preview_b: Record<string, unknown>[];
}

export interface ReconcileResponse {
  summary: Record<string, unknown>;
  stats: Stats;
  execution_time: number;
  matched_count: number;
  exception_count: number;
}

export interface Stats {
  match_types: Record<string, number>;
  exception_categories: Record<string, number>;
  confidence_distribution: Record<string, number>;
  total_matches: number;
  total_exceptions: number;
}

export interface MatchRecord {
  Transaction_ID_A: string;
  Transaction_ID_B: string;
  Match_Type: string;
  Confidence_Score: number;
  Amount_Difference: number;
  Date_Difference_Days: number | null;
  Matching_Layer: string;
  Details: string;
  A_Date: string;
  A_Description: string;
  A_Voucher: string;
  A_Debit: number;
  A_Credit: number;
  B_Date: string;
  B_Description: string;
  B_Voucher: string;
  B_Debit: number;
  B_Credit: number;
}

export interface ExceptionRecord {
  Row_ID: string;
  Company: string;
  Transaction_Date: string;
  Net_Amount: number;
  Description: string;
  Voucher: string;
  Reference: string;
  Debit: number;
  Credit: number;
  Category: string;
}

export interface BalanceSummary {
  opening_balance: {
    company_a: number; company_b: number; difference: number;
    a_debit: number; a_credit: number; b_debit: number; b_credit: number;
    a_count: number; b_count: number;
  };
  closing_balance: {
    company_a: number; company_b: number; difference: number;
  };
  balance_difference: number;
  matched_summary: {
    count: number;
    a_total_debit: number; a_total_credit: number;
    b_total_debit: number; b_total_credit: number;
    total_amount_diff: number;
  };
  unmatched_summary: {
    count_a: number; count_b: number;
    a_debit: number; a_credit: number; a_net: number;
    b_debit: number; b_credit: number; b_net: number;
  };
  breakdown: { label: string; amount: number }[];
}

export interface FullResults {
  summary: Record<string, unknown>;
  stats: Stats;
  matched: MatchRecord[];
  exceptions: ExceptionRecord[];
  execution_time: number;
  balance_summary?: BalanceSummary;
}

export interface PreviewData {
  company_a: { name: string; rows: number; data: Record<string, unknown>[] };
  company_b: { name: string; rows: number; data: Record<string, unknown>[] };
}

export interface EngineConfig {
  date_tolerance_days: number;
  rounding_tolerance: number;
  amount_match_tolerance_pct: number;
  tax_tolerance_pct: number;
  forex_tolerance_pct: number;
  fuzzy_match_threshold: number;
  reference_match_threshold: number;
  weight_amount: number;
  weight_date: number;
  weight_reference: number;
  weight_narration: number;
  overall_match_threshold: number;
  max_group_size: number;
  partial_settlement_tolerance: number;
  [key: string]: number;
}
