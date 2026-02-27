"""
Report Generation Module.
Generates audit-ready Excel reports with multiple sheets.
"""

import pandas as pd
import io
from datetime import datetime
from typing import Dict
from .config import ReconciliationConfig


class ReportGenerator:
    """Generates structured Excel reconciliation reports."""

    def __init__(self, config: ReconciliationConfig):
        self.config = config

    def generate_excel_report(self, results: Dict, execution_time_seconds: float) -> io.BytesIO:
        """Generate a complete Excel report with 4 sheets."""
        output = io.BytesIO()

        with pd.ExcelWriter(output, engine='xlsxwriter',
                           engine_kwargs={'options': {'nan_inf_to_errors': True}}) as writer:
            workbook = writer.book

            # Define formats
            header_fmt = workbook.add_format({
                'bold': True, 'bg_color': '#1B2A4A', 'font_color': 'white',
                'border': 1, 'text_wrap': True, 'valign': 'vcenter',
                'align': 'center', 'font_size': 11,
            })
            cell_fmt = workbook.add_format({
                'border': 1, 'valign': 'vcenter', 'font_size': 10,
            })
            number_fmt = workbook.add_format({
                'border': 1, 'valign': 'vcenter', 'font_size': 10,
                'num_format': '#,##0.00',
            })
            pct_fmt = workbook.add_format({
                'border': 1, 'valign': 'vcenter', 'font_size': 10,
                'num_format': '0.0%',
            })
            title_fmt = workbook.add_format({
                'bold': True, 'font_size': 14, 'font_color': '#1B2A4A',
            })
            subtitle_fmt = workbook.add_format({
                'bold': True, 'font_size': 11, 'font_color': '#4A6FA5',
            })
            good_fmt = workbook.add_format({
                'border': 1, 'bg_color': '#C6EFCE', 'font_color': '#006100',
                'valign': 'vcenter', 'font_size': 10,
            })
            bad_fmt = workbook.add_format({
                'border': 1, 'bg_color': '#FFC7CE', 'font_color': '#9C0006',
                'valign': 'vcenter', 'font_size': 10,
            })
            warn_fmt = workbook.add_format({
                'border': 1, 'bg_color': '#FFEB9C', 'font_color': '#9C6500',
                'valign': 'vcenter', 'font_size': 10,
            })

            # ---- SHEET 1: SUMMARY ----
            self._write_summary_sheet(writer, workbook, results, execution_time_seconds,
                                      header_fmt, cell_fmt, number_fmt, title_fmt, subtitle_fmt,
                                      good_fmt, bad_fmt, warn_fmt)

            # ---- SHEET 2: MATCHED TRANSACTIONS ----
            self._write_matched_sheet(writer, workbook, results,
                                      header_fmt, cell_fmt, number_fmt, good_fmt, warn_fmt)

            # ---- SHEET 3: EXCEPTIONS ----
            self._write_exceptions_sheet(writer, workbook, results,
                                         header_fmt, cell_fmt, number_fmt, bad_fmt, warn_fmt)

        output.seek(0)
        return output

    def _write_summary_sheet(self, writer, workbook, results, exec_time,
                             header_fmt, cell_fmt, number_fmt, title_fmt, subtitle_fmt,
                             good_fmt, bad_fmt, warn_fmt):
        """Write the Summary sheet."""
        ws = workbook.add_worksheet('Summary')
        writer.sheets['Summary'] = ws
        ws.set_column('A:A', 40)
        ws.set_column('B:B', 25)
        ws.set_tab_color('#1B2A4A')

        row = 0
        ws.write(row, 0, 'Inter-Company Ledger Reconciliation Report', title_fmt)
        row += 1
        ws.write(row, 0, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', subtitle_fmt)
        row += 2

        ws.write(row, 0, 'Metric', header_fmt)
        ws.write(row, 1, 'Value', header_fmt)
        row += 1

        summary = results['summary']
        for key, value in summary.items():
            ws.write(row, 0, key, cell_fmt)
            if isinstance(value, float):
                ws.write(row, 1, value, number_fmt)
            else:
                ws.write(row, 1, value, cell_fmt)
            row += 1

        row += 1
        ws.write(row, 0, f'Processing Time: {exec_time:.2f} seconds', subtitle_fmt)

    def _write_matched_sheet(self, writer, workbook, results,
                             header_fmt, cell_fmt, number_fmt, good_fmt, warn_fmt):
        """Write the Matched Transactions sheet."""
        matched_df = pd.DataFrame(results['matched'])
        if matched_df.empty:
            matched_df = pd.DataFrame(columns=[
                'Transaction_ID_A', 'Transaction_ID_B', 'Match_Type',
                'Confidence_Score', 'Amount_Difference', 'Date_Difference_Days',
                'Matching_Layer', 'Details',
                'A_Date', 'A_Description', 'A_Voucher', 'A_Debit', 'A_Credit',
                'B_Date', 'B_Description', 'B_Voucher', 'B_Debit', 'B_Credit',
            ])

        ws = workbook.add_worksheet('Matched Transactions')
        writer.sheets['Matched Transactions'] = ws
        ws.set_tab_color('#006100')

        # Set column widths
        for i in range(len(matched_df.columns)):
            ws.set_column(i, i, 18)

        # Write headers
        for col_idx, col_name in enumerate(matched_df.columns):
            ws.write(0, col_idx, col_name.replace('_', ' '), header_fmt)

        # Write data
        for row_idx in range(len(matched_df)):
            for col_idx, col_name in enumerate(matched_df.columns):
                val = matched_df.iloc[row_idx, col_idx]
                if col_name == 'Confidence_Score':
                    fmt = good_fmt if val >= 90 else (warn_fmt if val >= 75 else cell_fmt)
                    ws.write(row_idx + 1, col_idx, val, fmt)
                elif col_name in ['Amount_Difference']:
                    ws.write(row_idx + 1, col_idx, val, number_fmt)
                else:
                    ws.write(row_idx + 1, col_idx, val, cell_fmt)

        # Auto-filter
        if len(matched_df) > 0:
            ws.autofilter(0, 0, len(matched_df), len(matched_df.columns) - 1)

    def _write_exceptions_sheet(self, writer, workbook, results,
                                header_fmt, cell_fmt, number_fmt, bad_fmt, warn_fmt):
        """Write the Exceptions sheet."""
        exceptions_df = pd.DataFrame(results['exceptions'])
        if exceptions_df.empty:
            exceptions_df = pd.DataFrame(columns=[
                'Row_ID', 'Company', 'Transaction_Date', 'Net_Amount',
                'Description', 'Voucher', 'Reference',
                'Debit', 'Credit', 'Category'
            ])

        ws = workbook.add_worksheet('Exceptions')
        writer.sheets['Exceptions'] = ws
        ws.set_tab_color('#9C0006')

        for i in range(len(exceptions_df.columns)):
            ws.set_column(i, i, 18)

        for col_idx, col_name in enumerate(exceptions_df.columns):
            ws.write(0, col_idx, col_name.replace('_', ' '), header_fmt)

        category_colors = {
            'Missing in Company A': bad_fmt,
            'Missing in Company B': bad_fmt,
            'Duplicate Entry': warn_fmt,
        }

        for row_idx in range(len(exceptions_df)):
            cat = exceptions_df.iloc[row_idx].get('Category', '')
            row_fmt = category_colors.get(cat, cell_fmt)
            for col_idx, col_name in enumerate(exceptions_df.columns):
                val = exceptions_df.iloc[row_idx, col_idx]
                if col_name == 'Net_Amount':
                    ws.write(row_idx + 1, col_idx, val, number_fmt)
                else:
                    ws.write(row_idx + 1, col_idx, val, row_fmt)

        if len(exceptions_df) > 0:
            ws.autofilter(0, 0, len(exceptions_df), len(exceptions_df.columns) - 1)

    def _write_audit_sheet(self, writer, workbook, exec_time,
                           header_fmt, cell_fmt, title_fmt, subtitle_fmt):
        """Write the Audit Log sheet."""
        ws = workbook.add_worksheet('Audit Log')
        writer.sheets['Audit Log'] = ws
        ws.set_column('A:A', 35)
        ws.set_column('B:B', 40)
        ws.set_tab_color('#4A6FA5')

        row = 0
        ws.write(row, 0, 'Audit Trail - Reconciliation Engine', title_fmt)
        row += 2

        ws.write(row, 0, 'Parameter', header_fmt)
        ws.write(row, 1, 'Value', header_fmt)
        row += 1

        audit_data = {
            'Execution Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Processing Duration (seconds)': f'{exec_time:.2f}',
            'Algorithm Version': self.config.algorithm_version,
        }
        audit_data.update(self.config.to_dict())

        for key, value in audit_data.items():
            ws.write(row, 0, key, cell_fmt)
            ws.write(row, 1, str(value), cell_fmt)
            row += 1

        row += 2
        ws.write(row, 0, 'Matching Layer Descriptions', subtitle_fmt)
        row += 1
        layers = [
            ('Layer 1 - Exact', 'Exact opposing amount + exact same date (ref/desc as tiebreaker)'),
            ('Layer 2 - Date Tolerance', 'Exact opposing amount + date within tolerance window'),
            ('Layer 3 - Rounding', 'Small amount difference (≤ tolerance) + date window'),
            ('Layer 4 - Pattern', 'Tax deduction (TDS) and forex exchange-rate pattern detection'),
            ('Layer 5 - Weighted', 'Weighted scoring model across amount, date, reference & narration'),
            ('Layer 6 - Partial Settlement', 'One-to-many and many-to-one grouped / structural matching'),
        ]
        for layer_name, layer_desc in layers:
            ws.write(row, 0, layer_name, cell_fmt)
            ws.write(row, 1, layer_desc, cell_fmt)
            row += 1

        row += 2
        ws.write(row, 0, 'Weighted Scoring Model', subtitle_fmt)
        row += 1
        ws.write(row, 0, 'Factor', header_fmt)
        ws.write(row, 1, 'Weight', header_fmt)
        row += 1
        weights = [
            ('Amount Similarity', f'{self.config.weight_amount * 100:.0f}%'),
            ('Date Proximity', f'{self.config.weight_date * 100:.0f}%'),
            ('Reference Similarity', f'{self.config.weight_reference * 100:.0f}%'),
            ('Narration Similarity', f'{self.config.weight_narration * 100:.0f}%'),
        ]
        for factor, weight in weights:
            ws.write(row, 0, factor, cell_fmt)
            ws.write(row, 1, weight, cell_fmt)
            row += 1


def generate_summary_stats(results: Dict) -> Dict:
    """Generate summary statistics for dashboard display."""
    summary = results['summary']
    matched = results['matched']
    exceptions = results['exceptions']

    # Match type distribution
    match_types = {}
    for m in matched:
        mt = m['Match_Type']
        match_types[mt] = match_types.get(mt, 0) + 1

    # Exception category distribution
    exception_cats = {}
    for e in exceptions:
        cat = e['Category']
        exception_cats[cat] = exception_cats.get(cat, 0) + 1

    # Confidence distribution
    confidence_ranges = {'90-100%': 0, '80-89%': 0, '75-79%': 0, '<75%': 0}
    for m in matched:
        c = m['Confidence_Score']
        if c >= 90:
            confidence_ranges['90-100%'] += 1
        elif c >= 80:
            confidence_ranges['80-89%'] += 1
        elif c >= 75:
            confidence_ranges['75-79%'] += 1
        else:
            confidence_ranges['<75%'] += 1

    return {
        'match_types': match_types,
        'exception_categories': exception_cats,
        'confidence_distribution': confidence_ranges,
        'total_matches': len(matched),
        'total_exceptions': len(exceptions),
    }
