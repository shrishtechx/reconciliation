import type { BalanceSummary } from '../types';

const fmt = (n: number) => n.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const diffCls = (n: number) => n === 0 ? 'text-emerald-600' : 'text-red-600';

interface Props {
  data: BalanceSummary;
  view?: 'full' | 'matched' | 'exceptions';
}

export default function BalanceSummaryCard({ data, view = 'full' }: Props) {
  const { opening_balance: ob, closing_balance: cb, matched_summary: ms, unmatched_summary: us, breakdown } = data;

  /* ─── OverView / Dashboard ─── */
  if (view === 'full') {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm mb-4 animate-fadeIn overflow-hidden">
        {/* Opening & Closing Balance */}
        <div className="grid grid-cols-1 md:grid-cols-2 divide-y md:divide-y-0 md:divide-x divide-gray-100">
          {/* Opening Balance */}
          <div className="p-4">
            <h4 className="text-xs font-semibold text-indigo-600 uppercase tracking-wide mb-3">
              Opening Balance <span className="text-gray-400 font-normal">(Before Reconciliation)</span>
            </h4>
            <div className="space-y-1.5 text-sm mb-3">
              <Row label={`Company A — ${ob.a_count} txns`} value="" cls="font-semibold text-gray-700" bold />
              <Row label="  Total Debit" value={fmt(ob.a_debit)} cls="text-emerald-700" />
              <Row label="  Total Credit" value={fmt(ob.a_credit)} cls="text-red-600" />
              <Row label="  Net" value={fmt(ob.company_a)} cls="font-bold text-navy-800" bold />
            </div>
            <div className="space-y-1.5 text-sm mb-3">
              <Row label={`Company B — ${ob.b_count} txns`} value="" cls="font-semibold text-gray-700" bold />
              <Row label="  Total Debit" value={fmt(ob.b_debit)} cls="text-emerald-700" />
              <Row label="  Total Credit" value={fmt(ob.b_credit)} cls="text-red-600" />
              <Row label="  Net" value={fmt(ob.company_b)} cls="font-bold text-navy-800" bold />
            </div>
            <div className="border-t border-gray-200 pt-2">
              <div className="flex justify-between text-sm">
                <span className="font-bold text-gray-700">Opening Balance Difference</span>
                <span className={`font-mono font-bold ${diffCls(ob.difference)}`}>
                  {ob.difference === 0 ? 'Nil' : fmt(ob.difference)}
                </span>
              </div>
            </div>
          </div>

          {/* Closing Balance */}
          <div className="p-4">
            <h4 className="text-xs font-semibold text-indigo-600 uppercase tracking-wide mb-3">
              Closing Balance <span className="text-gray-400 font-normal">(After Reconciliation)</span>
            </h4>
            <div className="space-y-1.5 text-sm mb-3">
              <Row label="Company A (Unmatched Net)" value={fmt(cb.company_a)} cls="font-bold text-navy-800" bold />
              <Row label="Company B (Unmatched Net)" value={fmt(cb.company_b)} cls="font-bold text-navy-800" bold />
            </div>
            <div className="border-t border-gray-200 pt-2">
              <div className="flex justify-between text-sm">
                <span className="font-bold text-gray-700">Closing Balance Difference</span>
                <span className={`font-mono font-bold ${diffCls(cb.difference)}`}>
                  {cb.difference === 0 ? 'Nil' : fmt(cb.difference)}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Breakdown */}
        {breakdown.length > 0 && (
          <div className="border-t border-gray-100 px-4 py-3">
            <h4 className="text-xs font-semibold text-navy-700 uppercase tracking-wide mb-2">
              Where the Difference Comes From
            </h4>
            <div className="space-y-1.5">
              {breakdown.map((item, i) => {
                const isLast = i === breakdown.length - 1;
                return (
                  <div key={i} className={`flex justify-between text-sm py-1 ${isLast ? 'border-t border-gray-200 pt-2 mt-1' : ''}`}>
                    <span className={isLast ? 'font-bold text-navy-800' : 'text-gray-600'}>{item.label}</span>
                    <span className={`font-mono ${item.amount === 0 ? 'text-emerald-600 font-bold' : isLast ? 'font-bold text-red-600' : 'text-gray-800'}`}>
                      {fmt(item.amount)}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    );
  }

  /* ─── Matches screen — only matched totals ─── */
  if (view === 'matched') {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm mb-4 animate-fadeIn overflow-hidden">
        <div className="px-4 py-3">
          <h4 className="text-xs font-semibold text-navy-700 uppercase tracking-wide mb-2">
            Matched Transactions — {ms.count} pairs
          </h4>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-sm">
            <MiniStat label="A Debit" value={fmt(ms.a_total_debit)} />
            <MiniStat label="B Credit" value={fmt(ms.b_total_credit)} />
            <MiniStat label="A Credit" value={fmt(ms.a_total_credit)} />
            <MiniStat label="B Debit" value={fmt(ms.b_total_debit)} />
            <MiniStat label="Total Amount Diff" value={fmt(ms.total_amount_diff)}
              highlight={ms.total_amount_diff !== 0} />
          </div>
        </div>
      </div>
    );
  }

  /* ─── Exceptions screen — only unmatched totals ─── */
  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm mb-4 animate-fadeIn overflow-hidden">
      {(us.count_a > 0 || us.count_b > 0) && (
        <div className="px-4 py-3">
          <h4 className="text-xs font-semibold text-amber-700 uppercase tracking-wide mb-2">
            Unmatched Transactions
          </h4>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
            {us.count_a > 0 && (
              <div className="bg-amber-50/50 rounded-lg p-3 border border-amber-100">
                <p className="text-xs font-semibold text-amber-800 mb-1">Company A — {us.count_a} items</p>
                <div className="space-y-1">
                  <Row label="Debit" value={fmt(us.a_debit)} cls="text-emerald-700" />
                  <Row label="Credit" value={fmt(us.a_credit)} cls="text-red-600" />
                  <Row label="Net" value={fmt(us.a_net)} cls="font-semibold text-navy-800" bold />
                </div>
              </div>
            )}
            {us.count_b > 0 && (
              <div className="bg-amber-50/50 rounded-lg p-3 border border-amber-100">
                <p className="text-xs font-semibold text-amber-800 mb-1">Company B — {us.count_b} items</p>
                <div className="space-y-1">
                  <Row label="Debit" value={fmt(us.b_debit)} cls="text-emerald-700" />
                  <Row label="Credit" value={fmt(us.b_credit)} cls="text-red-600" />
                  <Row label="Net" value={fmt(us.b_net)} cls="font-semibold text-navy-800" bold />
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function Row({ label, value, cls = '', bold = false }: { label: string; value: string; cls?: string; bold?: boolean }) {
  return (
    <div className="flex justify-between">
      <span className={`text-gray-500 ${bold ? 'font-medium' : ''}`}>{label}</span>
      <span className={`font-mono ${cls}`}>{value}</span>
    </div>
  );
}

function MiniStat({ label, value, highlight = false }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div className={`rounded-lg p-2 text-center ${highlight ? 'bg-amber-50 border border-amber-200' : 'bg-gray-50 border border-gray-100'}`}>
      <p className="text-[10px] text-gray-400 font-medium uppercase">{label}</p>
      <p className={`font-mono text-sm font-semibold ${highlight ? 'text-amber-700' : 'text-gray-800'}`}>{value}</p>
    </div>
  );
}
