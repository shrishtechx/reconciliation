import { useState, useEffect } from 'react';
import { Loader2, AlertCircle, Search } from 'lucide-react';
import DataTable from './DataTable';
import { getResults } from '../api';
import type { FullResults } from '../types';
import BalanceSummaryCard from './BalanceSummaryCard';

const MATCH_COLS = [
  'Match_Type',
  'A_Date', 'B_Date', 'Date_Difference_Days',
  'A_Description', 'B_Description',
  'A_Voucher', 'B_Voucher',
  'A_Debit', 'B_Credit',
  'A_Credit', 'B_Debit',
  'Amount_Difference', 'Matching_Layer',
];

export default function MatchDetails() {
  const [results, setResults] = useState<FullResults | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [typeFilter, setTypeFilter] = useState('All');

  useEffect(() => {
    setLoading(true);
    getResults()
      .then(setResults)
      .catch((e) => setError(e?.response?.data?.detail || e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <Center><Loader2 size={32} className="animate-spin text-navy-500" /></Center>;
  if (error) return <Center><AlertCircle size={24} className="text-red-500" /><span className="text-red-600 ml-2">{error}</span></Center>;
  if (!results) return null;

  const matchTypes = ['All', ...Array.from(new Set(results.matched.map((m) => m.Match_Type)))];
  const filtered = typeFilter === 'All'
    ? results.matched
    : results.matched.filter((m) => m.Match_Type === typeFilter);

  return (
    <div className="w-full py-3 px-6 flex flex-col h-full min-h-0 animate-fadeIn">
      <div className="flex items-center justify-between mb-3 shrink-0">
        <div>
          <h2 className="text-lg font-bold text-navy-800 flex items-center gap-2">
            <Search size={20} /> Match Details
          </h2>
          <p className="text-xs text-gray-500 mt-0.5">{results.matched.length} matched transaction pairs</p>
        </div>
      </div>

      {/* Balance Summary */}
      {results.balance_summary && (
        <div className="shrink-0">
          <BalanceSummaryCard data={results.balance_summary} view="matched" />
        </div>
      )}

      {/* Type filter chips */}
      <div className="flex flex-wrap gap-2 mb-3 shrink-0">
        {matchTypes.map((t) => (
          <button
            key={t}
            onClick={() => setTypeFilter(t)}
            className={`px-3 py-1 rounded-full text-xs font-semibold transition-all
              ${typeFilter === t
                ? 'bg-navy-800 text-white shadow'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
          >
            {t}
            {t !== 'All' && (
              <span className="ml-1 opacity-70">
                ({results.matched.filter((m) => m.Match_Type === t).length})
              </span>
            )}
          </button>
        ))}
      </div>

      <div className="flex-1 min-h-0">
        <DataTable
          data={filtered as unknown as Record<string, unknown>[]}
          columns={MATCH_COLS}
          keyPrefix="match"
        />
      </div>
    </div>
  );
}

function Center({ children }: { children: React.ReactNode }) {
  return <div className="flex items-center justify-center py-20">{children}</div>;
}
