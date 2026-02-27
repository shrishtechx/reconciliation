import { useState, useEffect } from 'react';
import { Loader2, AlertCircle, AlertTriangle } from 'lucide-react';
import DataTable from './DataTable';
import { getResults } from '../api';
import type { FullResults } from '../types';
import BalanceSummaryCard from './BalanceSummaryCard';

const EXC_COLS = [
  'Category', 'Company', 'Transaction_Date', 'Description', 'Voucher',
  'Debit', 'Credit', 'Net_Amount', 'Reference',
];

export default function Exceptions() {
  const [results, setResults] = useState<FullResults | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [catFilter, setCatFilter] = useState('All');

  useEffect(() => {
    setLoading(true);
    getResults()
      .then(setResults)
      .catch((e: unknown) => {
        const err = e as { response?: { data?: { detail?: string } }; message?: string };
        setError(err?.response?.data?.detail || err?.message || 'Error');
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <Center><Loader2 size={32} className="animate-spin text-navy-500" /></Center>;
  if (error) return <Center><AlertCircle size={24} className="text-red-500" /><span className="text-red-600 ml-2">{error}</span></Center>;
  if (!results) return null;

  const categories = ['All', ...Array.from(new Set(results.exceptions.map((e) => e.Category)))];
  const filtered = catFilter === 'All'
    ? results.exceptions
    : results.exceptions.filter((e) => e.Category === catFilter);

  return (
    <div className="w-full py-3 px-6 flex flex-col h-full min-h-0 animate-fadeIn">
      <div className="flex items-center justify-between mb-3 shrink-0">
        <div>
          <h2 className="text-lg font-bold text-navy-800 flex items-center gap-2">
            <AlertTriangle size={20} className="text-amber-500" /> Exceptions
          </h2>
          <p className="text-xs text-gray-500 mt-0.5">{results.exceptions.length} unmatched transactions</p>
        </div>
      </div>

      {/* Balance Summary */}
      {results.balance_summary && (
        <div className="shrink-0">
          <BalanceSummaryCard data={results.balance_summary} view="exceptions" />
        </div>
      )}

      {/* Category filter chips */}
      <div className="flex flex-wrap gap-2 mb-3 shrink-0">
        {categories.map((c) => (
          <button
            key={c}
            onClick={() => setCatFilter(c)}
            className={`px-3 py-1 rounded-full text-xs font-semibold transition-all
              ${catFilter === c
                ? 'bg-red-600 text-white shadow'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
          >
            {c}
            {c !== 'All' && (
              <span className="ml-1 opacity-70">
                ({results.exceptions.filter((e) => e.Category === c).length})
              </span>
            )}
          </button>
        ))}
      </div>

      <div className="flex-1 min-h-0">
        <DataTable
          data={filtered as unknown as Record<string, unknown>[]}
          columns={EXC_COLS}
          keyPrefix="exc"
        />
      </div>
    </div>
  );
}

function Center({ children }: { children: React.ReactNode }) {
  return <div className="flex items-center justify-center py-20">{children}</div>;
}
