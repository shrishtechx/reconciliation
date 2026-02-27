import { useState, useEffect } from 'react';
import { Loader2, AlertCircle, Download, CheckCircle2, XCircle, Clock, TrendingUp } from 'lucide-react';
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { getResults, getReportUrl } from '../api';
import type { FullResults } from '../types';
import BalanceSummaryCard from './BalanceSummaryCard';

const COLORS = ['#059669', '#2563eb', '#d97706', '#dc2626', '#7c3aed', '#0891b2'];

export default function ResultsDashboard() {
  const [results, setResults] = useState<FullResults | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

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

  const { summary, stats } = results;
  const matchRate = Number(summary['Match Rate A (%)'] ?? 0);
  const matchRateB = Number(summary['Match Rate B (%)'] ?? 0);

  const pieData = Object.entries(stats.match_types).map(([name, value]) => ({ name, value }));
  const confData = Object.entries(stats.confidence_distribution).map(([name, value]) => ({ name, value }));

  return (
    <div className="w-full py-3 px-6 h-full overflow-auto animate-fadeIn">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-bold text-navy-800">OverView</h2>
          <p className="text-sm text-gray-500 mt-1">
            Processed in {results.execution_time?.toFixed(2)}s
          </p>
        </div>
        <a
          href={getReportUrl()}
          download
          className="flex items-center gap-2 px-5 py-2.5 bg-emerald-600 text-white font-semibold rounded-lg hover:bg-emerald-700 transition shadow"
        >
          <Download size={16} /> Download Report
        </a>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <KpiCard
          icon={<CheckCircle2 size={22} className="text-emerald-500" />}
          label="Matched"
          value={stats.total_matches}
          color="emerald"
        />
        <KpiCard
          icon={<XCircle size={22} className="text-red-500" />}
          label="Exceptions"
          value={stats.total_exceptions}
          color="red"
        />
        <KpiCard
          icon={<TrendingUp size={22} className="text-blue-500" />}
          label="Match Rate A"
          value={`${matchRate.toFixed(1)}%`}
          color="blue"
        />
        <KpiCard
          icon={<Clock size={22} className="text-amber-500" />}
          label="Match Rate B"
          value={`${matchRateB.toFixed(1)}%`}
          color="amber"
        />
      </div>

      {/* Balance Summary */}
      {results.balance_summary && (
        <BalanceSummaryCard data={results.balance_summary} view="full" />
      )}

      {/* Summary table */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5 mb-8">
        <h3 className="text-sm font-semibold text-navy-800 mb-3 uppercase tracking-wide">Reconciliation Summary</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-12 gap-y-2">
          {Object.entries(summary).map(([k, v]) => (
            <div key={k} className="flex justify-between py-1.5 border-b border-gray-100 text-sm">
              <span className="text-gray-600">{k}</span>
              <span className="font-semibold text-gray-800">{formatVal(v)}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {pieData.length > 0 && (
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
            <h3 className="text-sm font-semibold text-navy-800 mb-4 uppercase tracking-wide">Match Type Distribution</h3>
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie data={pieData} cx="50%" cy="50%" outerRadius={100} dataKey="value" label={({ name, value }) => `${name} (${value})`}>
                  {pieData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        )}

        {confData.length > 0 && (
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
            <h3 className="text-sm font-semibold text-navy-800 mb-4 uppercase tracking-wide">Confidence Distribution</h3>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={confData}>
                <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                <YAxis allowDecimals={false} tick={{ fontSize: 12 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey="value" name="Matches" fill="#1B2A4A" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  );
}

function KpiCard({ icon, label, value, color }: { icon: React.ReactNode; label: string; value: string | number; color: string }) {
  const bgMap: Record<string, string> = {
    emerald: 'bg-emerald-50 border-emerald-200',
    red: 'bg-red-50 border-red-200',
    blue: 'bg-blue-50 border-blue-200',
    amber: 'bg-amber-50 border-amber-200',
  };
  return (
    <div className={`rounded-xl border p-5 ${bgMap[color] || 'bg-gray-50 border-gray-200'}`}>
      <div className="flex items-center gap-3 mb-2">{icon}<span className="text-sm text-gray-500 font-medium">{label}</span></div>
      <p className="text-2xl font-bold text-gray-800">{value}</p>
    </div>
  );
}

function Center({ children }: { children: React.ReactNode }) {
  return <div className="flex items-center justify-center py-20">{children}</div>;
}

function formatVal(v: unknown): string {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 2 });
  }
  return String(v);
}
