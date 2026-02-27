import { useState, useEffect } from 'react';
import { Loader2, AlertCircle, Table2 } from 'lucide-react';
import DataTable from './DataTable';
import { getPreview } from '../api';
import type { PreviewData } from '../types';

export default function DataPreview() {
  const [data, setData] = useState<PreviewData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [tab, setTab] = useState<'a' | 'b'>('a');

  useEffect(() => {
    setLoading(true);
    getPreview()
      .then(setData)
      .catch((e) => setError(e?.response?.data?.detail || e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <CenterMsg><Loader2 size={32} className="animate-spin text-navy-500" /></CenterMsg>;
  if (error) return <CenterMsg><AlertCircle size={24} className="text-red-500" /><span className="text-red-600 ml-2">{error}</span></CenterMsg>;
  if (!data) return null;

  const active = tab === 'a' ? data.company_a : data.company_b;

  return (
    <div className="w-full py-3 px-6 flex flex-col h-full min-h-0 animate-fadeIn">
      <div className="flex items-center justify-between mb-3 shrink-0">
        <div>
          <h2 className="text-lg font-bold text-navy-800 flex items-center gap-2">
            <Table2 size={20} /> Data Preview
          </h2>
          <p className="text-xs text-gray-500 mt-0.5">
            Data after header detection, column mapping, and row filtering
          </p>
        </div>
      </div>

      {/* Tab buttons */}
      <div className="flex gap-2 mb-3 shrink-0">
        <TabBtn active={tab === 'a'} onClick={() => setTab('a')}>
          {data.company_a.name} ({data.company_a.rows} rows)
        </TabBtn>
        <TabBtn active={tab === 'b'} onClick={() => setTab('b')}>
          {data.company_b.name} ({data.company_b.rows} rows)
        </TabBtn>
      </div>

      <div className="flex-1 min-h-0">
        <DataTable data={active.data} keyPrefix={`preview-${tab}`} />
      </div>
    </div>
  );
}

function TabBtn({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      className={`px-5 py-2 rounded-lg text-sm font-semibold transition-all
        ${active ? 'bg-navy-800 text-white shadow' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
    >
      {children}
    </button>
  );
}

function CenterMsg({ children }: { children: React.ReactNode }) {
  return <div className="flex items-center justify-center py-20">{children}</div>;
}
