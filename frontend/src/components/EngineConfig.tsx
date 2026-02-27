import { useState, useEffect } from 'react';
import { Loader2, AlertCircle, Save, Settings, RotateCcw } from 'lucide-react';
import { getConfig, updateConfig } from '../api';
import type { EngineConfig as EngineConfigType } from '../types';

const CONFIG_GROUPS = [
  {
    title: 'Date & Amount Tolerances',
    fields: [
      { key: 'date_tolerance_days', label: 'Date Tolerance (days)', type: 'int' },
      { key: 'rounding_tolerance', label: 'Rounding Tolerance (₹)', type: 'float' },
      { key: 'amount_match_tolerance_pct', label: 'Amount Match Tolerance (%)', type: 'float' },
    ],
  },
  {
    title: 'Tax & Forex Detection',
    fields: [
      { key: 'tax_tolerance_pct', label: 'Tax Rate Tolerance (%)', type: 'float' },
      { key: 'forex_tolerance_pct', label: 'Forex Tolerance (%)', type: 'float' },
    ],
  },
  {
    title: 'Text Similarity',
    fields: [
      { key: 'fuzzy_match_threshold', label: 'Fuzzy Match Threshold (%)', type: 'float' },
      { key: 'reference_match_threshold', label: 'Reference Match Threshold (%)', type: 'float' },
    ],
  },
  {
    title: 'Weighted Scoring Model',
    fields: [
      { key: 'weight_amount', label: 'Weight: Amount', type: 'float' },
      { key: 'weight_date', label: 'Weight: Date', type: 'float' },
      { key: 'weight_reference', label: 'Weight: Reference', type: 'float' },
      { key: 'weight_narration', label: 'Weight: Narration', type: 'float' },
      { key: 'overall_match_threshold', label: 'Overall Match Threshold (%)', type: 'float' },
    ],
  },
  {
    title: 'Partial Settlement',
    fields: [
      { key: 'max_group_size', label: 'Max Group Size', type: 'int' },
      { key: 'partial_settlement_tolerance', label: 'Partial Settlement Tolerance (₹)', type: 'float' },
    ],
  },
];

export default function EngineConfig() {
  const [config, setConfig] = useState<EngineConfigType | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  useEffect(() => {
    setLoading(true);
    getConfig()
      .then(setConfig)
      .catch((e: unknown) => {
        const err = e as { response?: { data?: { detail?: string } }; message?: string };
        setError(err?.response?.data?.detail || err?.message || 'Error');
      })
      .finally(() => setLoading(false));
  }, []);

  const handleChange = (key: string, value: string, type: string) => {
    if (!config) return;
    const num = type === 'int' ? parseInt(value) : parseFloat(value);
    if (!isNaN(num)) {
      setConfig({ ...config, [key]: num });
    }
  };

  const handleSave = async () => {
    if (!config) return;
    setSaving(true);
    setError('');
    setSuccess('');
    try {
      const updated = await updateConfig(config);
      setConfig(updated);
      setSuccess('Configuration saved successfully');
      setTimeout(() => setSuccess(''), 3000);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      setError(err?.response?.data?.detail || err?.message || 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  const handleReset = async () => {
    setLoading(true);
    try {
      const fresh = await getConfig();
      setConfig(fresh);
    } catch {
      /* ignore */
    } finally {
      setLoading(false);
    }
  };

  if (loading) return <Center><Loader2 size={32} className="animate-spin text-navy-500" /></Center>;
  if (!config) return <Center><AlertCircle size={24} className="text-red-500" /><span className="ml-2 text-red-600">{error}</span></Center>;

  return (
    <div className="w-full py-3 px-6 h-full overflow-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-bold text-navy-800 flex items-center gap-2">
            <Settings size={22} /> Engine Configuration
          </h2>
          <p className="text-sm text-gray-500 mt-1">Adjust reconciliation parameters</p>
        </div>
        <div className="flex gap-2">
          <button onClick={handleReset} className="flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-600 hover:bg-gray-50">
            <RotateCcw size={14} /> Reset
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-2 px-5 py-2 bg-navy-800 text-white font-semibold rounded-lg hover:bg-navy-700 disabled:opacity-50 shadow"
          >
            {saving ? <Loader2 size={14} className="animate-spin" /> : <Save size={14} />} Save
          </button>
        </div>
      </div>

      {error && (
        <div className="flex items-center gap-2 p-3 mb-4 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
          <AlertCircle size={16} />{error}
        </div>
      )}
      {success && (
        <div className="flex items-center gap-2 p-3 mb-4 bg-emerald-50 border border-emerald-200 rounded-lg text-emerald-700 text-sm">
          <Save size={16} />{success}
        </div>
      )}

      <div className="space-y-6">
        {CONFIG_GROUPS.map((group) => (
          <div key={group.title} className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
            <h3 className="text-sm font-semibold text-navy-800 uppercase tracking-wide mb-4">{group.title}</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {group.fields.map((f) => (
                <div key={f.key}>
                  <label className="block text-xs font-medium text-gray-500 mb-1">{f.label}</label>
                  <input
                    type="number"
                    step={f.type === 'int' ? '1' : '0.01'}
                    value={config[f.key] ?? ''}
                    onChange={(e) => handleChange(f.key, e.target.value, f.type)}
                    className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-navy-500 focus:border-navy-500"
                  />
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function Center({ children }: { children: React.ReactNode }) {
  return <div className="flex items-center justify-center py-20">{children}</div>;
}
