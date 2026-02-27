import { useState, useCallback, useEffect } from 'react';
import { RotateCcw, Loader2, X, AlertCircle, CheckCircle2 } from 'lucide-react';
import Stepper from './components/Stepper';
import UploadScreen from './components/UploadScreen';
import DataPreview from './components/DataPreview';
import ResultsDashboard from './components/ResultsDashboard';
import MatchDetails from './components/MatchDetails';
import Exceptions from './components/Exceptions';
import { reconcile, resetAll } from './api';
import type { UploadResponse } from './types';

// ── Toast notification ───────────────────────────────────────
interface Toast { id: number; type: 'error' | 'success'; message: string }
function ToastContainer({ toasts, onDismiss }: { toasts: Toast[]; onDismiss: (id: number) => void }) {
  return (
    <div className="fixed top-16 right-4 z-50 flex flex-col gap-2 max-w-sm">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`flex items-start gap-2 px-4 py-3 rounded-lg shadow-xl border animate-slideIn backdrop-blur-sm
            ${t.type === 'error' ? 'bg-red-50/95 border-red-200 text-red-800' : 'bg-emerald-50/95 border-emerald-200 text-emerald-800'}`}
        >
          {t.type === 'error' ? <AlertCircle size={16} className="shrink-0 mt-0.5" /> : <CheckCircle2 size={16} className="shrink-0 mt-0.5" />}
          <span className="text-sm flex-1">{t.message}</span>
          <button onClick={() => onDismiss(t.id)} className="shrink-0 hover:opacity-70"><X size={14} /></button>
        </div>
      ))}
    </div>
  );
}

// ── Allowed file extensions ──────────────────────────────────
const VALID_EXT = ['.xls', '.xlsx', '.csv'];
const MIN_FILE_SIZE = 50; // bytes — empty file threshold

function validateFile(file: File): string | null {
  const ext = file.name.slice(file.name.lastIndexOf('.')).toLowerCase();
  if (!VALID_EXT.includes(ext)) return `"${file.name}" is not a valid format. Use .xls, .xlsx, or .csv`;
  if (file.size < MIN_FILE_SIZE) return `"${file.name}" appears to be empty (${file.size} bytes)`;
  if (file.size > 50 * 1024 * 1024) return `"${file.name}" is too large (max 50 MB)`;
  return null;
}

export default function App() {
  const [step, setStep] = useState(0);
  const [hasData, setHasData] = useState(false);
  const [hasResults, setHasResults] = useState(false);
  const [reconciling, setReconciling] = useState(false);
  const [uploadInfo, setUploadInfo] = useState<UploadResponse | null>(null);
  const [dataKey, setDataKey] = useState(0);
  const [resultKey, setResultKey] = useState(0);
  const [uploadKey, setUploadKey] = useState(0);
  const [uploadValid, setUploadValid] = useState(true);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [toastId, setToastId] = useState(0);

  const addToast = useCallback((type: 'error' | 'success', message: string) => {
    const id = Date.now();
    setToasts((prev) => [...prev, { id, type, message }]);
    setToastId((k) => k + 1);
  }, []);

  const dismissToast = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  // Auto-dismiss toasts after 5s
  useEffect(() => {
    if (toasts.length === 0) return;
    const timer = setTimeout(() => setToasts((prev) => prev.slice(1)), 5000);
    return () => clearTimeout(timer);
  }, [toasts.length, toastId]);

  const handleUploadError = useCallback((msg: string) => {
    setUploadValid(false);
    addToast('error', msg);
  }, [addToast]);

  const handleUploaded = useCallback((data: UploadResponse) => {
    // Validate the response data
    if (data.rows_a === 0 || data.rows_b === 0) {
      setUploadValid(false);
      addToast('error', `Ledger file has no data rows (A: ${data.rows_a}, B: ${data.rows_b}). Please upload valid ledger files.`);
      return;
    }
    setUploadInfo(data);
    setHasData(true);
    setHasResults(false);
    setUploadValid(true);
    setStep(0);
    setDataKey((k) => k + 1);
    setResultKey((k) => k + 1);
    addToast('success', `Files uploaded: ${data.rows_a} + ${data.rows_b} rows`);
  }, [addToast]);

  const handleReconcile = useCallback(async () => {
    setReconciling(true);
    try {
      await reconcile();
      setHasResults(true);
      setResultKey((k) => k + 1);
      setDataKey((k) => k + 1);
      setStep(1);
      addToast('success', 'Reconciliation complete!');
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      addToast('error', err?.response?.data?.detail || err?.message || 'Reconciliation failed');
    } finally {
      setReconciling(false);
    }
  }, [addToast]);

  const handleReset = useCallback(async () => {
    await resetAll();
    setStep(0);
    setHasData(false);
    setHasResults(false);
    setUploadInfo(null);
    setUploadValid(true);
    setDataKey(0);
    setResultKey(0);
    setUploadKey((k) => k + 1);
    setToasts([]);
    addToast('success', 'All data cleared');
  }, [addToast]);

  return (
    <div className="h-screen w-screen bg-gradient-to-br from-gray-50 to-gray-100 flex flex-col overflow-hidden">
      {/* Toast notifications */}
      <ToastContainer toasts={toasts} onDismiss={dismissToast} />

      {/* Header with stepper inline */}
      <header className="bg-gradient-to-r from-navy-900 via-navy-800 to-navy-900 text-white px-5 py-2.5 flex items-center shadow-xl shrink-0 border-b border-navy-700/50">
        {/* Left — Brand */}
        <div className="flex items-center gap-3 shrink-0">
          <div className="w-9 h-9 rounded-lg bg-white/10 flex items-center justify-center backdrop-blur-sm">
            <span className="text-lg">⚖️</span>
          </div>
          <div>
            <h1 className="text-base font-bold tracking-tight leading-tight">Ledger Reconciliation</h1>
            <p className="text-[11px] text-navy-300 leading-tight">Inter-Company Matching</p>
          </div>
        </div>

        {/* Center — Stepper */}
        <div className="flex-1 flex justify-center px-6">
          <Stepper active={step} onStep={setStep} hasData={hasData} hasResults={hasResults} />
        </div>

        {/* Right — Reset */}
        <button
          onClick={handleReset}
          className="flex items-center gap-2 px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg text-xs font-medium transition-all duration-200 shrink-0 hover:scale-105 active:scale-95"
        >
          <RotateCcw size={13} /> Reset
        </button>
      </header>

      {/* Content — fills remaining viewport */}
      <main className="flex-1 min-h-0 overflow-hidden flex flex-col">
        {step === 0 && (
          <div className="flex-1 overflow-auto">
            <UploadScreen
              key={`upload-${uploadKey}`}
              onUploaded={handleUploaded}
              onValidationError={handleUploadError}
              validateFile={validateFile}
            />

            {/* Post-upload: show file info + reconcile button */}
            {hasData && uploadInfo && (
              <div className="max-w-3xl mx-auto px-6 pb-8 animate-fadeIn">
                <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
                  <div className="grid grid-cols-2 gap-6 mb-6">
                    <FileInfo label="Company A" name={uploadInfo.file_a} rows={uploadInfo.rows_a} cols={uploadInfo.columns_a.length} />
                    <FileInfo label="Company B" name={uploadInfo.file_b} rows={uploadInfo.rows_b} cols={uploadInfo.columns_b.length} />
                  </div>

                  <div className="flex items-center justify-center">
                    <button
                      onClick={handleReconcile}
                      disabled={reconciling || !uploadValid}
                      className="flex items-center gap-2 px-10 py-3 bg-emerald-600 text-white font-bold rounded-lg hover:bg-emerald-700 transition-all duration-200 shadow-md hover:shadow-lg disabled:opacity-40 disabled:cursor-not-allowed text-base hover:scale-[1.02] active:scale-[0.98]"
                    >
                      {reconciling ? (
                        <>
                          <Loader2 size={20} className="animate-spin" />
                          Reconciling...
                        </>
                      ) : (
                        '⚖️ Run Reconciliation'
                      )}
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {step === 1 && <ResultsDashboard key={`dash-${resultKey}`} />}
        {step === 2 && <MatchDetails key={`match-${resultKey}`} />}
        {step === 3 && <Exceptions key={`exc-${resultKey}`} />}
        {step === 4 && <DataPreview key={`preview-${dataKey}`} />}
      </main>
    </div>
  );
}

function FileInfo({ label, name, rows, cols }: { label: string; name: string; rows: number; cols: number }) {
  return (
    <div className="bg-gradient-to-br from-gray-50 to-gray-100/50 rounded-lg p-4 border border-gray-200 transition-all duration-200 hover:shadow-sm">
      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">{label}</p>
      <p className="font-semibold text-gray-800 text-sm truncate">{name}</p>
      <p className="text-xs text-gray-500 mt-1">{rows} rows &middot; {cols} columns</p>
    </div>
  );
}
