import { useState, useRef, useCallback, useEffect } from 'react';
import { Upload, FileSpreadsheet, Loader2, AlertCircle } from 'lucide-react';
import type { UploadResponse } from '../types';
import { uploadFiles } from '../api';

interface UploadScreenProps {
  onUploaded: (data: UploadResponse) => void;
  onValidationError?: (msg: string) => void;
  validateFile?: (file: File) => string | null;
}

export default function UploadScreen({ onUploaded, onValidationError, validateFile }: UploadScreenProps) {
  const [fileA, setFileA] = useState<File | null>(null);
  const [fileB, setFileB] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const refA = useRef<HTMLInputElement>(null);
  const refB = useRef<HTMLInputElement>(null);
  const uploadedRef = useRef<string>('');

  // Validate file before accepting
  const handleFileA = useCallback((f: File) => {
    if (validateFile) {
      const err = validateFile(f);
      if (err) { onValidationError?.(err); return; }
    }
    setError('');
    setFileA(f);
  }, [validateFile, onValidationError]);

  const handleFileB = useCallback((f: File) => {
    if (validateFile) {
      const err = validateFile(f);
      if (err) { onValidationError?.(err); return; }
    }
    setError('');
    setFileB(f);
  }, [validateFile, onValidationError]);

  // Auto-upload when both files are selected
  useEffect(() => {
    if (!fileA || !fileB) return;
    const sig = `${fileA.name}:${fileA.size}:${fileA.lastModified}|${fileB.name}:${fileB.size}:${fileB.lastModified}`;
    if (uploadedRef.current === sig) return;

    const doUpload = async () => {
      setLoading(true);
      setError('');
      try {
        const data = await uploadFiles(fileA, fileB);
        uploadedRef.current = sig;
        onUploaded(data);
      } catch (e: unknown) {
        const err = e as { response?: { data?: { detail?: string } }; message?: string };
        const msg = err?.response?.data?.detail || err?.message || 'Upload failed';
        setError(msg);
        onValidationError?.(msg);
      } finally {
        setLoading(false);
      }
    };
    doUpload();
  }, [fileA, fileB, onUploaded, onValidationError]);

  return (
    <div className="max-w-3xl mx-auto py-8 px-6 animate-fadeIn">
      <div className="text-center mb-8">
        <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-navy-100 mb-3">
          <Upload size={28} className="text-navy-600" />
        </div>
        <h2 className="text-xl font-bold text-navy-800 mb-1">Upload Ledger Files</h2>
        <p className="text-gray-500 text-sm">Drop or select two company ledger files (Excel / CSV)</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-5 mb-4">
        <FileDropZone
          label="Company A"
          sublabel="Primary ledger file"
          file={fileA}
          inputRef={refA}
          onFile={handleFileA}
          accent="blue"
          disabled={loading}
        />
        <FileDropZone
          label="Company B"
          sublabel="Counter-party ledger file"
          file={fileB}
          inputRef={refB}
          onFile={handleFileB}
          accent="indigo"
          disabled={loading}
        />
      </div>

      {loading && (
        <div className="flex items-center justify-center gap-2 py-4 text-navy-600 text-sm animate-pulse">
          <Loader2 size={18} className="animate-spin" /> Uploading & validating files…
        </div>
      )}

      {error && (
        <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 animate-fadeIn">
          <AlertCircle size={16} />
          <span className="text-sm">{error}</span>
        </div>
      )}
    </div>
  );
}

/* ── Drag & Drop File Zone ────────────────────────────────── */
interface FileDropZoneProps {
  label: string;
  sublabel: string;
  file: File | null;
  inputRef: React.RefObject<HTMLInputElement>;
  onFile: (f: File) => void;
  accent: 'blue' | 'indigo';
  disabled?: boolean;
}

function FileDropZone({ label, sublabel, file, inputRef, onFile, accent, disabled }: FileDropZoneProps) {
  const [dragging, setDragging] = useState(false);
  const borderColor = accent === 'blue' ? 'border-blue-300' : 'border-indigo-300';
  const bgDrag = accent === 'blue' ? 'bg-blue-50' : 'bg-indigo-50';

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    if (disabled) return;
    const f = e.dataTransfer.files[0];
    if (f) onFile(f);
  }, [onFile, disabled]);

  return (
    <div
      onDragOver={(e) => { e.preventDefault(); if (!disabled) setDragging(true); }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      onClick={() => !disabled && inputRef.current?.click()}
      className={`relative flex flex-col items-center justify-center p-6 border-2 border-dashed rounded-xl transition-all group
        ${disabled ? 'opacity-50 cursor-wait' : 'cursor-pointer hover:shadow-md'}
        ${dragging ? `${borderColor} ${bgDrag}` : file ? 'border-emerald-300 bg-emerald-50' : 'border-gray-300 hover:border-navy-300 hover:bg-gray-50'}`}
    >
      <input
        ref={inputRef}
        type="file"
        accept=".xls,.xlsx,.csv"
        className="hidden"
        onChange={(e) => e.target.files?.[0] && onFile(e.target.files[0])}
      />

      {file ? (
        <>
          <FileSpreadsheet size={32} className="text-emerald-500 mb-2" />
          <p className="font-semibold text-emerald-700 text-sm">{file.name}</p>
          <p className="text-xs text-emerald-500 mt-1">{(file.size / 1024).toFixed(1)} KB</p>
        </>
      ) : (
        <>
          <Upload size={32} className="text-gray-400 mb-2 group-hover:text-navy-500 transition-colors" />
          <p className="font-semibold text-gray-700 text-sm">{label}</p>
          <p className="text-xs text-gray-400 mt-1">{sublabel}</p>
          <p className="text-xs text-gray-400 mt-2">Drop file here or click to browse</p>
          <p className="text-xs text-gray-300 mt-1">.xls, .xlsx, .csv</p>
        </>
      )}
    </div>
  );
}
