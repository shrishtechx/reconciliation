import { useState, useRef, useCallback, useEffect } from 'react';
import { Upload, FileSpreadsheet, Loader2, AlertCircle, Plus, X, Files } from 'lucide-react';
import type { UploadResponse, FileInfo } from '../types';
import { uploadFiles, addFile } from '../api';

interface UploadScreenProps {
  onUploaded: (data: UploadResponse) => void;
  onValidationError?: (msg: string) => void;
  validateFile?: (file: File) => string | null;
  companyNameA?: string;
  companyNameB?: string;
  onCompanyNameChange?: (company: 'A' | 'B', name: string) => void;
}

export default function UploadScreen({ onUploaded, onValidationError, validateFile, companyNameA = '', companyNameB = '', onCompanyNameChange }: UploadScreenProps) {
  const [filesA, setFilesA] = useState<File[]>([]);
  const [filesB, setFilesB] = useState<File[]>([]);
  const [uploadedFilesA, setUploadedFilesA] = useState<FileInfo[]>([]);
  const [uploadedFilesB, setUploadedFilesB] = useState<FileInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [nameA, setNameA] = useState(companyNameA);
  const [nameB, setNameB] = useState(companyNameB);
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
    setFilesA(prev => [...prev, f]);
  }, [validateFile, onValidationError]);

  const handleFileB = useCallback((f: File) => {
    if (validateFile) {
      const err = validateFile(f);
      if (err) { onValidationError?.(err); return; }
    }
    setError('');
    setFilesB(prev => [...prev, f]);
  }, [validateFile, onValidationError]);

  const removeFileA = useCallback((index: number) => {
    setFilesA(prev => prev.filter((_, i) => i !== index));
  }, []);

  const removeFileB = useCallback((index: number) => {
    setFilesB(prev => prev.filter((_, i) => i !== index));
  }, []);

  // Auto-upload when at least one file from each company is selected
  useEffect(() => {
    if (filesA.length === 0 || filesB.length === 0) return;
    
    const sigA = filesA.map(f => `${f.name}:${f.size}`).join('|');
    const sigB = filesB.map(f => `${f.name}:${f.size}`).join('|');
    const sig = `${sigA}||${sigB}`;
    if (uploadedRef.current === sig) return;

    const doUpload = async () => {
      setLoading(true);
      setError('');
      try {
        // Upload first file from each company
        const data = await uploadFiles(filesA[0], filesB[0]);
        
        // Add additional files if any
        for (let i = 1; i < filesA.length; i++) {
          await addFile('A', filesA[i]);
        }
        for (let i = 1; i < filesB.length; i++) {
          await addFile('B', filesB[i]);
        }
        
        uploadedRef.current = sig;
        
        // Update uploaded files info
        setUploadedFilesA(data.files_a || [{ name: data.file_a, rows: data.rows_a }]);
        setUploadedFilesB(data.files_b || [{ name: data.file_b, rows: data.rows_b }]);
        
        onUploaded({
          ...data,
          rows_a: filesA.reduce((sum, _, i) => sum + (data.files_a?.[i]?.rows || data.rows_a), 0),
          rows_b: filesB.reduce((sum, _, i) => sum + (data.files_b?.[i]?.rows || data.rows_b), 0),
          company_name_a: nameA || 'Company A',
          company_name_b: nameB || 'Company B',
        });
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
  }, [filesA, filesB, onUploaded, onValidationError]);

  return (
    <div className="max-w-4xl mx-auto py-8 px-6 animate-fadeIn">
      <div className="text-center mb-8">
        <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-navy-100 mb-3">
          <Upload size={28} className="text-navy-600" />
        </div>
        <h2 className="text-xl font-bold text-navy-800 mb-1">Upload Ledger Files</h2>
        <p className="text-gray-500 text-sm">Drop or select company ledger files (Excel, CSV, PDF, JPG, PNG supported)</p>
        <p className="text-gray-400 text-xs mt-1">You can upload multiple ledgers per company using the + button</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
        {/* Company A Section */}
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h3 className="font-semibold text-navy-700 flex items-center gap-2">
              <Files size={16} /> Ledger 1
            </h3>
            {filesA.length > 0 && (
              <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full">
                {filesA.length} file{filesA.length > 1 ? 's' : ''}
              </span>
            )}
          </div>
          
          {/* Company Name Input */}
          <input
            type="text"
            placeholder="Enter company/ledger name"
            value={nameA}
            onChange={(e) => {
              setNameA(e.target.value);
              onCompanyNameChange?.('A', e.target.value);
            }}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          
          <FileDropZone
            label={nameA || "Ledger 1"}
            sublabel="Primary ledger (Debit-side / Payables)"
            files={filesA}
            inputRef={refA}
            onFile={handleFileA}
            accent="blue"
            disabled={loading}
          />
          
          {/* File list with remove option */}
          {filesA.length > 0 && (
            <div className="space-y-2">
              {filesA.map((f, i) => (
                <div key={i} className="flex items-center justify-between bg-blue-50 border border-blue-200 rounded-lg px-3 py-2">
                  <div className="flex items-center gap-2">
                    <FileSpreadsheet size={16} className="text-blue-500" />
                    <span className="text-sm text-blue-700 truncate max-w-[180px]">{f.name}</span>
                    <span className="text-xs text-blue-400">({(f.size / 1024).toFixed(1)} KB)</span>
                  </div>
                  <button 
                    onClick={() => removeFileA(i)} 
                    className="text-blue-400 hover:text-red-500 transition-colors"
                    disabled={loading}
                  >
                    <X size={16} />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Company B Section */}
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h3 className="font-semibold text-navy-700 flex items-center gap-2">
              <Files size={16} /> Ledger 2
            </h3>
            {filesB.length > 0 && (
              <span className="text-xs bg-indigo-100 text-indigo-700 px-2 py-0.5 rounded-full">
                {filesB.length} file{filesB.length > 1 ? 's' : ''}
              </span>
            )}
          </div>
          
          {/* Company Name Input */}
          <input
            type="text"
            placeholder="Enter company/ledger name"
            value={nameB}
            onChange={(e) => {
              setNameB(e.target.value);
              onCompanyNameChange?.('B', e.target.value);
            }}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
          />
          
          <FileDropZone
            label={nameB || "Ledger 2"}
            sublabel="Counter-party ledger (Credit-side / Receivables)"
            files={filesB}
            inputRef={refB}
            onFile={handleFileB}
            accent="indigo"
            disabled={loading}
          />
          
          {/* File list with remove option */}
          {filesB.length > 0 && (
            <div className="space-y-2">
              {filesB.map((f, i) => (
                <div key={i} className="flex items-center justify-between bg-indigo-50 border border-indigo-200 rounded-lg px-3 py-2">
                  <div className="flex items-center gap-2">
                    <FileSpreadsheet size={16} className="text-indigo-500" />
                    <span className="text-sm text-indigo-700 truncate max-w-[180px]">{f.name}</span>
                    <span className="text-xs text-indigo-400">({(f.size / 1024).toFixed(1)} KB)</span>
                  </div>
                  <button 
                    onClick={() => removeFileB(i)} 
                    className="text-indigo-400 hover:text-red-500 transition-colors"
                    disabled={loading}
                  >
                    <X size={16} />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
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
  files: File[];
  inputRef: React.RefObject<HTMLInputElement>;
  onFile: (f: File) => void;
  accent: 'blue' | 'indigo';
  disabled?: boolean;
}

function FileDropZone({ label, sublabel, files, inputRef, onFile, accent, disabled }: FileDropZoneProps) {
  const [dragging, setDragging] = useState(false);
  const borderColor = accent === 'blue' ? 'border-blue-300' : 'border-indigo-300';
  const bgDrag = accent === 'blue' ? 'bg-blue-50' : 'bg-indigo-50';
  const hasFiles = files.length > 0;

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    if (disabled) return;
    const droppedFiles = Array.from(e.dataTransfer.files);
    droppedFiles.forEach(f => onFile(f));
  }, [onFile, disabled]);

  // Compact drop zone when files already added - still allows adding more
  if (hasFiles) {
    return (
      <div
        onDragOver={(e) => { e.preventDefault(); if (!disabled) setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={handleDrop}
        onClick={() => !disabled && inputRef.current?.click()}
        className={`relative flex items-center justify-center gap-2 p-3 border-2 border-dashed rounded-lg transition-all
          ${disabled ? 'opacity-50 cursor-wait' : 'cursor-pointer hover:shadow-sm'}
          ${dragging ? `${borderColor} ${bgDrag}` : accent === 'blue' ? 'border-blue-200 hover:border-blue-300 hover:bg-blue-50/50' : 'border-indigo-200 hover:border-indigo-300 hover:bg-indigo-50/50'}`}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".xls,.xlsx,.csv,.pdf,.jpg,.jpeg,.png,.bmp,.tiff,.tif,.webp"
          className="hidden"
          multiple
          onChange={(e) => {
            if (e.target.files) {
              Array.from(e.target.files).forEach(f => onFile(f));
            }
          }}
        />
        <Plus size={16} className={accent === 'blue' ? 'text-blue-400' : 'text-indigo-400'} />
        <span className={`text-sm ${accent === 'blue' ? 'text-blue-500' : 'text-indigo-500'}`}>
          Drop or click to add more files
        </span>
      </div>
    );
  }

  return (
    <div
      onDragOver={(e) => { e.preventDefault(); if (!disabled) setDragging(true); }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      onClick={() => !disabled && inputRef.current?.click()}
      className={`relative flex flex-col items-center justify-center p-8 border-2 border-dashed rounded-xl transition-all group
        ${disabled ? 'opacity-50 cursor-wait' : 'cursor-pointer hover:shadow-md'}
        ${dragging ? `${borderColor} ${bgDrag}` : 'border-gray-300 hover:border-navy-300 hover:bg-gray-50'}`}
    >
      <input
        ref={inputRef}
        type="file"
        accept=".xls,.xlsx,.csv,.pdf,.jpg,.jpeg,.png,.bmp,.tiff,.tif,.webp"
        className="hidden"
        multiple
        onChange={(e) => {
          if (e.target.files) {
            Array.from(e.target.files).forEach(f => onFile(f));
          }
        }}
      />

      <Upload size={36} className="text-gray-400 mb-3 group-hover:text-navy-500 transition-colors" />
      <p className="font-semibold text-gray-700">{label}</p>
      <p className="text-xs text-gray-400 mt-1">{sublabel}</p>
      <p className="text-xs text-gray-400 mt-3">Drop files here or click to browse</p>
      <p className="text-xs text-gray-300 mt-1">.xlsx, .xls, .csv, .pdf, .jpg, .jpeg, .png (multiple files supported)</p>
    </div>
  );
}
