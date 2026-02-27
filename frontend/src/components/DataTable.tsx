import { useState, useMemo, useRef, useCallback, useEffect } from 'react';
import { ChevronUp, ChevronDown, Search } from 'lucide-react';

interface DataTableProps {
  data: Record<string, unknown>[];
  columns?: string[];
  batchSize?: number;
  keyPrefix?: string;
}

const BATCH = 50;

export default function DataTable({ data, columns, batchSize = BATCH, keyPrefix = 'tbl' }: DataTableProps) {
  const cols = useMemo(() => columns ?? (data.length > 0 ? Object.keys(data[0]) : []), [data, columns]);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);
  const [visibleCount, setVisibleCount] = useState(batchSize);
  const sentinelRef = useRef<HTMLDivElement>(null);

  const filtered = useMemo(() => {
    let rows = [...data];
    for (const [col, val] of Object.entries(filters)) {
      if (!val) continue;
      rows = rows.filter((r) => {
        const cell = String(r[col] ?? '').toLowerCase();
        return cell.includes(val.toLowerCase());
      });
    }
    if (sortCol) {
      rows.sort((a, b) => {
        const va = a[sortCol] ?? '';
        const vb = b[sortCol] ?? '';
        if (typeof va === 'number' && typeof vb === 'number') return sortAsc ? va - vb : vb - va;
        return sortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
      });
    }
    return rows;
  }, [data, filters, sortCol, sortAsc]);

  useEffect(() => { setVisibleCount(batchSize); }, [filtered, batchSize]);

  const loadMore = useCallback(() => {
    setVisibleCount((prev) => Math.min(prev + batchSize, filtered.length));
  }, [batchSize, filtered.length]);

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => { if (entries[0].isIntersecting) loadMore(); },
      { rootMargin: '200px' }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [loadMore]);

  const visible = filtered.slice(0, visibleCount);

  const handleSort = (col: string) => {
    if (sortCol === col) setSortAsc(!sortAsc);
    else { setSortCol(col); setSortAsc(true); }
  };

  const totalCols = cols.length + 1; // +1 for serial number

  return (
    <div className="w-full h-full flex flex-col min-h-0 animate-fadeIn">
      <div className="text-xs text-gray-500 mb-1 shrink-0">
        Showing {visible.length} of {filtered.length} records
        {filtered.length < data.length && <span className="ml-1">(filtered from {data.length})</span>}
      </div>

      <div className="flex-1 min-h-0 overflow-auto rounded-lg border border-gray-300 shadow-sm scrollbar-thin">
        <table className="min-w-full text-sm border-collapse">
          <thead className="sticky top-0 z-20">
            {/* Header row */}
            <tr>
              <th className="px-2 py-2 text-center font-semibold whitespace-nowrap select-none bg-navy-800 text-white border border-navy-600 w-12">
                #
              </th>
              {cols.map((col) => (
                <th
                  key={`${keyPrefix}-h-${col}`}
                  className="px-3 py-2 text-left font-semibold whitespace-nowrap cursor-pointer select-none hover:bg-navy-700 transition-colors bg-navy-800 text-white border border-navy-600"
                  onClick={() => handleSort(col)}
                >
                  <div className="flex items-center gap-1">
                    <span>{col.replace(/_/g, ' ')}</span>
                    {sortCol === col && (sortAsc ? <ChevronUp size={14} /> : <ChevronDown size={14} />)}
                  </div>
                </th>
              ))}
            </tr>
            {/* Filter row */}
            <tr>
              <th className="px-1 py-1 bg-gray-100 border border-gray-200 w-12" />
              {cols.map((col) => (
                <th key={`${keyPrefix}-f-${col}`} className="px-2 py-1 bg-gray-100 border border-gray-200">
                  <div className="relative">
                    <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400" />
                    <input
                      type="text"
                      placeholder="Filter..."
                      value={filters[col] ?? ''}
                      onChange={(e) => setFilters({ ...filters, [col]: e.target.value })}
                      className="w-full pl-7 pr-2 py-1 text-xs border border-gray-200 rounded bg-white focus:outline-none focus:ring-1 focus:ring-navy-500 focus:border-navy-500"
                    />
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 ? (
              <tr>
                <td colSpan={totalCols} className="px-4 py-8 text-center text-gray-400 border border-gray-200">
                  No data to display
                </td>
              </tr>
            ) : (
              visible.map((row, ri) => (
                <tr
                  key={`${keyPrefix}-r-${ri}`}
                  className={`hover:bg-blue-50/80 transition-colors ${ri % 2 === 0 ? 'bg-white' : 'bg-gray-50'}`}
                >
                  <td className="px-2 py-1.5 text-center text-xs text-gray-400 font-medium border border-gray-200 bg-gray-50/80 w-12">
                    {ri + 1}
                  </td>
                  {cols.map((col) => (
                    <td key={`${keyPrefix}-${ri}-${col}`} className="px-3 py-1.5 whitespace-nowrap text-gray-700 border border-gray-200">
                      {formatCell(row[col])}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>

        {visibleCount < filtered.length && (
          <div ref={sentinelRef} className="flex items-center justify-center py-3 text-xs text-gray-400">
            Loading more rows…
          </div>
        )}
      </div>
    </div>
  );
}

function formatCell(val: unknown): string {
  if (val === null || val === undefined || val === '') return '—';
  if (typeof val === 'number') {
    if (Number.isInteger(val) && Math.abs(val) < 1e6) return val.toLocaleString();
    return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  return String(val);
}
