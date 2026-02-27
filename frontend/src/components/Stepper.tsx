const STEPS = ['Upload', 'OverView', 'Matches', 'Exceptions', 'Preview'];

interface StepperProps {
  active: number;
  onStep: (step: number) => void;
  hasData: boolean;
  hasResults: boolean;
}

export default function Stepper({ active, onStep, hasData, hasResults }: StepperProps) {
  const canGo = (idx: number) => {
    if (idx === 0) return true;           // Upload — always
    if (idx === 4) return hasData;        // Preview — needs data
    return hasResults;                    // OverView, Matches, Exceptions — need results
  };

  return (
    <div className="flex items-center gap-1">
      {STEPS.map((label, idx) => {
        const isActive = idx === active;
        const allowed = canGo(idx);

        return (
          <div key={idx} className="flex items-center">
            <button
              onClick={() => allowed && onStep(idx)}
              disabled={!allowed}
              className={`relative px-5 py-2 rounded-lg text-xs font-semibold transition-all duration-300 ease-out
                ${isActive
                  ? 'bg-white text-navy-800 shadow-lg shadow-white/20'
                  : allowed
                    ? 'text-navy-200 hover:bg-white/15 hover:text-white'
                    : 'text-navy-500/40 cursor-not-allowed'
                }`}
            >
              {label}
              {isActive && (
                <span className="absolute bottom-0 left-1/2 -translate-x-1/2 w-8 h-[2px] bg-white rounded-full" />
              )}
            </button>
            {idx < STEPS.length - 1 && (
              <div className="w-4 h-[1px] mx-0.5 bg-white/10" />
            )}
          </div>
        );
      })}
    </div>
  );
}
