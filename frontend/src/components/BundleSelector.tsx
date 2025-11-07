import React from 'react';

export interface BundleOption {
  key: string;       // e.g. "0|BF01"
  label: string;     // e.g. "BF01 (row 0, FOOD)"
}

interface BundleSelectorProps {
  options: BundleOption[];
  pendingKey: string | null;
  onPendingKeyChange: (key: string) => void;
  onConfirm: () => void;
}

const BundleSelector: React.FC<BundleSelectorProps> = ({
  options,
  pendingKey,
  onPendingKeyChange,
  onConfirm,
}) => {
  const textColor = '#2d3748';

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '0.75rem',
        marginTop: '1.5rem',
        marginBottom: '1.5rem',
        color: textColor,
      }}
    >
      <label style={{ fontWeight: 600 }}>
        Select Bundle:
        <select
          value={pendingKey ?? ''}
          onChange={(e) => onPendingKeyChange(e.target.value)}
          style={{
            marginLeft: '0.5rem',
            padding: '0.35rem 0.6rem',
            borderRadius: 4,
            border: '1px solid #cbd5e0',
            color: textColor,
            backgroundColor: '#ffffff',
          }}
        >
          {options.map((opt) => (
            <option key={opt.key} value={opt.key}>
              {opt.label}
            </option>
          ))}
        </select>
      </label>

      <button
        type="button"
        onClick={onConfirm}
        style={{
          padding: '0.4rem 0.9rem',
          borderRadius: 4,
          border: 'none',
          backgroundColor: '#3182ce',
          color: '#ffffff',
          fontWeight: 600,
          cursor: 'pointer',
        }}
      >
        Confirm
      </button>
    </div>
  );
};

export default BundleSelector;