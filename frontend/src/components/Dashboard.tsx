import React, { useEffect, useMemo, useState } from 'react';
import styles from './Dashboard.module.css';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import BundleSelector, { type BundleOption } from './BundleSelector';

type ChartRow = {
  date: string;

  bundleUnits: number | null;
  bundleForecast: number | null;
  bundleAdjustedForecast: number | null;

  antecedentUnits: number | null;
  antecedentForecast: number | null;
  antecedentAfterCannibal: number | null;

  consequentUnits: number | null;
  consequentForecast: number | null;
  consequentAfterCannibal: number | null;

  bundleId: string;
  bundleRow: string;
  category: string;
};

// CSV is at: frontend/src/assets/holtwinters_results/holtwinters_results_all.csv
const csvUrl = new URL(
  '../assets/holtwinters_results/holtwinters_results_all.csv',
  import.meta.url
).href;

const toNumOrNull = (value: string | undefined): number | null => {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const n = parseFloat(trimmed);
  return Number.isNaN(n) ? null : n;
};

export const Dashboard: React.FC = () => {
  const [allRows, setAllRows] = useState<ChartRow[]>([]);
  const [activeBundleKey, setActiveBundleKey] = useState<string | null>(null);
  const [pendingBundleKey, setPendingBundleKey] = useState<string | null>(null);

  useEffect(() => {
    const fetchCsv = async () => {
      try {
        const res = await fetch(csvUrl);
        if (!res.ok) throw new Error('Failed to fetch CSV');

        const text = await res.text();
        const trimmedText = text.trim();

        // Guard: make sure we didn’t get HTML
        if (
          trimmedText.toLowerCase().startsWith('<!doctype html') ||
          trimmedText.toLowerCase().startsWith('<html')
        ) {
          console.error('Got HTML instead of CSV. Check csvUrl:', csvUrl);
          return;
        }

        const lines = trimmedText.split(/\r?\n/);
        if (lines.length < 2) return;

        const headerLine = lines[0].replace(/^\uFEFF/, '');
        const headers = headerLine.split(',').map((h) => h.trim());

        const rows = lines
          .slice(1)
          .filter((line) => line.trim())
          .map((line) => line.split(','))
          .map((cols) => {
            const row: Record<string, string> = {};
            headers.forEach((h, i) => {
              row[h] = cols[i] ?? '';
            });
            return row;
          });

        const parsed: ChartRow[] = rows.map((r) => ({
          date: r['Date']?.trim() ?? '',

          bundleUnits: toNumOrNull(r['Bundle_Units']),
          bundleForecast: toNumOrNull(r['Bundle_Units_Forecast']),
          bundleAdjustedForecast: toNumOrNull(r['Bundle_Units_Adjusted_Forecast']),

          antecedentUnits: toNumOrNull(r['Antecedent_Units']),
          antecedentForecast: toNumOrNull(r['Antecedent_Units_Forecast']),
          antecedentAfterCannibal: toNumOrNull(
            r['Antecedent_Units_After_Cannibalization']
          ),

          consequentUnits: toNumOrNull(r['Consequent_Units']),
          consequentForecast: toNumOrNull(r['Consequent_Units_Forecast']),
          consequentAfterCannibal: toNumOrNull(
            r['Consequent_Units_After_Cannibalization']
          ),

          bundleId: r['bundle_id']?.trim() ?? '',
          bundleRow: r['bundle_row']?.trim() ?? '',
          category: r['category']?.trim() ?? '',
        }));

        setAllRows(parsed);

        // Initialize default bundle selection (first non-empty)
        const firstWithBundle = parsed.find((r) => r.bundleId && r.bundleRow);
        if (firstWithBundle) {
          const key = `${firstWithBundle.bundleRow}|${firstWithBundle.bundleId}`;
          setActiveBundleKey(key);
          setPendingBundleKey(key);
        }
      } catch (err) {
        console.error('Error loading CSV:', err);
      }
    };

    fetchCsv();
  }, []);

  // Build dropdown options from all unique bundleRow + bundleId
  const bundleOptions: BundleOption[] = useMemo(() => {
    const map = new Map<string, BundleOption>();
    for (const row of allRows) {
      if (!row.bundleId || !row.bundleRow) continue;
      const key = `${row.bundleRow}|${row.bundleId}`;
      if (!map.has(key)) {
        const label = `${row.bundleId} (row ${row.bundleRow}, ${row.category || 'N/A'})`;
        map.set(key, { key, label });
      }
    }
    return Array.from(map.values());
  }, [allRows]);

  // Data for the currently active bundle (used by charts)
  const chartData = useMemo(() => {
    if (!activeBundleKey) return [];
    const [row, id] = activeBundleKey.split('|');
    return allRows.filter(
      (r) => r.bundleId === id && r.bundleRow === row
    );
  }, [allRows, activeBundleKey]);

  const chartTextColor = '#2d3748';
  const forecastBlue = '#3182ce';
  const adjustedGreen = '#38a169';

  const renderChart = (
    title: string,
    lines: { key: keyof ChartRow; name: string; color: string; dash?: string }[]
  ) => (
    <div
      style={{
        width: '100%',
        height: 360,
        marginTop: '3.5rem',
        color: chartTextColor,
      }}
    >
      <h3 style={{ color: chartTextColor, marginBottom: '1rem', textAlign: 'center' }}>
        {title}
      </h3>
      <ResponsiveContainer>
        <LineChart data={chartData} margin={{ top: 20, right: 30, left: 10, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" stroke={chartTextColor} />
          <YAxis stroke={chartTextColor} />
          <Tooltip />
          <Legend />
          {lines.map((line, idx) => (
            <Line
              key={idx}
              type="monotone"
              dataKey={line.key}
              name={line.name}
              stroke={line.color}
              strokeWidth={2}
              strokeDasharray={line.dash}
              dot={{ r: 3 }}
              connectNulls
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );

  const isLoading = allRows.length === 0 || !activeBundleKey || !pendingBundleKey;

  return (
    <div className={styles.dashboard} style={{ color: chartTextColor }}>
      <div className={styles.dashboardHeader}>
        <div>
          <h1>Welcome back, Book Latte!</h1>
          <p>You are now signed in to your analytics dashboard.</p>
        </div>
      </div>

      {/* Bundle Selector */}
      {bundleOptions.length > 0 && (
        <BundleSelector
          options={bundleOptions}
          pendingKey={pendingBundleKey}
          onPendingKeyChange={(key) => setPendingBundleKey(key)}
          onConfirm={() => {
            if (pendingBundleKey) {
              setActiveBundleKey(pendingBundleKey);
            }
          }}
        />
      )}

      {isLoading ? (
        <p>Loading Holt-Winters charts...</p>
      ) : (
        <>
          {/* 1. Bundle */}
          {renderChart('Bundle Units – Actual vs Forecast', [
            { key: 'bundleUnits', name: 'Bundle Units (Actual)', color: '#000000' },
            {
              key: 'bundleForecast',
              name: 'Bundle Units (Forecast)',
              color: forecastBlue,
              dash: '5 5',
            },
            {
              key: 'bundleAdjustedForecast',
              name: 'Bundle Units (Adjusted Forecast)',
              color: adjustedGreen,
              dash: '5 5',
            },
          ])}

          {/* 2. Antecedent */}
          {renderChart('Antecedent Units – Actual vs Forecast', [
            { key: 'antecedentUnits', name: 'Antecedent Units (Actual)', color: '#000000' },
            {
              key: 'antecedentForecast',
              name: 'Antecedent Units (Forecast)',
              color: forecastBlue,
              dash: '5 5',
            },
            {
              key: 'antecedentAfterCannibal',
              name: 'Antecedent Units (After Cannibalization)',

              color: adjustedGreen,
              dash: '5 5',
            },
          ])}

          {/* 3. Consequent */}
          {renderChart('Consequent Units – Actual vs Forecast', [
            { key: 'consequentUnits', name: 'Consequent Units (Actual)', color: '#000000' },
            {
              key: 'consequentForecast',
              name: 'Consequent Units (Forecast)',
              color: forecastBlue,
              dash: '5 5',
            },
            {
              key: 'consequentAfterCannibal',
              name: 'Consequent Units (After Cannibalization)',
              color: adjustedGreen,
              dash: '5 5',
            },
          ])}
        </>
      )}
    </div>
  );
};

export default Dashboard;
