import React, { useEffect, useState } from 'react';
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

type ChartRow = {
  date: string;

  // Bundle
  bundleUnits: number | null;
  bundleForecast: number | null;
  bundleAdjustedForecast: number | null;

  // Antecedent
  antecedentUnits: number | null;
  antecedentForecast: number | null;
  antecedentAfterCannibal: number | null;

  // Consequent
  consequentUnits: number | null;
  consequentForecast: number | null;
  consequentAfterCannibal: number | null;
};

const csvUrl = new URL(
  '../assets/holtwinters_results/holtwinters_results_all.csv',
  import.meta.url
).href;

const toNumOrNull = (value: string | undefined): number | null => {
  if (value === undefined) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const n = parseFloat(trimmed);
  return Number.isNaN(n) ? null : n;
};

export const Dashboard: React.FC = () => {
  const [data, setData] = useState<ChartRow[]>([]);

  useEffect(() => {
    const fetchCsv = async () => {
      try {
        const res = await fetch(csvUrl);
        if (!res.ok) {
          console.error('Failed to fetch CSV:', res.status, res.statusText);
          return;
        }

        const text = await res.text();
        const trimmedText = text.trim();

        if (
          trimmedText.toLowerCase().startsWith('<!doctype html') ||
          trimmedText.toLowerCase().startsWith('<html')
        ) {
          console.error('Got HTML instead of CSV. Check csvUrl:', csvUrl);
          return;
        }

        const lines = trimmedText.split(/\r?\n/);
        const headerLine = lines[0].replace(/^\uFEFF/, '');
        const headers = headerLine.split(',').map((h) => h.trim());

        const rows = lines
          .slice(1)
          .filter((line) => line.trim().length > 0)
          .map((line) => line.split(','))
          .map((cols) => {
            const row: Record<string, string> = {};
            headers.forEach((h, i) => {
              row[h] = cols[i] ?? '';
            });
            return row;
          });

        const filtered = rows.filter(
          (row) => row['bundle_id']?.trim() === 'BF01' && row['bundle_row']?.trim() === '0'
        );

        const parsed: ChartRow[] = filtered.map((row) => ({
          date: row['Date']?.trim() ?? '',

          // Bundle
          bundleUnits: toNumOrNull(row['Bundle_Units']),
          bundleForecast: toNumOrNull(row['Bundle_Units_Forecast']),
          bundleAdjustedForecast: toNumOrNull(row['Bundle_Units_Adjusted_Forecast']),

          // Antecedent
          antecedentUnits: toNumOrNull(row['Antecedent_Units']),
          antecedentForecast: toNumOrNull(row['Antecedent_Units_Forecast']),
          antecedentAfterCannibal: toNumOrNull(
            row['Antecedent_Units_After_Cannibalization']
          ),

          // Consequent
          consequentUnits: toNumOrNull(row['Consequent_Units']),
          consequentForecast: toNumOrNull(row['Consequent_Units_Forecast']),
          consequentAfterCannibal: toNumOrNull(
            row['Consequent_Units_After_Cannibalization']
          ),
        }));

        setData(parsed);
      } catch (err) {
        console.error('Error loading CSV:', err);
      }
    };

    fetchCsv();
  }, []);

  const chartTextColor = '#2d3748'; // dark gray-blue font
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
        marginTop: '3rem',
        color: chartTextColor,
      }}
    >
      <h3 style={{ color: chartTextColor, marginBottom: '1rem', textAlign: 'center' }}>
        {title}
      </h3>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 20, right: 30, left: 10, bottom: 20 }}>
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

  return (
    <div className={styles.dashboard} style={{ color: chartTextColor }}>
      <div className={styles.dashboardHeader}>
        <div>
          <h1>Welcome back, Book Latte!</h1>
          <p>You are now signed in to your analytics dashboard.</p>
        </div>
      </div>

      {data.length === 0 ? (
        <p>Loading Holt-Winters charts...</p>
      ) : (
        <>
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
              dash: '2 4',
            },
          ])}

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
              dash: '2 4',
            },
          ])}

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
              dash: '2 4',
            },
          ])}
        </>
      )}
    </div>
  );
};

export default Dashboard;
