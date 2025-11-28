import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX

# =========================
# USER CONFIGURATION
# =========================
PARENT_DIR        = 'mba_output'
RULES_PATH        = PARENT_DIR + '/association_rules.csv'
FACT_PATH         = 'etl_dimensions/fact_transaction_dimension.csv'
PRODUCT_PATH      = 'etl_dimensions/current_product_dimension.csv'
PED_SUMMARY_PATH  = PARENT_DIR + '/ped_output/ped_summary.csv'
NLP_OPT_PATH      = PARENT_DIR + '/nlp_output/nlp_optimization_results.csv'
OUTPUT_DIR        = 'sarima_results'
BUNDLE_ROW = 0

AGG_FREQ = 'QE'            # Quarterly
SEASONAL_PERIODS = 4       # Quarterly seasonality
HORIZON = 4                # Forecast steps

# SARIMA order (tune as needed)
SARIMA_ORDER = (1, 1, 1)
SARIMA_SEASONAL_ORDER = (1, 1, 1, SEASONAL_PERIODS)

# Price scenario (fallback)
DISCOUNT_RATE = 0.05

# =========================
# HELPERS
# =========================


def load_bundle_from_rules(rules_path: str, bundle_row: int) -> tuple[str, str]:
    rules_df = pd.read_csv(rules_path)
    need = {'antecedents_names', 'consequents_names'}
    if not need.issubset(rules_df.columns):
        print("Error: Required columns not found in association_rules.csv")
        sys.exit(1)
    if not (0 <= bundle_row < len(rules_df)):
        print(
            f"Error: BUNDLE_ROW {bundle_row} out of range (0..{len(rules_df)-1}).")
        sys.exit(1)
    row = rules_df.iloc[bundle_row]
    return str(row['antecedents_names']), str(row['consequents_names'])


def resolve_ids(product_df: pd.DataFrame, name_a: str, name_b: str) -> tuple[str, str]:
    a = product_df.loc[product_df['product_name'] == name_a]
    b = product_df.loc[product_df['product_name'] == name_b]
    if a.empty or b.empty:
        print(f"Could not find product IDs for '{name_a}' or '{name_b}'.")
        sys.exit(1)
    return str(a['product_id'].iloc[0]), str(b['product_id'].iloc[0])


def pick_ped_row(ped_df: pd.DataFrame, id_a: str, id_b: str) -> pd.Series | None:
    a, b = str(id_a), str(id_b)
    m1 = (ped_df['product_id_1'].astype(str) == a) & (
        ped_df['product_id_2'].astype(str) == b)
    m2 = (ped_df['product_id_1'].astype(str) == b) & (
        ped_df['product_id_2'].astype(str) == a)
    matches = ped_df[m1 | m2].copy()
    if matches.empty:
        return None
    non_strict = matches[matches['mode'].astype(
        str).str.lower() == 'non_strict']
    if not non_strict.empty:
        return non_strict.iloc[0]
    return matches.iloc[0]


def fit_and_forecast_sarima(series: pd.Series, label: str, idx: pd.DatetimeIndex) -> pd.Series:
    if series is None or series.empty:
        print(
            f"\nNo sales found for {label}. Returning zeros on common index.")
        return pd.Series(0.0, index=idx)
    model = SARIMAX(series, order=SARIMA_ORDER, seasonal_order=SARIMA_SEASONAL_ORDER,
                    enforce_stationarity=False, enforce_invertibility=False)
    fitted = model.fit(disp=False)
    fc = fitted.forecast(len(idx))
    fc.index = idx
    return fc


def detect_trend(series: pd.Series, label: str) -> str:
    if series is None or len(series) < 2:
        return f"{label}: No data"
    first_val, last_val = series.iloc[0], series.iloc[-1]
    if last_val > first_val * 1.05:
        return f"{label}: Upward trend"
    elif last_val < first_val * 0.95:
        return f"{label}: Downward trend"
    else:
        return f"{label}: Relatively flat trend"


# =========================
# LOAD META (RULES / PRODUCTS / PED FILE)
# =========================
try:
    product_a_name, product_b_name = load_bundle_from_rules(
        RULES_PATH, BUNDLE_ROW)
    print(f"Analyzing bundle: '{product_a_name}' and '{product_b_name}'")
except Exception as e:
    print(f"Error reading association rules: {e}")
    sys.exit(1)

try:
    fact_df = pd.read_csv(FACT_PATH)
    product_df = pd.read_csv(PRODUCT_PATH)
except FileNotFoundError as e:
    print(f"Error loading data: {e}")
    sys.exit(1)

product_a_id, product_b_id = resolve_ids(
    product_df, product_a_name, product_b_name)

try:
    ped_df = pd.read_csv(PED_SUMMARY_PATH)
except FileNotFoundError:
    print(
        f"Error: PED summary not found at '{PED_SUMMARY_PATH}'. Run the standalone PED script first.")
    sys.exit(1)

needed_cols = {'product_id_1', 'product_id_2',
               'elasticity_epsilon', 'intercept_logk', 'mode', 'n_price_points'}
if not needed_cols.issubset(ped_df.columns):
    print(
        f"Error: '{PED_SUMMARY_PATH}' missing required columns: {sorted(list(needed_cols - set(ped_df.columns)))}")
    sys.exit(1)

ped_row = pick_ped_row(ped_df, product_a_id, product_b_id)
if ped_row is None:
    print("Error: No matching row in ped_summary.csv for the selected bundle.")
    sys.exit(1)

epsilon = float(ped_row['elasticity_epsilon']) if pd.notna(
    ped_row['elasticity_epsilon']) else 0.0
intercept = float(ped_row['intercept_logk']) if pd.notna(
    ped_row['intercept_logk']) else 0.0
n_points = int(ped_row['n_price_points']) if pd.notna(
    ped_row['n_price_points']) else 0
mode_used = str(ped_row.get('mode', ''))
print(
    f"\nLoaded PED from file [{mode_used}] — points={n_points}, ε={epsilon:.6f}, intercept={intercept:.6f}")
if n_points < 2:
    print("Warning: PED file shows fewer than 2 price points; elasticity may be unreliable.")

# =========================
# BUILD HISTORICAL SERIES
# =========================
pair_transactions = fact_df[fact_df['Product ID'].astype(
    str).isin([product_a_id, product_b_id])]
receipt_products = pair_transactions.groupby(
    'Receipt No')['Product ID'].apply(lambda s: set(s.astype(str)))
receipts_with_both = receipt_products[
    receipt_products.apply(lambda s: (product_a_id in s)
                           and (product_b_id in s))
].index

working_df = fact_df[fact_df['Receipt No'].isin(receipts_with_both)]
receipt_summary = working_df.groupby('Receipt No').agg(
    Combined_Price=('Line Total', 'sum'),
    Date=('Date', 'first')
)

receipt_summary['Date'] = pd.to_datetime(
    receipt_summary['Date'], errors='coerce')
bundle_sales_ts = receipt_summary.groupby(
    pd.Grouper(key='Date', freq=AGG_FREQ)).size()


def build_ts_all(lines: pd.DataFrame) -> pd.Series:
    if lines.empty:
        return pd.Series(dtype=float)
    rec = lines.groupby('Receipt No').agg(Date=('Date', 'first'))
    rec['Date'] = pd.to_datetime(rec['Date'], errors='coerce')
    return rec.groupby(pd.Grouper(key='Date', freq=AGG_FREQ)).size()


a_lines_all = fact_df[fact_df['Product ID'].astype(str) == str(product_a_id)]
b_lines_all = fact_df[fact_df['Product ID'].astype(str) == str(product_b_id)]
a_ts_all = build_ts_all(a_lines_all)
b_ts_all = build_ts_all(b_lines_all)

if bundle_sales_ts.empty and (a_ts_all.empty and b_ts_all.empty):
    print("No sales found for forecasting.")
    sys.exit(0)

# =========================
# PRICE SCENARIO (USING ε FROM FILE)
# =========================
current_price_a = float(product_df.loc[product_df['product_id'].astype(
    str) == str(product_a_id), 'Price'].iloc[0])
current_price_b = float(product_df.loc[product_df['product_id'].astype(
    str) == str(product_b_id), 'Price'].iloc[0])
current_price = current_price_a + current_price_b
new_price = float(NEW_PRICE)

print(f"\n--- PRICE setting FORECAST (using ε from file) ---")
print(f"Current price: Php {current_price:.2f}")
print(f"New promotional price: Php {new_price:.2f}")
print(
    f"Price change: {((new_price - current_price) / current_price * 100):.1f}%")

if current_price <= 0 or new_price <= 0:
    print("Warning: Non-positive price encountered; cannot apply power formula. Falling back to no change.")
    demand_multiplier = 1.0
else:
    price_ratio = new_price / current_price
    demand_multiplier = price_ratio ** epsilon

expected_demand_change_pct = (demand_multiplier - 1.0) * 100.0
print(
    f"Expected demand change in bundle units (power model): {expected_demand_change_pct:.1f}%")

# =========================
# COMMON FORECAST INDEX
# =========================
if AGG_FREQ.upper().startswith('Q'):
    step = pd.offsets.QuarterEnd()
elif AGG_FREQ.upper().startswith('M'):
    step = pd.offsets.MonthEnd()
else:
    step = pd.tseries.frequencies.to_offset(AGG_FREQ)


def last_index_or_min(ts: pd.Series):
    return ts.index[-1] if (ts is not None and not ts.empty) else pd.Timestamp.min


latest_actual = max([last_index_or_min(bundle_sales_ts),
                     last_index_or_min(a_ts_all),
                     last_index_or_min(b_ts_all)])

COMMON_FC_INDEX = pd.date_range(
    start=latest_actual + step, periods=HORIZON, freq=AGG_FREQ)

# =========================
# FORECASTS (SARIMA) + ADJUST WITH ε
# =========================
bundle_fc_raw = fit_and_forecast_sarima(
    bundle_sales_ts, "Bundle (baseline)", COMMON_FC_INDEX)
a_fc_all_raw = fit_and_forecast_sarima(
    a_ts_all, f"{product_a_name} (ALL)", COMMON_FC_INDEX)
b_fc_all_raw = fit_and_forecast_sarima(
    b_ts_all, f"{product_b_name} (ALL)", COMMON_FC_INDEX)

bundle_fc = bundle_fc_raw.clip(lower=0)
bundle_fc_adj = (bundle_fc_raw * demand_multiplier).clip(lower=0)
a_fc_all = a_fc_all_raw.clip(lower=0)
b_fc_all = b_fc_all_raw.clip(lower=0)

# =========================
# CANNIBALIZATION (simple subtraction)
# =========================
cannibalization_units = bundle_fc
a_fc_after_aligned = (a_fc_all - cannibalization_units).clip(lower=0)
b_fc_after_aligned = (b_fc_all - cannibalization_units).clip(lower=0)

# =========================
# REVENUE IMPACT
# =========================


def revenue_forecast(series: pd.Series, price: float) -> pd.Series:
    return series * price


price_a = current_price_a
price_b = current_price_b

rev_a_before = revenue_forecast(a_fc_all, price_a)
rev_b_before = revenue_forecast(b_fc_all, price_b)

rev_a_after = revenue_forecast(a_fc_after_aligned, price_a)
rev_b_after = revenue_forecast(b_fc_after_aligned, price_b)

rev_bundle_before = pd.Series(0.0, index=COMMON_FC_INDEX)
rev_bundle_after = bundle_fc_adj * new_price

indiv_rev_before = {"A": float(rev_a_before.sum()),
                    "B": float(rev_b_before.sum())}
indiv_rev_after = {"A": float(rev_a_after.sum()),
                   "B": float(rev_b_after.sum())}

overall_before = indiv_rev_before["A"] + indiv_rev_before["B"]
overall_after = indiv_rev_after["A"] + \
    indiv_rev_after["B"] + float(rev_bundle_after.sum())

print("\n--- DETAILED REVENUE IMPACT ANALYSIS (Common index, power elasticity, non-negative forecasts) ---")
bundle_before_total = float(rev_bundle_before.sum())   # 0.0
bundle_after_total = float(rev_bundle_after.sum())

print("Individual Revenue Forecast BEFORE Bundling/Cannibalization:")
print(f"  {product_a_name}: Php {indiv_rev_before['A']:.2f}")
print(f"  {product_b_name}: Php {indiv_rev_before['B']:.2f}")
print(
    f"  BUNDLE (status-quo price Php {current_price:.2f}): Php {bundle_before_total:.2f}")

print("\nIndividual Revenue Forecast AFTER Bundling/Cannibalization:")
print(f"  {product_a_name}: Php {indiv_rev_after['A']:.2f}")
print(f"  {product_b_name}: Php {indiv_rev_after['B']:.2f}")
print(
    f"  BUNDLE (promo price Php {new_price:.2f}): Php {bundle_after_total:.2f}")

impact_abs = overall_after - overall_before
impact_pct = (impact_abs / overall_before *
              100.0) if overall_before != 0 else float('nan')
print(f"\nOverall Revenue BEFORE (A + B): Php {overall_before:.2f}")
print(
    f"Overall Revenue AFTER  (A_after + B_after + Bundle_after): Php {overall_after:.2f}")
print(f"Revenue Impact: Php {impact_abs:.2f} ({impact_pct:.1f}%)")

# =========================
# BREAK-EVEN OR SURPLUS BUNDLE UNITS
# =========================
overall_before_series = rev_a_before + rev_b_before
overall_after_series = rev_a_after + rev_b_after + rev_bundle_after

revenue_diff_series = (overall_after_series - overall_before_series)

if new_price <= 0:
    print("\nBreak-even / surplus analysis skipped: NEW_PRICE must be > 0.")
else:
    if impact_abs < 0:
        revenue_gap_series = (-revenue_diff_series).clip(lower=0)
        needed_bundles_series = (revenue_gap_series / new_price)
        needed_bundles_ceiling = np.ceil(needed_bundles_series)

        total_gap = float(revenue_gap_series.sum())
        total_needed_bundles = int(np.ceil(max(0.0, total_gap / new_price)))

        print("\n--- BREAK-EVEN (Incremental Bundle Units Required) ---")
        print(f"Overall revenue shortfall: Php {(-impact_abs):.2f}")
        print(f"Bundle price used for break-even: Php {new_price:.2f}")
        print(
            f"Total incremental bundle units required to break even: {total_needed_bundles:,}")

        be_df = pd.DataFrame({
            'Period': COMMON_FC_INDEX,
            'Revenue_Gap': revenue_gap_series.values.round(2),
            'Bundle_Units_Required_Ceil': needed_bundles_ceiling.astype('Int64').values
        })
        print("\nPer-period incremental bundle units required (rounded up):")
        print(be_df.to_string(index=False))

    else:
        surplus_series = revenue_diff_series.clip(lower=0)
        surplus_bundles_series = (surplus_series / new_price)
        surplus_bundles_floor = np.floor(surplus_bundles_series)

        total_surplus = float(surplus_series.sum())
        total_surplus_bundles = int(
            np.floor(max(0.0, total_surplus / new_price)))

        print("\n--- SURPLUS (Bundle Unit Equivalent) ---")
        print(f"Overall revenue surplus: Php {impact_abs:.2f}")
        print(
            f"Bundle price used for surplus equivalence: Php {new_price:.2f}")
        print(
            f"Total bundle units equivalent to surplus (floored): {total_surplus_bundles:,}")

        surplus_df = pd.DataFrame({
            'Period': COMMON_FC_INDEX,
            'Revenue_Surplus': surplus_series.values.round(2),
            'Bundle_Units_Surplus_Floor': surplus_bundles_floor.astype('Int64').values
        })
        print("\nPer-period bundle units equivalent to surplus (floored):")
        print(surplus_df.to_string(index=False))

# =========================
# TREND ANALYSIS (clamped)
# =========================
print("\n--- TREND ANALYSIS (clamped series) ---")
print(detect_trend(bundle_fc, "Baseline bundle forecast"))
print(detect_trend(bundle_fc_adj, "Adjusted bundle forecast (power model)"))
print(detect_trend(a_fc_all, f"{product_a_name} baseline forecast"))
print(detect_trend(a_fc_after_aligned,
      f"{product_a_name} after cannibalization"))
print(detect_trend(b_fc_all, f"{product_b_name} baseline forecast"))
print(detect_trend(b_fc_after_aligned,
      f"{product_b_name} after cannibalization"))

# =========================
# PLOTS
# =========================
fig, axes = plt.subplots(3, 1, figsize=(10, 12), sharex=False)
fig.subplots_adjust(hspace=0.3)

# Plot 1: Bundle forecast
axes[0].plot(bundle_sales_ts.index, bundle_sales_ts.values,
             label=f'Actual', marker='o', color='#1f77b4')
axes[0].plot(COMMON_FC_INDEX, bundle_fc.values,
             label='Baseline Forecast', linestyle='--', marker='x', color='orange')
axes[0].plot(COMMON_FC_INDEX, bundle_fc_adj.values,
             label=f'Adjusted Forecast (Php {new_price:.0f})', linestyle='--', marker='x', color='green')
axes[0].set_title(
    f'Bundle Forecast: {product_a_name} + {product_b_name}\n'
    f'Old price: Php {current_price:.2f} → New price: Php {new_price:.2f}'
)
axes[0].set_ylabel('Bundle Sales (units)')
axes[0].legend()
axes[0].grid(True, alpha=0.3)
axes[0].tick_params(axis='x', rotation=45)

# Plot 2: Product A forecast
if not a_ts_all.empty:
    axes[1].plot(a_ts_all.index, a_ts_all.values,
                 label=f'Actual', marker='o', color='#1f77b4')
axes[1].plot(COMMON_FC_INDEX, a_fc_all.values,
             label='Baseline Forecast', linestyle='--', marker='x', color='orange')
axes[1].plot(COMMON_FC_INDEX, a_fc_after_aligned.values,
             label='After Cannibalization', linestyle='--', marker='x', color='red')
axes[1].set_title(f'{product_a_name} — Baseline vs After Cannibalization')
axes[1].set_ylabel('Units Sold')
axes[1].legend()
axes[1].grid(True, alpha=0.3)
axes[1].tick_params(axis='x', rotation=45)

# Plot 3: Product B forecast
if not b_ts_all.empty:
    axes[2].plot(b_ts_all.index, b_ts_all.values,
                 label=f'Actual', marker='o', color='#1f77b4')
axes[2].plot(COMMON_FC_INDEX, b_fc_all.values,
             label='Baseline Forecast', linestyle='--', marker='x', color='orange')
axes[2].plot(COMMON_FC_INDEX, b_fc_after_aligned.values,
             label='After Cannibalization', linestyle='--', marker='x', color='red')
axes[2].set_title(f'{product_b_name} — Baseline vs After Cannibalization')
axes[2].set_xlabel('Period')
axes[2].set_ylabel('Units Sold')
axes[2].legend()
axes[2].grid(True, alpha=0.3)
axes[2].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# =========================
# MODEL EVALUATION METRICS (SARIMA in-sample fit)
# =========================
print("\n--- MODEL EVALUATION METRICS (SARIMA in-sample fit) ---")


def evaluate_sarima(series: pd.Series, order, seasonal_order, label: str):
    """Fit SARIMA in-sample and compute accuracy metrics."""
    try:
        if series is None or series.empty:
            print(f"[{label}] Skipped — no data.")
            return

        model = SARIMAX(series, order=order, seasonal_order=seasonal_order,
                        enforce_stationarity=False, enforce_invertibility=False)
        fitted = model.fit(disp=False)

        actual_vals = series.values
        fitted_vals = fitted.fittedvalues.values

        # Align lengths
        if len(fitted_vals) < len(actual_vals):
            fitted_vals = np.pad(
                fitted_vals, (len(actual_vals) - len(fitted_vals), 0), mode='edge')

        residuals = actual_vals - fitted_vals
        mse = np.mean(residuals ** 2)
        rmse = np.sqrt(mse)
        mae = np.mean(np.abs(residuals))
        wmape = np.sum(np.abs(residuals)) / np.sum(np.abs(actual_vals)
                                                   ) * 100 if np.sum(actual_vals) != 0 else np.nan
        ss_res = np.sum(residuals ** 2)
        ss_tot = np.sum((actual_vals - np.mean(actual_vals)) ** 2)

        # MASE calculation
        if len(actual_vals) > 1:
            naive_errors = np.abs(actual_vals[1:] - actual_vals[:-1])
            mase_denom = np.mean(naive_errors) if np.mean(naive_errors) != 0 else np.nan
            mase = mae / mase_denom if mase_denom else np.nan
        else:
            mase = np.nan

        print(f"\n[{label}]")
        print(f"  Mean Squared Error (MSE):       {mse:.4f}")
        print(f"  Root Mean Squared Error (RMSE): {rmse:.4f}")
        print(f"  Mean Absolute Error (MAE):      {mae:.4f}")
        print(f"  Weighted MAPE (WMAPE):          {wmape:.2f}%")
        print(f"  Mean Absolute Scaled Error (MASE): {mase:.4f}")

    except Exception as e:
        print(f"[{label}] Evaluation failed: {e}")


# Evaluate all relevant SARIMA fits
evaluate_sarima(bundle_sales_ts, SARIMA_ORDER, SARIMA_SEASONAL_ORDER,
                f"Bundle ({product_a_name} + {product_b_name})")
evaluate_sarima(a_ts_all, SARIMA_ORDER, SARIMA_SEASONAL_ORDER,
                f"{product_a_name} (Individual)")
evaluate_sarima(b_ts_all, SARIMA_ORDER, SARIMA_SEASONAL_ORDER,
                f"{product_b_name} (Individual)")
