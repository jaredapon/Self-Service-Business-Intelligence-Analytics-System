import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX
import warnings

# Suppress SARIMAX warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message="Too few observations")
warnings.filterwarnings(
    "ignore", message="Maximum Likelihood optimization failed")

# =========================
# USER CONFIGURATION
# =========================
PARENT_DIR = 'mba_output'
RULES_PATH = PARENT_DIR + '/association_rules.csv'
FACT_PATH = 'etl_dimensions/fact_transaction_dimension.csv'
PRODUCT_PATH = 'etl_dimensions/current_product_dimension.csv'
PED_SUMMARY_PATH = PARENT_DIR + '/ped_output/ped_summary.csv'
NLP_OPT_PATH = PARENT_DIR + '/nlp_output/nlp_optimization_results.csv'
OUTPUT_DIR = 'sarima_results'

# Time-series settings
AGG_FREQ = 'QE'            # Quarterly frequency
SEASONAL_PERIODS = 4       # 4 quarters per year
HORIZON = 4                # Forecast 4 periods ahead

# SARIMA parameters
SARIMA_ORDER = (1, 1, 1)
SARIMA_SEASONAL_ORDER = (1, 1, 1, SEASONAL_PERIODS)

# Price scenario (fallback)
DISCOUNT_RATE = 0.05

# Evaluation mode - Show one sample from each category (food, drink, meal)
SHOW_SAMPLE_MODE = False
CATEGORIES_TO_SHOW = ['food', 'drink', 'meal']  # Show one from each

# =========================
# HELPER FUNCTIONS
# =========================


def resolve_ids(product_df: pd.DataFrame, name_a: str, name_b: str) -> tuple[str, str]:
    a = product_df.loc[product_df['product_name'] == name_a]
    b = product_df.loc[product_df['product_name'] == name_b]
    if a.empty or b.empty:
        raise ValueError(
            f"Could not find product IDs for '{name_a}' or '{name_b}'.")
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


def fit_and_forecast_to_index(series: pd.Series, label: str, idx: pd.DatetimeIndex) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(0.0, index=idx)
    model = SARIMAX(
        series,
        order=SARIMA_ORDER,
        seasonal_order=SARIMA_SEASONAL_ORDER,
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    fitted = model.fit(disp=False)
    fc = fitted.forecast(len(idx))
    fc.index = idx
    return fc


def build_ts_all(lines: pd.DataFrame, AGG_FREQ: str) -> pd.Series:
    if lines.empty:
        return pd.Series(dtype=float)
    rec = lines.groupby('Receipt No').agg(Date=('Date', 'first'))
    rec['Date'] = pd.to_datetime(rec['Date'], errors='coerce')
    return rec.groupby(pd.Grouper(key='Date', freq=AGG_FREQ)).size()


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
        if len(actual_vals) > 1:
            naive_errors = np.abs(actual_vals[1:] - actual_vals[:-1])
            mase_denom = np.mean(naive_errors) if np.mean(naive_errors) != 0 else np.nan
            mase = mae / mase_denom if mase_denom else np.nan
        else:
            mase = np.nan
        print(f"\n{label}:")
        print(f"MAE:  {mae:.3f}")
        print(f"MSE:  {mse:.3f}")
        print(f"RMSE: {rmse:.3f}")
        print(f"MAPE: {wmape:.2f}%")
        print(f"MASE: {mase:.4f}")
        
    except Exception as e:
        print(f"[{label}] Evaluation failed: {e}")


# =========================
# LOAD DATA
# =========================
try:
    rules_df = pd.read_csv(RULES_PATH)
    fact_df = pd.read_csv(FACT_PATH)
    product_df = pd.read_csv(PRODUCT_PATH)
    ped_df = pd.read_csv(PED_SUMMARY_PATH)
    nlp_opt_df = pd.read_csv(NLP_OPT_PATH)
except FileNotFoundError as e:
    print(f"Error loading required data: {e}")
    sys.exit(1)

os.makedirs(OUTPUT_DIR, exist_ok=True)
all_results = []

nlp_opt_indexed = nlp_opt_df.set_index('bundle_id')

# Track first bundle per category for graphing
graphed_categories = set()

# Track categories processed in sample mode
processed_sample_categories = set()

# =========================
# LOOP THROUGH ALL BUNDLE ROWS
# =========================
for idx, rule_row in rules_df.iterrows():
    try:
        product_a_name = str(rule_row['antecedents_names'])
        product_b_name = str(rule_row['consequents_names'])
        bundle_id = rule_row['bundle_id'] if 'bundle_id' in rule_row else ""
        category = rule_row['category'] if 'category' in rule_row else ""

        # Skip if in sample mode and we already processed this category
        if SHOW_SAMPLE_MODE and category.lower() in processed_sample_categories:
            continue

        # Skip if in sample mode and this category is not in our list
        if SHOW_SAMPLE_MODE and category.lower() not in CATEGORIES_TO_SHOW:
            continue

        print(f"\n==============================")
        print(
            f"Processing bundle row {idx}: {product_a_name} + {product_b_name}")
        print(f"Bundle ID: {bundle_id}")
        print(f"Category: {category}")
        print(f"==============================")

        product_a_id, product_b_id = resolve_ids(
            product_df, product_a_name, product_b_name)

        ped_row = pick_ped_row(ped_df, product_a_id, product_b_id)
        if ped_row is None:
            print(
                f" Skipping {product_a_name} + {product_b_name}: No PED match.")
            continue

        epsilon = float(ped_row.get('elasticity_epsilon', 0.0) or 0.0)
        intercept = float(ped_row.get('intercept_logk', 0.0) or 0.0)
        n_points = int(ped_row.get('n_price_points', 0) or 0)

        pair_transactions = fact_df[fact_df['Product ID'].astype(
            str).isin([product_a_id, product_b_id])]
        receipt_products = pair_transactions.groupby(
            'Receipt No')['Product ID'].apply(lambda s: set(s.astype(str)))
        receipts_with_both = receipt_products[
            receipt_products.apply(lambda s: (
                product_a_id in s) and (product_b_id in s))
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

        a_lines_all = fact_df[fact_df['Product ID'].astype(
            str) == str(product_a_id)]
        b_lines_all = fact_df[fact_df['Product ID'].astype(
            str) == str(product_b_id)]
        a_ts_all = build_ts_all(a_lines_all, AGG_FREQ)
        b_ts_all = build_ts_all(b_lines_all, AGG_FREQ)

        if bundle_sales_ts.empty and (a_ts_all.empty and b_ts_all.empty):
            print(" No sales data found. Skipping.")
            continue

        current_price_a = float(
            product_df.loc[product_df['product_id'].astype(
                str) == str(product_a_id), 'Price'].iloc[0]
        )
        current_price_b = float(
            product_df.loc[product_df['product_id'].astype(
                str) == str(product_b_id), 'Price'].iloc[0]
        )
        current_price = current_price_a + current_price_b

        recommended_price = None
        if bundle_id and bundle_id in nlp_opt_indexed.index:
            try:
                recommended_price = float(
                    nlp_opt_indexed.loc[bundle_id, 'bundle_price_recommended'])
            except KeyError:
                recommended_price = None

        if recommended_price is not None and not np.isnan(recommended_price):
            new_price = recommended_price
        else:
            new_price = current_price * (1 - DISCOUNT_RATE)

        price_ratio = new_price / current_price if current_price > 0 else 1.0
        demand_multiplier = price_ratio ** epsilon if current_price > 0 else 1.0

        if AGG_FREQ.upper().startswith('Q'):
            step = pd.offsets.QuarterEnd()
        elif AGG_FREQ.upper().startswith('M'):
            step = pd.offsets.MonthEnd()
        else:
            step = pd.tseries.frequencies.to_offset(AGG_FREQ)

        def last_index_or_min(ts: pd.Series):
            return ts.index[-1] if (ts is not None and not ts.empty) else pd.Timestamp.min

        latest_actual = max([
            last_index_or_min(bundle_sales_ts),
            last_index_or_min(a_ts_all),
            last_index_or_min(b_ts_all)
        ])
        COMMON_FC_INDEX = pd.date_range(
            start=latest_actual + step, periods=HORIZON, freq=AGG_FREQ)

        bundle_fc_raw = fit_and_forecast_to_index(
            bundle_sales_ts, "Bundle", COMMON_FC_INDEX)
        a_fc_all_raw = fit_and_forecast_to_index(
            a_ts_all, f"{product_a_name}", COMMON_FC_INDEX)
        b_fc_all_raw = fit_and_forecast_to_index(
            b_ts_all, f"{product_b_name}", COMMON_FC_INDEX)

        bundle_fc = bundle_fc_raw.clip(lower=0)
        bundle_fc_adj = (bundle_fc_raw * demand_multiplier).clip(lower=0)
        a_fc_all = a_fc_all_raw.clip(lower=0)
        b_fc_all = b_fc_all_raw.clip(lower=0)

        cannibalization_units = bundle_fc
        a_fc_after_aligned = (a_fc_all - cannibalization_units).clip(lower=0)
        b_fc_after_aligned = (b_fc_all - cannibalization_units).clip(lower=0)

        def revenue_forecast(series, price):
            return series * price

        price_a, price_b = current_price_a, current_price_b

        rev_a_before = revenue_forecast(a_fc_all, price_a)
        rev_b_before = revenue_forecast(b_fc_all, price_b)
        rev_a_after = revenue_forecast(a_fc_after_aligned, price_a)
        rev_b_after = revenue_forecast(b_fc_after_aligned, price_b)
        rev_bundle_after = bundle_fc_adj * new_price

        overall_before = rev_a_before.sum() + rev_b_before.sum()
        overall_after = rev_a_after.sum() + rev_b_after.sum() + rev_bundle_after.sum()
        impact_abs = overall_after - overall_before
        impact_pct = (impact_abs / overall_before *
                      100.0) if overall_before != 0 else np.nan

        df_points = pd.DataFrame({
            'Bundle_Units': bundle_sales_ts,
            'Antecedent_Units': a_ts_all,
            'Consequent_Units': b_ts_all
        })
        df_forecast = pd.DataFrame({
            'Bundle_Units_Forecast': bundle_fc,
            'Bundle_Units_Adjusted_Forecast': bundle_fc_adj,
            'Antecedent_Units_Forecast': a_fc_all,
            'Antecedent_Units_After_Cannibalization': a_fc_after_aligned,
            'Consequent_Units_Forecast': b_fc_all,
            'Consequent_Units_After_Cannibalization': b_fc_after_aligned
        })
        df_all = pd.concat([df_points, df_forecast], axis=0)
        df_all.index.name = 'Date'
        df_all['bundle_row'] = idx
        df_all['bundle_id'] = bundle_id
        df_all['category'] = category

        all_results.append(df_all)

        # =========================
        # MODEL EVALUATION
        # =========================
        print(f"\n  Model Evaluation Metrics:")
        evaluate_sarima(a_ts_all, SARIMA_ORDER, SARIMA_SEASONAL_ORDER,
                        f"{product_a_name} (Individual Item)")
        evaluate_sarima(b_ts_all, SARIMA_ORDER, SARIMA_SEASONAL_ORDER,
                        f"{product_b_name} (Individual Item)")
        evaluate_sarima(bundle_sales_ts, SARIMA_ORDER, SARIMA_SEASONAL_ORDER,
                        f"Bundle ({product_a_name} + {product_b_name})")

        # =========================
        # GENERATE PLOT (only for first bundle of each category)
        # =========================
        if category not in graphed_categories:
            graphed_categories.add(category)

            fig, axes = plt.subplots(3, 1, figsize=(10, 12), sharex=False)
            fig.subplots_adjust(hspace=0.3)

            # Plot 1: Bundle forecast
            if not bundle_sales_ts.empty:
                axes[0].plot(bundle_sales_ts.index, bundle_sales_ts.values,
                             label='Actual', marker='o', color='#1f77b4')
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
                             label='Actual', marker='o', color='#1f77b4')
            axes[1].plot(COMMON_FC_INDEX, a_fc_all.values,
                         label='Baseline Forecast', linestyle='--', marker='x', color='orange')
            axes[1].plot(COMMON_FC_INDEX, a_fc_after_aligned.values,
                         label='After Cannibalization', linestyle='--', marker='x', color='red')
            axes[1].set_title(
                f'{product_a_name} — Baseline vs After Cannibalization')
            axes[1].set_ylabel('Units Sold')
            axes[1].legend()
            axes[1].grid(True, alpha=0.3)
            axes[1].tick_params(axis='x', rotation=45)

            # Plot 3: Product B forecast
            if not b_ts_all.empty:
                axes[2].plot(b_ts_all.index, b_ts_all.values,
                             label='Actual', marker='o', color='#1f77b4')
            axes[2].plot(COMMON_FC_INDEX, b_fc_all.values,
                         label='Baseline Forecast', linestyle='--', marker='x', color='orange')
            axes[2].plot(COMMON_FC_INDEX, b_fc_after_aligned.values,
                         label='After Cannibalization', linestyle='--', marker='x', color='red')
            axes[2].set_title(
                f'{product_b_name} — Baseline vs After Cannibalization')
            axes[2].set_xlabel('Period')
            axes[2].set_ylabel('Units Sold')
            axes[2].legend()
            axes[2].grid(True, alpha=0.3)
            axes[2].tick_params(axis='x', rotation=45)

            plt.tight_layout()

            # Save the plot
            plot_filename = f"{bundle_id}_{category}_sarima_forecast.png"
            plot_path = os.path.join(OUTPUT_DIR, plot_filename)
            plt.savefig(plot_path, dpi=150, bbox_inches='tight')
            plt.close()

            print(
                f"Original Price: {current_price:.2f} | "
                f"Recommended Price: {new_price:.2f} | "
                f"Impact: {impact_abs:.2f} ({impact_pct:.1f}%)"
            )
            print(f"  Graph saved: {plot_filename}")
        else:
            print(
                f"Original Price: {current_price:.2f} | "
                f"Recommended Price: {new_price:.2f} | "
                f"Impact: {impact_abs:.2f} ({impact_pct:.1f}%)"
            )

        # In sample mode, track this category as processed
        if SHOW_SAMPLE_MODE:
            processed_sample_categories.add(category.lower())
            # Stop if we've processed one bundle from each desired category
            if all(cat in processed_sample_categories for cat in CATEGORIES_TO_SHOW):
                print(
                    f"\n[SAMPLE MODE: Processed one bundle from each category: {', '.join(CATEGORIES_TO_SHOW)}]")
                print(f"[Set SHOW_SAMPLE_MODE=False to process all bundles.]")
                break

    except Exception as e:
        print(f" Error in row {idx}: {e}")
        continue

# =========================
# SAVE ALL RESULTS
# =========================
if all_results:
    combined_df = pd.concat(all_results)
    drop_cols = [
        'antecedent_name', 'consequent_name',
        'elasticity_epsilon', 'intercept_logk',
        'revenue_impact_abs', 'revenue_impact_pct'
    ]
    combined_df = combined_df.drop(
        columns=[c for c in drop_cols if c in combined_df.columns], errors='ignore')
    OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'sarima_results_all.csv')
    combined_df.to_csv(OUTPUT_CSV, index=True)
    print(f"\nAll bundle forecasts saved to {OUTPUT_CSV}")
else:
    print("\n No successful forecasts generated.")
