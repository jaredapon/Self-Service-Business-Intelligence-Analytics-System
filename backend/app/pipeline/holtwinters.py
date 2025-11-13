import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# =========================
# USER CONFIGURATION
# =========================

# Time-series settings
AGG_FREQ = 'QE'            # Quarterly frequency
SEASONAL_PERIODS = 4       # 4 quarters per year
HORIZON = 4                # Forecast 4 periods ahead

# Holt-Winters parameters
OPTIMIZED = False
HW_ALPHA = 0.2
HW_BETA  = 0.2
HW_GAMMA = 0.2

# Price scenario (fallback)
DISCOUNT_RATE = 0.05

# =========================
# HELPER FUNCTIONS
# =========================
def resolve_ids(product_df: pd.DataFrame, name_a: str, name_b: str) -> tuple[str, str]:
    a = product_df.loc[product_df['product_name'] == name_a]
    b = product_df.loc[product_df['product_name'] == name_b]
    if a.empty or b.empty:
        raise ValueError(f"Could not find product IDs for '{name_a}' or '{name_b}'.")
    return str(a['product_id'].iloc[0]), str(b['product_id'].iloc[0])

def pick_ped_row(ped_df: pd.DataFrame, id_a: str, id_b: str) -> pd.Series | None:
    a, b = str(id_a), str(id_b)
    m1 = (ped_df['product_id_1'].astype(str) == a) & (ped_df['product_id_2'].astype(str) == b)
    m2 = (ped_df['product_id_1'].astype(str) == b) & (ped_df['product_id_2'].astype(str) == a)
    matches = ped_df[m1 | m2].copy()
    if matches.empty:
        return None
    non_strict = matches[matches['mode'].astype(str).str.lower() == 'non_strict']
    if not non_strict.empty:
        return non_strict.iloc[0]
    return matches.iloc[0]

def fit_and_forecast_to_index(series: pd.Series, label: str, idx: pd.DatetimeIndex) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(0.0, index=idx)
    model = ExponentialSmoothing(
        series, trend='add', seasonal='add',
        seasonal_periods=SEASONAL_PERIODS, initialization_method="estimated"
    )
    if OPTIMIZED:
        fitted = model.fit(optimized=True)
    else:
        fitted = model.fit(
            smoothing_level=HW_ALPHA,
            smoothing_trend=HW_BETA,
            smoothing_seasonal=HW_GAMMA,
            optimized=False
        )
    fc = fitted.forecast(len(idx))
    fc.index = idx
    return fc

def build_ts_all(lines: pd.DataFrame, AGG_FREQ: str) -> pd.Series:
    if lines.empty:
        return pd.Series(dtype=float)
    rec = lines.groupby('Receipt No').agg(Date=('Date', 'first'))
    rec['Date'] = pd.to_datetime(rec['Date'], errors='coerce')
    return rec.groupby(pd.Grouper(key='Date', freq=AGG_FREQ)).size()

# =========================
# MAIN
# =========================
def main():
    # Import loader
    from . import loader
    import time
    
    # LOAD DATA from PostgreSQL
    print("Loading data from PostgreSQL...")
    try:
        rules_df   = loader.export_table_to_csv('association_rules')
        fact_df    = loader.export_table_to_csv('fact_transaction_dimension')
        product_df = loader.export_table_to_csv('current_product_dimension')
        ped_df     = loader.export_table_to_csv('ped_summary')
        nlp_opt_df = loader.export_table_to_csv('nlp_optimization_results')
    except Exception as e:
        print(f"Error loading required data: {e}")
        sys.exit(1)

    # Normalize column names
    rules_df.columns = [c.lower() for c in rules_df.columns]
    fact_df.columns = [c.title().replace('_', ' ') for c in fact_df.columns]
    product_df.columns = [c.lower() for c in product_df.columns]
    ped_df.columns = [c.lower() for c in ped_df.columns]
    nlp_opt_df.columns = [c.lower() for c in nlp_opt_df.columns]
    
    # Compatibility renames
    fact_df = fact_df.rename(columns={'Product Id': 'Product ID'})
    product_df = product_df.rename(columns={'price': 'Price'})

    all_results = []
    nlp_opt_indexed = nlp_opt_df.set_index('bundle_id')

    # =========================
    # LOOP THROUGH ALL BUNDLE ROWS
    # =========================
    for idx, rule_row in rules_df.iterrows():
        try:
            product_a_name = str(rule_row['antecedents_names'])
            product_b_name = str(rule_row['consequents_names'])
            bundle_id = rule_row['bundle_id'] if 'bundle_id' in rule_row else ""
            category  = rule_row['category'] if 'category' in rule_row else ""

            print(f"\n==============================")
            print(f"Processing bundle row {idx}: {product_a_name} + {product_b_name}")
            print(f"Bundle ID: {bundle_id}")
            print(f"==============================")

            product_a_id, product_b_id = resolve_ids(product_df, product_a_name, product_b_name)

            ped_row = pick_ped_row(ped_df, product_a_id, product_b_id)
            if ped_row is None:
                print(f" Skipping {product_a_name} + {product_b_name}: No PED match.")
                continue

            epsilon   = float(ped_row.get('elasticity_epsilon', 0.0) or 0.0)
            intercept = float(ped_row.get('intercept_logk', 0.0) or 0.0)
            n_points  = int(ped_row.get('n_price_points', 0) or 0)

            pair_transactions = fact_df[fact_df['Product ID'].astype(str).isin([product_a_id, product_b_id])]
            receipt_products = pair_transactions.groupby('Receipt No')['Product ID'].apply(lambda s: set(s.astype(str)))
            receipts_with_both = receipt_products[
                receipt_products.apply(lambda s: (product_a_id in s) and (product_b_id in s))
            ].index

            working_df = fact_df[fact_df['Receipt No'].isin(receipts_with_both)]
            receipt_summary = working_df.groupby('Receipt No').agg(
                Combined_Price=('Line Total', 'sum'),
                Date=('Date', 'first')
            )
            receipt_summary['Date'] = pd.to_datetime(receipt_summary['Date'], errors='coerce')
            bundle_sales_ts = receipt_summary.groupby(pd.Grouper(key='Date', freq=AGG_FREQ)).size()

            a_lines_all = fact_df[fact_df['Product ID'].astype(str) == str(product_a_id)]
            b_lines_all = fact_df[fact_df['Product ID'].astype(str) == str(product_b_id)]
            a_ts_all = build_ts_all(a_lines_all, AGG_FREQ)
            b_ts_all = build_ts_all(b_lines_all, AGG_FREQ)

            if bundle_sales_ts.empty and (a_ts_all.empty and b_ts_all.empty):
                print(" No sales data found. Skipping.")
                continue

            current_price_a = float(
                product_df.loc[product_df['product_id'].astype(str) == str(product_a_id), 'Price'].iloc[0]
            )
            current_price_b = float(
                product_df.loc[product_df['product_id'].astype(str) == str(product_b_id), 'Price'].iloc[0]
            )
            current_price = current_price_a + current_price_b

            recommended_price = None
            if bundle_id and bundle_id in nlp_opt_indexed.index:
                try:
                    recommended_price = float(nlp_opt_indexed.loc[bundle_id, 'bundle_price_recommended'])
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
            COMMON_FC_INDEX = pd.date_range(start=latest_actual + step, periods=HORIZON, freq=AGG_FREQ)

            bundle_fc_raw = fit_and_forecast_to_index(bundle_sales_ts, "Bundle", COMMON_FC_INDEX)
            a_fc_all_raw = fit_and_forecast_to_index(a_ts_all, f"{product_a_name}", COMMON_FC_INDEX)
            b_fc_all_raw = fit_and_forecast_to_index(b_ts_all, f"{product_b_name}", COMMON_FC_INDEX)

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
            rev_a_after  = revenue_forecast(a_fc_after_aligned, price_a)
            rev_b_after  = revenue_forecast(b_fc_after_aligned, price_b)
            rev_bundle_after = bundle_fc_adj * new_price

            overall_before = rev_a_before.sum() + rev_b_before.sum()
            overall_after  = rev_a_after.sum() + rev_b_after.sum() + rev_bundle_after.sum()
            impact_abs = overall_after - overall_before
            impact_pct = (impact_abs / overall_before * 100.0) if overall_before != 0 else np.nan

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
            df_all['category']  = category

            all_results.append(df_all)

            print(
                f"Original Price: {current_price:.2f} | "
                f"Recommended Price: {new_price:.2f} | "
                f"Impact: {impact_abs:.2f} ({impact_pct:.1f}%)"
            )

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
        combined_df = combined_df.drop(columns=[c for c in drop_cols if c in combined_df.columns], errors='ignore')
        
        # Convert column names to snake_case
        combined_df.columns = [col.replace(' ', '_').lower() for col in combined_df.columns]
        combined_df.index.name = combined_df.index.name.lower() if combined_df.index.name else None
        
        # Upload to MinIO staging and PostgreSQL
        print("\nUploading results to MinIO and PostgreSQL...")
        csv_bytes = combined_df.to_csv(index=True).encode('utf-8')
        
        # Upload to MinIO
        run_id = time.strftime("%Y%m%d_%H%M%S")
        from app.core.config import settings
        minio_path = f"models/holtwinters/{run_id}/holtwinters_results_all.csv"
        loader.staging_put_bytes(minio_path, csv_bytes)
        print(f"Uploaded to MinIO: {minio_path}")
        
        # Clear and load to PostgreSQL
        loader.clear_result_table('holtwinters_results_all')
        loader.load_result_csv_to_table(csv_bytes, 'holtwinters_results_all')
        print("Loaded to PostgreSQL: holtwinters_results_all")
        
        # Clean up MinIO staging after successful load
        loader.staging_delete_prefix(f"models/holtwinters/{run_id}")
        print(f"Cleaned up MinIO staging: models/holtwinters/{run_id}")
    else:
        print("\n No successful forecasts generated.")

if __name__ == '__main__':
    main()
