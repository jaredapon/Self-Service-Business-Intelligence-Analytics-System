import sys
import numpy as np
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from io import BytesIO
from minio import MinIO
from minio.error import S3Error
from app.core.config import settings

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

# --- New: MinIO Client Initialization ---
try:
    minio_client = MinIO(
        settings.minio_endpoint,
        access_key=settings.minio_access,
        secret_key=settings.minio_secret,
        secure=settings.minio_secure,
    )
    print("✅ Successfully connected to MinIO.")
except Exception as e:
    print(f"❌ Failed to connect to MinIO: {e}")
    minio_client = None

# =========================
# NEW HELPERS: MINIO I/O
# =========================

def get_csv_from_minio(bucket, object_name):
    """Downloads a CSV file from MinIO and returns it as a pandas DataFrame."""
    if not minio_client:
        print(f"MinIO client not available. Cannot download {object_name}.")
        return pd.DataFrame()

    try:
        print(f"  Downloading: {bucket}/{object_name}")
        response = minio_client.get_object(bucket, object_name)
        file_content = BytesIO(response.read())
        df = pd.read_csv(file_content)
        response.close()
        response.release_conn()
        return df
    except S3Error as e:
        print(f"Error getting file from MinIO at {bucket}/{object_name}: {e}")
        return pd.DataFrame()

def upload_df_to_minio(df, bucket, object_name):
    """Uploads a pandas DataFrame as a CSV to MinIO."""
    if not minio_client:
        print("MinIO client not available. Skipping upload.")
        return

    csv_bytes = df.to_csv(index=True).encode('utf-8') # Keep index for time-series
    csv_buffer = BytesIO(csv_bytes)

    try:
        minio_client.put_object(
            bucket,
            object_name,
            data=csv_buffer,
            length=len(csv_bytes),
            content_type='application/csv'
        )
        print(f"  Successfully uploaded to: {bucket}/{object_name}")
    except S3Error as e:
        print(f"Error uploading {object_name} to MinIO: {e}")

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
# LOAD DATA
# =========================
print("Loading data from MinIO staging bucket...")
rules_df   = get_csv_from_minio(settings.minio_staging_bucket, 'association_rules.csv')
fact_df    = get_csv_from_minio(settings.minio_staging_bucket, 'fact_transaction_dimension.csv')
product_df = get_csv_from_minio(settings.minio_staging_bucket, 'current_product_dimension.csv')
ped_df     = get_csv_from_minio(settings.minio_staging_bucket, 'ped_summary.csv')
nlp_opt_df = get_csv_from_minio(settings.minio_staging_bucket, 'nlp_optimization_results.csv')

if any(df.empty for df in [rules_df, fact_df, product_df, ped_df, nlp_opt_df]):
    print("Error: Could not load one or more required files from MinIO. Exiting.")
    sys.exit(1)

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

    print("\nUploading Holt-Winters results to MinIO staging bucket...")
    upload_df_to_minio(
        combined_df,
        settings.minio_staging_bucket,
        'holtwinters_results_all.csv'
    )
    print("\nAll bundle forecasts saved successfully to MinIO.")
else:
    print("\nNo successful forecasts generated.")
