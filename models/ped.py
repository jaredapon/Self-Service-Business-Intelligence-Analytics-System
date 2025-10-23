import os
import sys
import math
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

# =========================
# USER CONFIGURATION
# =========================
SAVE_FOLDER = 'mba_foods'
RULES_PATH   = SAVE_FOLDER + '/association_rules.csv'
FACT_PATH    = 'etl_dimensions/fact_transaction_dimension.csv'
PRODUCT_PATH = 'etl_dimensions/current_product_dimension.csv'
TOP_N        = 15
OUT_DIR      = SAVE_FOLDER + '/ped_output'   # folder for CSV + plots

# Toggle: use only receipts whose product set is EXACTLY {A, B} (no other items)
STRICT_BUNDLE_ONLY = False

# =========================
# Helpers
# =========================
def require_columns(df: pd.DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"Error: {name} is missing columns: {missing}")
        sys.exit(1)

def resolve_ids_by_exact_names(rule_row: pd.Series, product_df: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    EXACT product_name match, identical to your main Holt–Winters script.
    Returns (id_a, id_b) or (None, None) if not found.
    """
    nameA = rule_row.get('antecedents_names', None)
    nameB = rule_row.get('consequents_names', None)
    if nameA is None or nameB is None:
        return None, None
    a = product_df.loc[product_df['product_name'] == str(nameA)]
    b = product_df.loc[product_df['product_name'] == str(nameB)]
    if a.empty or b.empty:
        return None, None
    return str(a['product_id'].iloc[0]), str(b['product_id'].iloc[0])

def safe_name(product_df: pd.DataFrame, product_id: str) -> str:
    row = product_df.loc[product_df['product_id'].astype(str) == str(product_id)]
    return str(row['product_name'].iloc[0]) if not row.empty else f"(id:{product_id})"

def slugify(text: str, maxlen: int = 80) -> str:
    s = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '-' for ch in str(text))
    s = '-'.join(filter(None, s.split('-')))
    return s[:maxlen] if len(s) > maxlen else s

# ---------- Price–Quantity builders ----------
def build_price_qty_points(fact_df: pd.DataFrame, id_a: str, id_b: str) -> pd.DataFrame:
    """
    NON-STRICT version:
      1) Identify receipts that contain BOTH A and B (they may include other items).
      2) For those receipts, compute Combined_Price = sum(Line Total) of ONLY the A and B lines.
      3) Group by Combined_Price -> Num_Transactions (count of receipts at that A+B combined price).
    Returns: DataFrame[Combined_Price, Num_Transactions]
    """
    ida, idb = str(id_a), str(id_b)

    # First, find receipts that contain BOTH A and B (at least once)
    pair_candidate_lines = fact_df[fact_df['Product ID'].astype(str).isin([ida, idb])]
    receipt_products = pair_candidate_lines.groupby('Receipt No')['Product ID'] \
                                           .apply(lambda s: set(s.astype(str)))
    both_receipts = receipt_products[receipt_products.apply(lambda s: (ida in s) and (idb in s))].index

    if len(both_receipts) == 0:
        return pd.DataFrame(columns=['Combined_Price', 'Num_Transactions'])

    # Now restrict to ONLY the A/B lines on those receipts to compute price
    ab_lines = fact_df[
        fact_df['Receipt No'].isin(both_receipts) &
        fact_df['Product ID'].astype(str).isin([ida, idb])
    ]

    receipt_summary = ab_lines.groupby('Receipt No').agg(
        Combined_Price=('Line Total', 'sum'),
        Date=('Date', 'first')
    )

    # Optional: round to 2 decimals to merge near-duplicates (e.g., 419.399999 vs 419.40)
    # receipt_summary['Combined_Price'] = receipt_summary['Combined_Price'].round(2)

    price_qty = (receipt_summary.groupby('Combined_Price')
                 .agg(Num_Transactions=('Date', 'count'))
                 .reset_index()
                 .sort_values('Combined_Price'))

    return price_qty

def build_price_qty_points_strict(fact_df: pd.DataFrame, id_a: str, id_b: str) -> pd.DataFrame:
    """
    STRICT version:
      Use only receipts whose product set is exactly {A, B} (no other items).
      Combined_Price = sum(Line Total) of ONLY the A and B lines on those receipts.
    Returns: DataFrame[Combined_Price, Num_Transactions]
    """
    ida, idb = str(id_a), str(id_b)

    # Build product set for EVERY receipt
    receipt_sets = fact_df.groupby('Receipt No')['Product ID'] \
                          .apply(lambda s: set(s.astype(str)))

    target_set = {ida, idb}
    strict_receipts = receipt_sets[receipt_sets.apply(lambda s: s == target_set)].index

    if len(strict_receipts) == 0:
        return pd.DataFrame(columns=['Combined_Price', 'Num_Transactions'])

    # Only A/B lines from those receipts
    strict_ab = fact_df[
        fact_df['Receipt No'].isin(strict_receipts) &
        fact_df['Product ID'].astype(str).isin([ida, idb])
    ]

    receipt_summary = strict_ab.groupby('Receipt No').agg(
        Combined_Price=('Line Total', 'sum'),
        Date=('Date', 'first')
    )

    # Optional rounding as above
    # receipt_summary['Combined_Price'] = receipt_summary['Combined_Price'].round(2)

    price_qty = (receipt_summary.groupby('Combined_Price')
                 .agg(Num_Transactions=('Date', 'count'))
                 .reset_index()
                 .sort_values('Combined_Price'))

    return price_qty

# ---------- Estimation & Plotting ----------
def estimate_elasticity(price_qty_df: pd.DataFrame) -> dict:
    """
    Estimate constant-elasticity via log–log regression:
      - Filter to Combined_Price > 0 and Num_Transactions > 0
      - log_Q = intercept + ε * log_P
      - LinearRegression without weights (matches your main script)
    Returns dict with epsilon, intercept, r2, n_points, and (if n>=2) df & y_hat.
    """
    df = price_qty_df[(price_qty_df['Combined_Price'] > 0) &
                      (price_qty_df['Num_Transactions'] > 0)].copy()

    df['log_P'] = np.log(df['Combined_Price'])
    df['log_Q'] = np.log(df['Num_Transactions'])

    n = len(df)
    if n < 2:
        return {'epsilon': 0.0, 'intercept': 0.0, 'r2': float('nan'), 'n_points': n}

    X = df[['log_P']].to_numpy()
    y = df['log_Q'].to_numpy()
    reg = LinearRegression().fit(X, y)

    epsilon = float(reg.coef_[0])       # slope = elasticity
    intercept = float(reg.intercept_)   # intercept = log(k)
    y_hat = reg.predict(X)
    r2 = float(r2_score(y, y_hat))

    return {'epsilon': epsilon, 'intercept': intercept, 'r2': r2, 'n_points': n, 'df': df, 'y_hat': y_hat}

def plot_demand_curve(df_with_logs: pd.DataFrame,
                      bundle_label: str,
                      epsilon: float,
                      intercept: float,
                      out_path: Path):
    """
    Plot the demand curve in original units (price on X, demand on Y):
      Q = exp(intercept) * P^epsilon
    Also scatter the observed (price, demand) points for context.
    """
    # Keep positive points only (to match regression input)
    dfp = df_with_logs[(df_with_logs['Combined_Price'] > 0) &
                       (df_with_logs['Num_Transactions'] > 0)].copy()
    if dfp.empty:
        return

    # Price grid over observed range
    p_min = float(dfp['Combined_Price'].min())
    p_max = float(dfp['Combined_Price'].max())
    if not np.isfinite(p_min) or not np.isfinite(p_max) or p_min <= 0 or p_max <= 0:
        return
    p_grid = np.linspace(p_min, p_max, 200)

    # Demand curve from fitted model
    k = np.exp(intercept)
    q_grid = k * (p_grid ** epsilon)

    # Plot
    plt.figure(figsize=(8, 6))
    plt.scatter(dfp['Combined_Price'], dfp['Num_Transactions'],
                label='Observed points', alpha=0.85, marker='o')
    plt.plot(p_grid, q_grid, linestyle='--', linewidth=2.0, label='Fitted demand curve')

    plt.title(
        f"Demand Curve: {bundle_label}\n"
        f"log(Q) = {intercept:.3f} + {epsilon:.3f}·log(P)"
    )
    plt.xlabel('Price (Combined Price from A+B lines only)')
    plt.ylabel('Demand (Num Transactions)')
    plt.grid(True, alpha=0.3)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

# =========================
# Main
# =========================
def main():
    # Load
    try:
        rules_df   = pd.read_csv(RULES_PATH)
        fact_df    = pd.read_csv(FACT_PATH)
        product_df = pd.read_csv(PRODUCT_PATH)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Sanity
    require_columns(rules_df,   ['antecedents_names', 'consequents_names'], 'RULES file')
    require_columns(fact_df,    ['Product ID', 'Receipt No', 'Line Total', 'Date'], 'FACT file')
    require_columns(product_df, ['product_id', 'product_name', 'Price'], 'PRODUCT file')

    # Output dirs
    out_dir = Path(OUT_DIR)
    plots_dir = out_dir / 'plots'
    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    mode = "STRICT {A,B} only" if STRICT_BUNDLE_ONLY else "Receipts containing A and B (may include others)"
    print(f"=== PED Summary for TOP {min(TOP_N, len(rules_df))} bundles (demand-curve regression) ===")
    print(f"Mode: {mode}")

    # Iterate TOP_N rules
    rows = []
    n = min(TOP_N, len(rules_df))
    for i in range(n):
        rule = rules_df.iloc[i]
        id_a, id_b = resolve_ids_by_exact_names(rule, product_df)
        if not id_a or not id_b:
            print(f"[SKIP] Row {i}: cannot resolve product IDs from names.")
            continue

        name_a = safe_name(product_df, id_a)
        name_b = safe_name(product_df, id_b)
        bundle_label = f"{name_a} + {name_b}"

        # Build price-quantity points according to toggle
        if STRICT_BUNDLE_ONLY:
            price_qty = build_price_qty_points_strict(fact_df, id_a, id_b)
        else:
            price_qty = build_price_qty_points(fact_df, id_a, id_b)

        # Estimate elasticity
        est = estimate_elasticity(price_qty)
        eps = est['epsilon']
        intercept = est['intercept']
        r2 = est['r2']
        npts = est['n_points']

        print(f"[{i:02d}] ε={eps:.3f} | intercept={intercept:.3f} | R²={r2 if not math.isnan(r2) else 'NA'} | points={npts} | {bundle_label}")

        # Save demand-curve plot if enough points
        plot_path = ""
        if npts >= 2:
            df_with_logs = est['df']  # contains Combined_Price, Num_Transactions, log_P, log_Q
            fname  = f"ped_demandcurve_{i:02d}_{slugify(name_a,40)}__{slugify(name_b,40)}.png"
            out_png = plots_dir / fname
            plot_demand_curve(df_with_logs, bundle_label, eps, intercept, out_png)
            plot_path = str(out_png)

        rows.append({
            'rule_row': i,
            'product_id_1': id_a,
            'product_id_2': id_b,
            'product_name_1': name_a,
            'product_name_2': name_b,
            'mode': "strict" if STRICT_BUNDLE_ONLY else "non_strict",
            'n_price_points': npts,
            'elasticity_epsilon': (None if math.isnan(eps) else round(float(eps), 6)),
            'intercept_logk': (None if math.isnan(intercept) else round(float(intercept), 6)),
            'r2_logspace': (None if math.isnan(r2) else round(float(r2), 6)),
            'plot_path': plot_path
        })

    if not rows:
        print("No bundles processed. Nothing to write.")
        sys.exit(0)

    # Save CSV
    out_csv = out_dir / 'ped_summary.csv'
    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding='utf-8')
    print(f"\nCSV saved: {out_csv}")
    print(f"Plots (if any) in: {plots_dir}")

if __name__ == '__main__':
    main()
