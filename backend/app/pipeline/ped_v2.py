import sys
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

# =========================
# USER CONFIGURATION
# =========================
SAVE_FOLDER = 'mba_output'
RULES_PATH   = SAVE_FOLDER + '/association_rules.csv'
FACT_PATH    = 'etl_dimensions/fact_transaction_dimension.csv'
PRODUCT_PATH = 'etl_dimensions/current_product_dimension.csv'
TOP_N        = 15
OUT_DIR      = SAVE_FOLDER + '/ped_output'

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
    """EXACT product_name match. Returns (id_a, id_b) or (None, None) if not found."""
    nameA = rule_row.get('antecedents_names', None)
    nameB = rule_row.get('consequents_names', None)
    if nameA is None or nameB is None:
        return None, None
    a = product_df.loc[product_df['product_name'] == str(nameA), 'product_id']
    b = product_df.loc[product_df['product_name'] == str(nameB), 'product_id']
    if a.empty or b.empty:
        return None, None
    return str(a.iloc[0]), str(b.iloc[0])

def safe_name(product_df: pd.DataFrame, product_id: str) -> str:
    row = product_df.loc[product_df['product_id'].astype(str) == str(product_id), 'product_name']
    return str(row.iloc[0]) if not row.empty else f"(id:{product_id})"

# ---------- Price–Quantity builders ----------
def build_price_qty_points(fact_df: pd.DataFrame, id_a: str, id_b: str, strict: bool = False) -> pd.DataFrame:
    """
    Build price-quantity points for bundle (A, B).
    If strict=True: only receipts with exactly {A, B}.
    If strict=False: receipts containing both A and B (may include other items).
    Returns: DataFrame[Combined_Price, Num_Transactions]
    """
    ida, idb = str(id_a), str(id_b)
    
    if strict:
        # Build product set for every receipt
        receipt_sets = fact_df.groupby('Receipt No')['Product ID'].apply(lambda s: set(s.astype(str)))
        target_receipts = receipt_sets[receipt_sets == {ida, idb}].index
    else:
        # Find receipts containing both A and B
        pair_lines = fact_df[fact_df['Product ID'].astype(str).isin([ida, idb])]
        receipt_products = pair_lines.groupby('Receipt No')['Product ID'].apply(lambda s: set(s.astype(str)))
        target_receipts = receipt_products[receipt_products.apply(lambda s: ida in s and idb in s)].index
    
    if len(target_receipts) == 0:
        return pd.DataFrame(columns=['Combined_Price', 'Num_Transactions'])
    
    # Get only A/B lines from target receipts
    ab_lines = fact_df[
        fact_df['Receipt No'].isin(target_receipts) &
        fact_df['Product ID'].astype(str).isin([ida, idb])
    ]
    
    # Aggregate by receipt
    price_qty = (ab_lines.groupby('Receipt No')['Line Total']
                 .sum()
                 .reset_index()
                 .rename(columns={'Line Total': 'Combined_Price'})
                 .groupby('Combined_Price')
                 .size()
                 .reset_index(name='Num_Transactions')
                 .sort_values('Combined_Price'))
    
    return price_qty

# ---------- Estimation ----------
def estimate_elasticity(price_qty_df: pd.DataFrame) -> dict:
    """
    Estimate constant-elasticity via log–log regression:
      log_Q = intercept + ε * log_P
    Returns dict with epsilon, intercept, r2, n_points.
    """
    df = price_qty_df[(price_qty_df['Combined_Price'] > 0) &
                      (price_qty_df['Num_Transactions'] > 0)].copy()
    
    n = len(df)
    if n < 2:
        return {'epsilon': 0.0, 'intercept': 0.0, 'r2': float('nan'), 'n_points': n}
    
    X = np.log(df['Combined_Price'].to_numpy()).reshape(-1, 1)
    y = np.log(df['Num_Transactions'].to_numpy())
    
    reg = LinearRegression().fit(X, y)
    y_hat = reg.predict(X)
    
    return {
        'epsilon': float(reg.coef_[0]),
        'intercept': float(reg.intercept_),
        'r2': float(r2_score(y, y_hat)),
        'n_points': n
    }

# =========================
# Main
# =========================
def main():
    # Load data
    try:
        rules_df   = pd.read_csv(RULES_PATH)
        fact_df    = pd.read_csv(FACT_PATH)
        product_df = pd.read_csv(PRODUCT_PATH)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Validate columns
    require_columns(rules_df,   ['antecedents_names', 'consequents_names'], 'RULES file')
    require_columns(fact_df,    ['Product ID', 'Receipt No', 'Line Total', 'Date'], 'FACT file')
    require_columns(product_df, ['product_id', 'product_name', 'Price'], 'PRODUCT file')
    
    # Create output directory
    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    mode = "STRICT {A,B} only" if STRICT_BUNDLE_ONLY else "Receipts containing A and B (may include others)"
    print(f"=== PED Summary for TOP {min(TOP_N, len(rules_df))} bundles ===")
    print(f"Mode: {mode}\n")
    
    # Process rules
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
        
        # Get bundle metadata
        bundle_id = rule.get('bundle_id', "")
        category = rule.get('category', "")
        
        # Build price-quantity points
        price_qty = build_price_qty_points(fact_df, id_a, id_b, strict=STRICT_BUNDLE_ONLY)
        
        # Estimate elasticity
        est = estimate_elasticity(price_qty)
        eps, intercept, r2, npts = est['epsilon'], est['intercept'], est['r2'], est['n_points']
        
        r2_display = f"{r2:.4f}" if not math.isnan(r2) else "NA"
        print(f"[{i:02d}] ε={eps:.3f} | intercept={intercept:.3f} | R²={r2_display} | points={npts} | {bundle_label}")
        
        rows.append({
            'bundle_id': bundle_id,
            'category': category,
            'rule_row': i,
            'product_id_1': id_a,
            'product_id_2': id_b,
            'product_name_1': name_a,
            'product_name_2': name_b,
            'mode': "strict" if STRICT_BUNDLE_ONLY else "non_strict",
            'n_price_points': npts,
            'elasticity_epsilon': None if math.isnan(eps) else round(float(eps), 6),
            'intercept_logk': None if math.isnan(intercept) else round(float(intercept), 6),
            'r2_logspace': None if math.isnan(r2) else round(float(r2), 6)
        })
    
    if not rows:
        print("No bundles processed. Nothing to write.")
        sys.exit(0)
    
    # Save results
    out_csv = out_dir / 'ped_summary.csv'
    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding='utf-8')
    print(f"\nCSV saved: {out_csv}")

if __name__ == '__main__':
    main()