"""
Non-Linear Programming (NLP) for Bundle Price Optimization
Uses elasticity of demand from PED analysis to optimize bundle pricing
while respecting COGS constraints and minimum discount requirements.
"""

import os
import sys
import math
import pandas as pd
import numpy as np
from scipy.optimize import minimize

# =========================
# USER CONFIGURATION
# =========================

# Default COGS multiplier (60% of current price) when COGS column is missing
DEFAULT_COGS_MULTIPLIER = 0.60

# Minimum discount percentage (e.g., 0.1 means 10% discount minimum)
MIN_DISCOUNT_PCT = 0.10

# =========================
# Helpers
# =========================


def require_columns(df: pd.DataFrame, cols: list[str], name: str):
    """Check that required columns exist in dataframe"""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"Error: {name} is missing columns: {missing}")
        sys.exit(1)


def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        return float(value) if pd.notna(value) else default
    except (ValueError, TypeError):
        return default


def get_product_info(product_df: pd.DataFrame, product_id: str) -> dict:
    """Get product name, price, and COGS for a given product_id"""
    row = product_df[product_df['product_id'].astype(str) == str(product_id)]
    if row.empty:
        return None

    info = {
        'product_id': str(product_id),
        'product_name': str(row['product_name'].iloc[0]),
        'current_price': safe_float(row['Price'].iloc[0], 0.0)
    }

    # Check for product_cost column (from ETL) first, then COGS, then default
    if 'product_cost' in row.columns and pd.notna(row['product_cost'].iloc[0]):
        info['cogs'] = safe_float(row['product_cost'].iloc[0],
                                  info['current_price'] * DEFAULT_COGS_MULTIPLIER)
    elif 'COGS' in row.columns and pd.notna(row['COGS'].iloc[0]):
        info['cogs'] = safe_float(row['COGS'].iloc[0],
                                  info['current_price'] * DEFAULT_COGS_MULTIPLIER)
    else:
        info['cogs'] = info['current_price'] * DEFAULT_COGS_MULTIPLIER

    return info


def extract_product_ids_from_bundle(bundle_name: str) -> tuple:
    """
    Extract product IDs from bundle name by matching against product dimension.
    Bundle name format: "PRODUCT_A_NAME + PRODUCT_B_NAME"
    Returns: (id_a, id_b) or (None, None) if not found
    """
    if ' + ' not in bundle_name:
        return None, None

    parts = bundle_name.split(' + ')
    if len(parts) != 2:
        return None, None

    return parts[0].strip(), parts[1].strip()

# =========================
# Optimization Functions
# =========================


def objective_function(P, epsilon, K):
    """
    Objective: Maximize profit = (P - COGS_total) * Q
    where Q = K * P^epsilon (constant elasticity demand)

    For optimization, we minimize negative profit.
    """
    if P <= 0:
        return 1e10  # Large penalty for invalid price

    Q = K * (P ** epsilon)
    return -Q  # We want to maximize Q at optimal price, so minimize -Q


def optimize_bundle_price(bundle_data: dict) -> dict:
    """
    Optimize bundle price using NLP.

    Parameters from bundle_data:
    - epsilon: price elasticity of demand
    - K: base demand factor (exp(intercept))
    - cogs_total: combined COGS of products A and B
    - current_price_total: sum of individual current prices
    - min_price: minimum allowed price (COGS_total / (1 - MIN_DISCOUNT_PCT))
    - max_price: maximum allowed price (current_price_total)

    Returns optimized results dictionary
    """
    epsilon = bundle_data['epsilon']
    K = bundle_data['K']
    cogs_total = bundle_data['cogs_total']
    current_price_total = bundle_data['current_price_total']

    # Price constraints
    # Minimum price: must cover COGS and allow for minimum discount
    price_cap = current_price_total
    min_price = current_price_total * (1 - MIN_DISCOUNT_PCT)

    # For the constraint: P >= COGS_total + COGS_total * MIN_DISCOUNT_PCT
    # This ensures we make at least MIN_DISCOUNT_PCT margin
    cogs_constraint_min = cogs_total / \
        (1 - MIN_DISCOUNT_PCT) if MIN_DISCOUNT_PCT < 1 else cogs_total * 1.1

    # Use the more restrictive minimum
    min_price = max(min_price, cogs_constraint_min)

    # Constraints for scipy.optimize
    bounds = [(min_price, price_cap)]

    # Initial guess: midpoint between min and max
    P0 = (min_price + price_cap) / 2

    # Optimize
    result = minimize(
        lambda P: objective_function(P[0], epsilon, K),
        x0=[P0],
        method='L-BFGS-B',
        bounds=bounds
    )

    if not result.success:
        # Optimization failed, use midpoint as fallback
        optimal_price = P0
        quantity_demanded = K * (optimal_price ** epsilon)
        profit = (optimal_price - cogs_total) * quantity_demanded
    else:
        optimal_price = float(result.x[0])
        quantity_demanded = K * (optimal_price ** epsilon)
        profit = (optimal_price - cogs_total) * quantity_demanded

    return {
        'bundle_price_recommended': round(optimal_price, 2),
        'quantity_demanded': round(quantity_demanded, 4),
        'profit': round(profit, 2),
        'optimization_success': result.success if 'result' in locals() else False,
        'current_price_total': current_price_total,
        'cogs_total': round(cogs_total, 2),
        'price_cap': round(price_cap, 2),
        'min_discount_pct': MIN_DISCOUNT_PCT * 100
    }

# =========================
# Main Processing
# =========================


def main():
    print("=== Non-Linear Programming for Bundle Price Optimization ===")

    # Import loader
    from . import loader
    import time
    
    # Load data from PostgreSQL
    print("Loading data from PostgreSQL...")
    try:
        ped_df = loader.export_table_to_csv('ped_summary')
        product_df = loader.export_table_to_csv('current_product_dimension')
    except Exception as e:
        print(f"Error loading data: {e}")
        print(f"Make sure to run mba.py and ped.py first.")
        sys.exit(1)

    # Normalize column names
    ped_df.columns = [c.lower() for c in ped_df.columns]
    product_df.columns = [c.lower() for c in product_df.columns]
    
    # Sanity checks
    require_columns(ped_df, ['product_name_1', 'product_name_2', 'elasticity_epsilon',
                    'intercept_logk', 'r2_logspace', 'n_price_points'], 'PED Summary')
    require_columns(product_df, ['product_id', 'product_name', 'price'], 'Product Dimension')
    
    # Rename for compatibility with existing code
    product_df = product_df.rename(columns={'price': 'Price'})

    # Check if cost column exists
    has_cost_column = 'product_cost' in product_df.columns or 'COGS' in product_df.columns
    if not has_cost_column:
        print(f"Warning: 'product_cost' or 'COGS' column not found in product dimension.")
        print(
            f"Using default COGS = {DEFAULT_COGS_MULTIPLIER*100}% of current price.\n")
    elif 'product_cost' in product_df.columns:
        print(f"[INFO] Using 'product_cost' column from ETL output.\n")

    # Process each bundle from PED results
    results = []
    skipped = []

    for idx, row in ped_df.iterrows():
        # Use product_name_1 and product_name_2 from PED output
        name_a = str(row['product_name_1'])
        name_b = str(row['product_name_2'])
        bundle_name = f"{name_a} + {name_b}"

        bundle_id = str(row.get('bundle_id', f'B{idx+1:02d}'))
        category = str(row.get('category', 'UNKNOWN'))
        epsilon = safe_float(row['elasticity_epsilon'], 0.0)
        intercept = safe_float(row['intercept_logk'], 0.0)
        r2 = safe_float(row['r2_logspace'], float('nan'))
        n_points = int(row.get('n_price_points', 0))

        # Skip if insufficient data or invalid elasticity
        if n_points < 2 or math.isnan(r2) or epsilon >= 0:
            skipped.append({
                'bundle_id': bundle_id,
                'bundle_name': bundle_name,
                'reason': 'Insufficient data or invalid elasticity (ε >= 0)'
            })
            continue

        # Get product information by matching names directly
        prod_a_row = product_df[product_df['product_name'].astype(
            str) == name_a]
        prod_b_row = product_df[product_df['product_name'].astype(
            str) == name_b]

        if prod_a_row.empty or prod_b_row.empty:
            skipped.append({
                'bundle_id': bundle_id,
                'bundle_name': bundle_name,
                'reason': 'Product(s) not found in dimension'
            })
            continue

        info_a = get_product_info(product_df, prod_a_row['product_id'].iloc[0])
        info_b = get_product_info(product_df, prod_b_row['product_id'].iloc[0])

        if not info_a or not info_b:
            skipped.append({
                'bundle_id': bundle_id,
                'bundle_name': bundle_name,
                'reason': 'Could not retrieve product info'
            })
            continue

        # Calculate totals
        cogs_total = info_a['cogs'] + info_b['cogs']
        current_price_total = info_a['current_price'] + info_b['current_price']

        # Base demand factor K = exp(intercept)
        K = np.exp(intercept)

        # Prepare bundle data for optimization
        bundle_data = {
            'epsilon': epsilon,
            'K': K,
            'cogs_total': cogs_total,
            'current_price_total': current_price_total
        }

        # Optimize
        opt_result = optimize_bundle_price(bundle_data)

        # Compile results
        result_row = {
            'bundle_id': bundle_id,
            'bundle_name': bundle_name,
            'category': category,
            'product_a': info_a['product_name'],
            'product_b': info_b['product_name'],
            'product_a_price': info_a['current_price'],
            'product_b_price': info_b['current_price'],
            'current_price_total': current_price_total,
            'product_a_cogs': round(info_a['cogs'], 2),
            'product_b_cogs': round(info_b['cogs'], 2),
            'cogs_total': opt_result['cogs_total'],
            'elasticity_epsilon': round(epsilon, 6),
            'base_demand_K': round(K, 6),
            'r_squared': round(r2, 6) if not math.isnan(r2) else 'N/A',
            'n_points': n_points,
            'bundle_price_recommended': opt_result['bundle_price_recommended'],
            'quantity_demanded': opt_result['quantity_demanded'],
            'profit': opt_result['profit'],
            'price_cap': opt_result['price_cap'],
            'min_discount_pct': opt_result['min_discount_pct'],
            'optimization_success': opt_result['optimization_success']
        }

        results.append(result_row)

        print(f"[{bundle_id}] {bundle_name}")
        print(
            f"  Current Total: {current_price_total:.2f} | COGS Total: {opt_result['cogs_total']:.2f}")
        print(
            f"  Recommended Price: {opt_result['bundle_price_recommended']:.2f}")
        print(
            f"  Expected Demand: {opt_result['quantity_demanded']:.4f} | Profit: {opt_result['profit']:.2f}")
        print()

    # Save results
    if results:
        results_df = pd.DataFrame(results)
        results_df.columns = [col.replace(' ', '_').lower() for col in results_df.columns]
        
        # Upload to MinIO staging and PostgreSQL
        print("\nUploading results to MinIO and PostgreSQL...")
        csv_bytes = results_df.to_csv(index=False).encode('utf-8')
        
        # Upload to MinIO
        run_id = time.strftime("%Y%m%d_%H%M%S")
        from app.core.config import settings
        minio_path = f"models/nlp/{run_id}/nlp_optimization_results.csv"
        loader.staging_put_bytes(minio_path, csv_bytes)
        print(f"Uploaded to MinIO: {minio_path}")
        
        # Clear and load to PostgreSQL
        loader.clear_result_table('nlp_optimization_results')
        loader.load_result_csv_to_table(csv_bytes, 'nlp_optimization_results')
        print("Loaded to PostgreSQL: nlp_optimization_results")
        print(f"  Total bundles optimized: {len(results)}")
        
        # Clean up MinIO staging after successful load
        loader.staging_delete_prefix(f"models/nlp/{run_id}")
        print(f"Cleaned up MinIO staging: models/nlp/{run_id}")
    else:
        print("\nNo bundles were successfully optimized.")

    if skipped:
        print(f"\n  Total bundles skipped: {len(skipped)}")

    print("\n=== NLP Optimization Complete ===")


if __name__ == '__main__':
    main()
