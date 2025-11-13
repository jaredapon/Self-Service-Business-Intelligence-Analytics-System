# Market Basket Analysis - Properly Optimized Version
# Used FP-Growth algorithm to find frequent itemsets and association rules

import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules
import os
import time

# --- HELPER FUNCTIONS ---

def translate_ids_to_names(id_string, id_to_name_map):
    """Translates a comma-separated string of product IDs to product names."""
    if not isinstance(id_string, str):
        return ''
    names = [id_to_name_map.get(id.strip(), id.strip()) for id in id_string.split(',')]
    return ', '.join(names)

def remove_reversed_rule_duplicates(df):
    """Removes duplicate rules where (A -> C) and (C -> A) exist."""
    if df.empty:
        return df
    
    # Create a key for each rule pair (A, C) -> sorted(A, C)
    rule_key = df.apply(lambda row: tuple(sorted([
        row['antecedents_names'], 
        row['consequents_names']
    ])), axis=1)
    
    # Keep the first occurrence of each unique key
    return df[~rule_key.duplicated(keep='first')].copy()

def map_skus_to_product_ids(sku_string, sku_to_product_id_map):
    """Maps a comma-separated SKU string to a sorted product ID string."""
    if not isinstance(sku_string, str):
        return ''
    # Use .get() for efficient and safe lookup
    product_ids = {sku_to_product_id_map.get(s.strip()) for s in sku_string.split(',')}
    # Filter out None values if a SKU wasn't in the map
    return ','.join(sorted([pid for pid in product_ids if pid]))

# --- MBA ANALYSIS FUNCTIONS ---

def run_mba_for_category(df, category_name, cat_product_ids, id_to_name_map, all_rules):
    """Runs the full MBA process for a single product category."""
    print(f"\n--- Running MBA for category: {category_name} ---")

    # OPTIMIZED: Vectorized transaction filtering (no .apply)
    print("Filtering transactions for category (vectorized)...")
    s_exploded = df['product_ids'].astype(str).str.split(',').explode()
    s_in_cat = s_exploded.str.strip().isin(cat_product_ids)
    
    # Group by original index (level=0) and check if .any() item was in the category
    mask = s_in_cat.groupby(level=0).any()
    df_cat = df[mask].copy()

    if df_cat.empty:
        print(f"No transactions found for category {category_name}.")
        return all_rules
    
    print(f"Found {len(df_cat)} transactions for {category_name}.")

    print("One hot encoding...")
    one_hot_cat = df_cat['product_ids'].astype(str).str.get_dummies(sep=',')
    one_hot_cat.columns = [c.strip() for c in one_hot_cat.columns]
    # Filter columns that are not in the category
    cols_to_keep = [col for col in one_hot_cat.columns if col in cat_product_ids]
    one_hot_cat = one_hot_cat[cols_to_keep]
    one_hot_cat = one_hot_cat.astype(bool)

    print("Running FP-Growth algorithm...")
    frequent_itemsets_cat = fpgrowth(one_hot_cat, min_support=0.003, use_colnames=True)

    if frequent_itemsets_cat.empty:
        print("No frequent itemsets found.")
        association_rules_export = pd.DataFrame()
    else:
        rules_cat = association_rules(frequent_itemsets_cat, metric="confidence", min_threshold=0.15)
        rules_cat = rules_cat[(rules_cat['lift'] >= 1)].sort_values(['confidence', 'lift'], ascending=[False, False])
        rules_cat = rules_cat[rules_cat['antecedents'].apply(lambda x: len(x) == 1) & rules_cat['consequents'].apply(lambda x: len(x) == 1)]

        # Filter to rules where all items are in the category (safeguard)
        rules_filtered_cat = rules_cat[
            rules_cat['antecedents'].apply(lambda fset: fset.issubset(cat_product_ids)) & 
            rules_cat['consequents'].apply(lambda fset: fset.issubset(cat_product_ids))
        ].copy()

        if rules_filtered_cat.empty:
            print("No association rules found for category.")
            association_rules_export = pd.DataFrame()
        else:
            max_support = rules_filtered_cat['support'].max()
            rules_filtered_cat['combined_score'] = (rules_filtered_cat['lift'] * 0.7) + (rules_filtered_cat['support'] / max_support * 30)
            rules_filtered_cat = rules_filtered_cat.sort_values('combined_score', ascending=False)
            
            rules_filtered_cat['antecedents_sku'] = rules_filtered_cat['antecedents'].apply(lambda s: ', '.join(s))
            rules_filtered_cat['consequents_sku'] = rules_filtered_cat['consequents'].apply(lambda s: ', '.join(s))
            
            association_rules_export = rules_filtered_cat[['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'combined_score']].copy()

    # Create default empty dataframe with correct columns if no rules were found
    if association_rules_export.empty:
        association_rules_export = pd.DataFrame(columns=['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'combined_score'])

    # Translate IDs to Names
    association_rules_export.rename(columns={'antecedents_sku': 'antecedents_names', 'consequents_sku': 'consequents_names'}, inplace=True)
    if not association_rules_export.empty:
        association_rules_export['antecedents_names'] = association_rules_export['antecedents_names'].apply(lambda x: translate_ids_to_names(x, id_to_name_map))
        association_rules_export['consequents_names'] = association_rules_export['consequents_names'].apply(lambda x: translate_ids_to_names(x, id_to_name_map))
    association_rules_export['category'] = category_name

    # Remove reversed duplicates
    association_rules_export = remove_reversed_rule_duplicates(association_rules_export)

    # Limit to top 5
    if not association_rules_export.empty:
        association_rules_export = association_rules_export.sort_values(['combined_score', 'confidence', 'lift'], ascending=[False, False, False]).head(5).reset_index(drop=True)

    # Add bundle id
    bundle_prefix = 'BF' if category_name == 'FOOD' else 'BD'
    association_rules_export['bundle_id'] = [f"{bundle_prefix}{str(i+1).zfill(2)}" for i in range(len(association_rules_export))]
    
    if not association_rules_export.empty:
        cols = association_rules_export.columns.tolist()
        cols.insert(0, cols.pop(cols.index('bundle_id')))
        association_rules_export = association_rules_export[cols]

    all_rules = pd.concat([all_rules, association_rules_export], ignore_index=True)
    return all_rules

def run_mba_for_meal(df, food_ids, drink_ids, id_to_name_map, all_rules):
    """Runs the full MBA process for MEAL (Food <-> Drink) combinations."""
    print(f"\n--- Running MBA for MEAL (FOOD <-> DRINK) ---")

    # OPTIMIZED: Vectorized transaction filtering (no .apply)
    print("Filtering transactions for MEAL (vectorized)...")
    s_exploded = df['product_ids'].astype(str).str.split(',').explode().str.strip()
    
    # Check for food and drink presence separately, then combine
    has_food = s_exploded.isin(food_ids).groupby(level=0).any()
    has_drink = s_exploded.isin(drink_ids).groupby(level=0).any()
    
    mask = has_food & has_drink
    df_meal = df[mask].copy()
    
    if df_meal.empty:
        print("No transactions found with both FOOD and DRINK.")
        return all_rules

    print(f"Found {len(df_meal)} MEAL transactions.")
    
    print("One hot encoding...")
    one_hot_meal = df_meal['product_ids'].astype(str).str.get_dummies(sep=',')
    one_hot_meal.columns = [c.strip() for c in one_hot_meal.columns]
    
    # Keep only food and drink columns
    cols_to_keep = [col for col in one_hot_meal.columns if col in food_ids or col in drink_ids]
    one_hot_meal = one_hot_meal[cols_to_keep]
    one_hot_meal = one_hot_meal.astype(bool)

    print("Running FP-Growth algorithm...")
    frequent_itemsets_meal = fpgrowth(one_hot_meal, min_support=0.003, use_colnames=True)

    if frequent_itemsets_meal.empty:
        print("No frequent itemsets found for MEAL.")
        association_rules_export = pd.DataFrame()
    else:
        rules_meal = association_rules(frequent_itemsets_meal, metric="confidence", min_threshold=0.15)
        rules_meal = rules_meal[(rules_meal['lift'] >= 1)].sort_values(['confidence', 'lift'], ascending=[False, False])
        rules_meal = rules_meal[rules_meal['antecedents'].apply(lambda x: len(x) == 1) & rules_meal['consequents'].apply(lambda x: len(x) == 1)]

        # Use efficient issubset check
        def is_meal_rule(antecedents, consequents):
            a_is_food = antecedents.issubset(food_ids)
            a_is_drink = antecedents.issubset(drink_ids)
            c_is_food = consequents.issubset(food_ids)
            c_is_drink = consequents.issubset(drink_ids)
            return (a_is_food and c_is_drink) or (a_is_drink and c_is_food)

        rules_filtered_meal = rules_meal[rules_meal.apply(lambda row: is_meal_rule(row['antecedents'], row['consequents']), axis=1)].copy()

        if rules_filtered_meal.empty:
            print("No meal association rules found.")
            association_rules_export = pd.DataFrame()
        else:
            max_support = rules_filtered_meal['support'].max()
            rules_filtered_meal['combined_score'] = (rules_filtered_meal['lift'] * 0.7) + (rules_filtered_meal['support'] / max_support * 30)
            rules_filtered_meal = rules_filtered_meal.sort_values('combined_score', ascending=False)
            
            rules_filtered_meal['antecedents_sku'] = rules_filtered_meal['antecedents'].apply(lambda s: ', '.join(s))
            rules_filtered_meal['consequents_sku'] = rules_filtered_meal['consequents'].apply(lambda s: ', '.join(s))
            
            association_rules_export = rules_filtered_meal[['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'combined_score']].copy()

    # Create default empty dataframe with correct columns if no rules were found
    if association_rules_export.empty:
        association_rules_export = pd.DataFrame(columns=['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'combined_score'])

    # Translate IDs to Names
    association_rules_export.rename(columns={'antecedents_sku': 'antecedents_names', 'consequents_sku': 'consequents_names'}, inplace=True)
    if not association_rules_export.empty:
        association_rules_export['antecedents_names'] = association_rules_export['antecedents_names'].apply(lambda x: translate_ids_to_names(x, id_to_name_map))
        association_rules_export['consequents_names'] = association_rules_export['consequents_names'].apply(lambda x: translate_ids_to_names(x, id_to_name_map))
    association_rules_export['category'] = 'MEAL'

    # Remove reversed duplicates
    association_rules_export = remove_reversed_rule_duplicates(association_rules_export)

    # Limit to top 5
    if not association_rules_export.empty:
        association_rules_export = association_rules_export.sort_values(['combined_score', 'confidence', 'lift'], ascending=[False, False, False]).head(5).reset_index(drop=True)

    # Add bundle id
    association_rules_export['bundle_id'] = [f"BM{str(i+1).zfill(2)}" for i in range(len(association_rules_export))]
    
    if not association_rules_export.empty:
        cols = association_rules_export.columns.tolist()
        cols.insert(0, cols.pop(cols.index('bundle_id')))
        association_rules_export = association_rules_export[cols]

    all_rules = pd.concat([all_rules, association_rules_export], ignore_index=True)
    return all_rules


# --- MAIN EXECUTION ---

def main():
    """
    Main function to run the complete Market Basket Analysis.
    """
    
    # --- Global Setup & Pre-processing ---
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 500)
    pd.set_option('display.max_colwidth', 500)

    start_time = time.time()
    
    print("Loading transaction data...")
    try:
        df = pd.read_csv('etl_dimensions/transaction_records.csv')
    except FileNotFoundError:
        print("Error: transaction_records.csv not found.")
        return

    print("Loading product dimension...")
    try:
        prod_dim = pd.read_csv('etl_dimensions/current_product_dimension.csv', dtype={'product_id': str})
    except FileNotFoundError:
        print("Error: current_product_dimension.csv not found.")
        return

    # Create lookup dictionaries (OPTIMIZED: done once)
    id_to_name = dict(zip(prod_dim['product_id'], prod_dim['product_name']))

    # SKU to product_id mapping (OPTIMIZED: vectorized, no iterrows)
    print("Creating SKU mappings (vectorized)...")
    prod_dim['product_id_clean'] = prod_dim['product_id'].str.strip()
    id_mapping = dict(zip(prod_dim['product_id_clean'], prod_dim['product_id_clean']))
    
    df_parent = prod_dim[prod_dim['parent_sku'].notna() & (prod_dim['parent_sku'] != '')][['product_id_clean', 'parent_sku']].copy()
    df_parent['parent_sku'] = df_parent['parent_sku'].str.split(',')
    df_parent = df_parent.explode('parent_sku')
    df_parent['parent_sku'] = df_parent['parent_sku'].str.strip()
    df_parent = df_parent[df_parent['parent_sku'] != '']
    parent_mapping = dict(zip(df_parent['parent_sku'], df_parent['product_id_clean']))
    
    sku_to_product_id = {**id_mapping, **parent_mapping}
    print(f"Created mapping for {len(sku_to_product_id)} unique SKUs.")

    # Map SKUs to product_ids
    print("Mapping SKUs to product_ids...")
    df['product_ids'] = df['SKU'].apply(lambda x: map_skus_to_product_ids(x, sku_to_product_id))

    # Get category sets
    excluded_cats = {'EXTRA', 'OTHERS'}
    excluded_tokens = set(prod_dim[prod_dim['CATEGORY'].isin(excluded_cats)]['product_id'].astype(str).str.strip())
    food_ids = set(prod_dim[prod_dim['CATEGORY'] == 'FOOD']['product_id'].astype(str).str.strip())
    drink_ids = set(prod_dim[prod_dim['CATEGORY'] == 'DRINK']['product_id'].astype(str).str.strip())

    print(f"Excluding {len(excluded_tokens)} product_ids from categories: {excluded_cats}")
    
    # --- Run Main Analysis ---
    output_folder = 'mba_output'
    os.makedirs(output_folder, exist_ok=True)
    all_rules = pd.DataFrame()
    
    # Get category-specific ID sets
    food_cat_ids = set(prod_dim[prod_dim['CATEGORY'] == 'FOOD']['product_id'].astype(str).str.strip())
    drink_cat_ids = set(prod_dim[prod_dim['CATEGORY'] == 'DRINK']['product_id'].astype(str).str.strip())
    
    # Run analysis
    all_rules = run_mba_for_category(df, 'FOOD', food_cat_ids, id_to_name, all_rules)
    all_rules = run_mba_for_category(df, 'DRINK', drink_cat_ids, id_to_name, all_rules)
    all_rules = run_mba_for_meal(df, food_ids, drink_ids, id_to_name, all_rules)

    # Save results (will be in snake_case)
    association_rules_csv_path = os.path.join(output_folder, 'association_rules.csv')
    all_rules.to_csv(association_rules_csv_path, index=False)

    print(f"\nResults exported successfully to {output_folder}")
    
    # --- Print Total Time ---
    end_time = time.time()
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()