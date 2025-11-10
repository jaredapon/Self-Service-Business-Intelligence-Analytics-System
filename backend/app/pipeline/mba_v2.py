# Market Basket Analysis - Properly Optimized Version
# Used FP-Growth algorithm to find frequent itemsets and association rules

import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules
import os
import time

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.max_colwidth', 500)

start_time = time.time()
print("Loading transaction data...")
df = pd.read_csv('etl_dimensions/transaction_records.csv')

# Load product dimension
print("Loading product dimension...")
prod_dim = pd.read_csv('etl_dimensions/current_product_dimension.csv', dtype={'product_id': str})

# Create lookup dictionaries (OPTIMIZED: done once)
id_to_name = dict(zip(prod_dim['product_id'], prod_dim['product_name']))

# SKU to product_id mapping (OPTIMIZED: vectorized where possible)
print("Creating SKU mappings...")
sku_to_product_id = {}
for _, r in prod_dim.iterrows():
    product_id = str(r['product_id']).strip()
    sku_to_product_id[product_id] = product_id
    parent_val = str(r.get('parent_sku', '') or '')
    if parent_val:
        for part in parent_val.split(','):
            part_clean = part.strip()
            if part_clean:
                sku_to_product_id[part_clean] = product_id

# Map SKUs to product_ids
print("Mapping SKUs to product_ids...")
def map_skus_to_product_ids(sku_string):
    if not isinstance(sku_string, str):
        return ''
    product_ids = {sku_to_product_id.get(s.strip()) for s in sku_string.split(',')}
    return ','.join(sorted([pid for pid in product_ids if pid]))

df['product_ids'] = df['SKU'].apply(map_skus_to_product_ids)

# Get category sets
excluded_cats = {'EXTRA', 'OTHERS'}
excluded_tokens = set(prod_dim[prod_dim['CATEGORY'].isin(excluded_cats)]['product_id'].astype(str).str.strip())
food_ids = set(prod_dim[prod_dim['CATEGORY'] == 'FOOD']['product_id'].astype(str).str.strip())
drink_ids = set(prod_dim[prod_dim['CATEGORY'] == 'DRINK']['product_id'].astype(str).str.strip())

print(f"Excluding {len(excluded_tokens)} product_ids from categories: {excluded_cats}")

# OPTIMIZED: Shared helper function for translation
def translate_ids_to_names(id_string):
    if not isinstance(id_string, str):
        return ''
    names = [id_to_name.get(id.strip(), id.strip()) for id in id_string.split(',')]
    return ', '.join(names)

# OPTIMIZED: Shared function to remove reversed duplicates
def remove_reversed_rule_duplicates(df):
    if df.empty:
        return df
    seen = set()
    keep_rows = []
    for _, row in df.iterrows():
        a = row['antecedents_names']
        c = row['consequents_names']
        key = tuple(sorted([a, c]))
        if key not in seen:
            seen.add(key)
            keep_rows.append(row)
    return pd.DataFrame(keep_rows)

def run_mba_for_category(category_name, output_folder, all_rules):
    print(f"\n--- Running MBA for category: {category_name} ---")
    os.makedirs(output_folder, exist_ok=True)

    # Filter product dimension for the target category
    cat_rows = prod_dim[prod_dim['CATEGORY'] == category_name]
    cat_product_ids = set(cat_rows['product_id'].astype(str).str.strip())

    # Filter transactions
    def transaction_has_cat_product_ids(pid_string):
        if not isinstance(pid_string, str):
            return False
        return any(pid.strip() in cat_product_ids for pid in pid_string.split(','))

    df_cat = df[df['product_ids'].apply(transaction_has_cat_product_ids)].copy()

    if df_cat.empty:
        print(f"No transactions found for category {category_name}.")
        return all_rules

    print("One hot encoding...")
    one_hot_cat = df_cat['product_ids'].astype(str).str.get_dummies(sep=',')
    one_hot_cat.columns = [c.strip() for c in one_hot_cat.columns]
    one_hot_cat = one_hot_cat.astype(bool)

    print("Running FP-Growth algorithm...")
    frequent_itemsets_cat = fpgrowth(one_hot_cat, min_support=0.003, use_colnames=True)

    if frequent_itemsets_cat.empty:
        print("No frequent itemsets found.")
        association_rules_export = pd.DataFrame(columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
    else:
        rules_cat = association_rules(frequent_itemsets_cat, metric="confidence", min_threshold=0.15)
        rules_cat = rules_cat[(rules_cat['lift'] >= 1)].sort_values(['confidence', 'lift'], ascending=[False, False])

        # Only keep single item rules
        rules_cat = rules_cat[rules_cat['antecedents'].apply(lambda x: len(x) == 1) & rules_cat['consequents'].apply(lambda x: len(x) == 1)]

        # Only keep rules where all items are in the category
        def rule_all_in_cat(fset):
            return all(str(tok) in cat_product_ids for tok in fset)

        rules_filtered_cat = rules_cat[rules_cat['antecedents'].apply(rule_all_in_cat) & rules_cat['consequents'].apply(rule_all_in_cat)].copy()

        if rules_filtered_cat.empty:
            print("No association rules found for category.")
            association_rules_export = pd.DataFrame(columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
        else:
            # OPTIMIZED: Store max_support once
            max_support = rules_filtered_cat['support'].max()
            rules_filtered_cat['combined_score'] = (rules_filtered_cat['lift'] * 0.7) + (rules_filtered_cat['support'] / max_support * 30)
            rules_filtered_cat = rules_filtered_cat.sort_values('combined_score', ascending=False)
            rules_filtered_cat['antecedents_sku'] = rules_filtered_cat['antecedents'].apply(lambda s: ', '.join(sorted(s)))
            rules_filtered_cat['consequents_sku'] = rules_filtered_cat['consequents'].apply(lambda s: ', '.join(sorted(s)))
            association_rules_export = rules_filtered_cat[['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'combined_score']].copy()

    # Translate IDs to Names
    association_rules_export.rename(columns={'antecedents_sku': 'antecedents_names', 'consequents_sku': 'consequents_names'}, inplace=True)
    if not association_rules_export.empty:
        association_rules_export['antecedents_names'] = association_rules_export['antecedents_names'].apply(translate_ids_to_names)
        association_rules_export['consequents_names'] = association_rules_export['consequents_names'].apply(translate_ids_to_names)
    association_rules_export['category'] = category_name

    # Remove reversed duplicates
    association_rules_export = remove_reversed_rule_duplicates(association_rules_export)

    # Limit to top 5
    if not association_rules_export.empty and {'combined_score', 'confidence', 'lift'}.issubset(association_rules_export.columns):
        association_rules_export = association_rules_export.sort_values(['combined_score', 'confidence', 'lift'], ascending=[False, False, False]).head(5).reset_index(drop=True)

    # Add bundle id
    bundle_prefix = 'BF' if category_name == 'FOOD' else 'BD'
    association_rules_export['bundle_id'] = [f"{bundle_prefix}{str(i+1).zfill(2)}" for i in range(len(association_rules_export))]
    
    if not association_rules_export.empty:
        cols = association_rules_export.columns.tolist()
        cols.insert(0, cols.pop(cols.index('bundle_id')))
        association_rules_export = association_rules_export[cols]

    # Append to all results
    all_rules = pd.concat([all_rules, association_rules_export], ignore_index=True)

    return all_rules

def run_mba_for_meal(output_folder, all_rules):
    print(f"\n--- Running MBA for MEAL (FOOD <-> DRINK) ---")
    os.makedirs(output_folder, exist_ok=True)

    # Only keep transactions that have at least one FOOD and one DRINK item
    def has_food_and_drink(pid_string):
        if not isinstance(pid_string, str):
            return False
        pids = set(pid.strip() for pid in pid_string.split(','))
        return bool(pids & food_ids) and bool(pids & drink_ids)

    df_meal = df[df['product_ids'].apply(has_food_and_drink)].copy()
    if df_meal.empty:
        print("No transactions found with both FOOD and DRINK.")
        return all_rules

    print("One hot encoding...")
    one_hot_meal = df_meal['product_ids'].astype(str).str.get_dummies(sep=',')
    one_hot_meal.columns = [c.strip() for c in one_hot_meal.columns]
    one_hot_meal = one_hot_meal.astype(bool)

    print("Running FP-Growth algorithm...")
    frequent_itemsets_meal = fpgrowth(one_hot_meal, min_support=0.003, use_colnames=True)

    # Generate rules
    rules_meal = association_rules(frequent_itemsets_meal, metric="confidence", min_threshold=0.15)
    rules_meal = rules_meal[(rules_meal['lift'] >= 1)].sort_values(['confidence', 'lift'], ascending=[False, False])

    # Only keep single item rules
    rules_meal = rules_meal[rules_meal['antecedents'].apply(lambda x: len(x) == 1) & rules_meal['consequents'].apply(lambda x: len(x) == 1)]

    def is_meal_rule(antecedents, consequents):
        a = set(str(tok) for tok in antecedents)
        c = set(str(tok) for tok in consequents)
        return ((a <= food_ids and c <= drink_ids) or (a <= drink_ids and c <= food_ids))

    rules_filtered_meal = rules_meal[rules_meal.apply(lambda row: is_meal_rule(row['antecedents'], row['consequents']), axis=1)].copy()

    if rules_filtered_meal.empty:
        print("No meal association rules found.")
        association_rules_export = pd.DataFrame(columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
    else:
        # OPTIMIZED: Store max_support once
        max_support = rules_filtered_meal['support'].max()
        rules_filtered_meal['combined_score'] = (rules_filtered_meal['lift'] * 0.7) + (rules_filtered_meal['support'] / max_support * 30)
        rules_filtered_meal = rules_filtered_meal.sort_values('combined_score', ascending=False)
        rules_filtered_meal['antecedents_sku'] = rules_filtered_meal['antecedents'].apply(lambda s: ', '.join(sorted(s)))
        rules_filtered_meal['consequents_sku'] = rules_filtered_meal['consequents'].apply(lambda s: ', '.join(sorted(s)))
        association_rules_export = rules_filtered_meal[['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'combined_score']].copy()

    # Translate IDs to Names
    association_rules_export.rename(columns={'antecedents_sku': 'antecedents_names', 'consequents_sku': 'consequents_names'}, inplace=True)
    if not association_rules_export.empty:
        association_rules_export['antecedents_names'] = association_rules_export['antecedents_names'].apply(translate_ids_to_names)
        association_rules_export['consequents_names'] = association_rules_export['consequents_names'].apply(translate_ids_to_names)
    association_rules_export['category'] = 'MEAL'

    # Remove reversed duplicates
    association_rules_export = remove_reversed_rule_duplicates(association_rules_export)

    # Limit to top 5
    if not association_rules_export.empty and {'combined_score', 'confidence', 'lift'}.issubset(association_rules_export.columns):
        association_rules_export = association_rules_export.sort_values(['combined_score', 'confidence', 'lift'], ascending=[False, False, False]).head(5).reset_index(drop=True)

    # Add bundle id
    association_rules_export['bundle_id'] = [f"BM{str(i+1).zfill(2)}" for i in range(len(association_rules_export))]
    
    if not association_rules_export.empty:
        cols = association_rules_export.columns.tolist()
        cols.insert(0, cols.pop(cols.index('bundle_id')))
        association_rules_export = association_rules_export[cols]

    # Append to all results
    all_rules = pd.concat([all_rules, association_rules_export], ignore_index=True)

    return all_rules

# Main execution
if 'prod_dim' in locals():
    output_folder = 'mba_output'
    os.makedirs(output_folder, exist_ok=True)
    all_rules = pd.DataFrame()
    
    all_rules = run_mba_for_category('FOOD', output_folder, all_rules)
    all_rules = run_mba_for_category('DRINK', output_folder, all_rules)
    all_rules = run_mba_for_meal(output_folder, all_rules)

    association_rules_csv_path = os.path.join(output_folder, 'association_rules.csv')
    all_rules.to_csv(association_rules_csv_path, index=False)

    print(f"\nResults exported successfully to {output_folder}")
else:
    print("Product dimension not loaded; cannot run category-specific MBA.")

end_time = time.time()
print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")