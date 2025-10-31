# Market Basket Analysis
# Used FP-Growth algorithm to find frequent itemsets and association rules

import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules
import os

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.max_colwidth', 500)


# Create folder for mba_results
# results_folder = 'mba_results'
# os.makedirs(results_folder, exist_ok=True)

print("Loading transaction data...")
df = pd.read_csv('etl_dimensions/transaction_records.csv')

# Initialize with SKU as the default column for analysis
analysis_column = 'SKU'
excluded_tokens = set()

# Load product dimension to map SKUs to product_ids and identify excluded items
try:
    prod_dim = pd.read_csv('etl_dimensions/current_product_dimension.csv',
                           dtype={'product_id': str})

    # Create a lookup dictionary for product_id to product_name
    id_to_name = dict(zip(prod_dim['product_id'], prod_dim['product_name']))

    # --- SKU to product_id mapping ---
    sku_to_product_id = {}
    for _, r in prod_dim.iterrows():
        product_id = str(r['product_id']).strip()
        # Map the product_id to itself
        sku_to_product_id[product_id] = product_id
        # Map all parts of parent_sku to the product_id
        parent_val = str(r.get('parent_sku', '') or '')
        for part in parent_val.split(','):
            part_clean = part.strip()
            if part_clean:
                sku_to_product_id[part_clean] = product_id

    # --- Transform transaction SKUs to product_ids ---
    def map_skus_to_product_ids(sku_string):
        if not isinstance(sku_string, str):
            return ''
        # Map each SKU in the comma-separated string to a product_id
        # Use a set to handle duplicates, then join
        product_ids = {sku_to_product_id.get(
            s.strip()) for s in sku_string.split(',')}
        # Filter out any None values that resulted from unmapped SKUs
        return ','.join(sorted([pid for pid in product_ids if pid]))

    print("Mapping SKUs to product_ids...")
    df['product_ids'] = df['SKU'].apply(map_skus_to_product_ids)
    analysis_column = 'product_ids'  # Use product_ids for analysis

    # --- Identify product_ids to exclude from analysis ---
    excluded_cats = {'EXTRA', 'OTHERS'}
    excluded_rows = prod_dim[prod_dim['CATEGORY'].isin(excluded_cats)]
    excluded_tokens = set(excluded_rows['product_id'].astype(str).str.strip())
    print(
        f"Excluding {len(excluded_tokens)} product_ids from categories: {excluded_cats}")

except FileNotFoundError:
    print("Warning: etl_dimensions/current_product_dimension.csv not found. Using original SKU column and no category filtering.")
    # Fallback to original SKU column if mapping file is not found
    analysis_column = 'SKU'


print("One hot encoding...")

one_hot = df[analysis_column].astype(str).str.get_dummies(sep=',')
one_hot.columns = [c.strip() for c in one_hot.columns]
one_hot = one_hot.astype(bool)

print("Running FP-Growth algorithm...")
frequent_itemsets_fpgrowth = fpgrowth(
    one_hot, min_support=0.003, use_colnames=True)

# Filter out itemsets containing excluded product_ids


def itemset_has_excluded(itemset):
    return any(str(tok) in excluded_tokens for tok in itemset)


filtered_itemsets = frequent_itemsets_fpgrowth[~frequent_itemsets_fpgrowth['itemsets'].apply(
    itemset_has_excluded)].copy()

# Represent itemsets as comma-separated sorted list for readability
if not filtered_itemsets.empty:
    filtered_itemsets['itemsets_sku'] = filtered_itemsets['itemsets'].apply(
        lambda s: ', '.join(sorted(s)))
else:  # Handle case where filtered_itemsets is empty from the start
    frequent_itemsets_fpgrowth['itemsets_sku'] = frequent_itemsets_fpgrowth['itemsets'].apply(
        lambda s: ', '.join(sorted(s)))
    filtered_itemsets = frequent_itemsets_fpgrowth


if frequent_itemsets_fpgrowth.empty:
    print("No frequent itemsets found. Lower the support value.")
    print("No association rules can be generated because there are no frequent itemsets.")
    # Create empty dataframes for export
    frequent_itemsets_export = pd.DataFrame(
        columns=['support', 'itemsets_names'])
    association_rules_export = pd.DataFrame(
        columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
else:
    # Prepare frequent itemsets for export
    frequent_itemsets_export = filtered_itemsets[
        ['support', 'itemsets_sku']].copy()

    # Use the original, unfiltered itemsets for rule generation
    rules = association_rules(
        frequent_itemsets_fpgrowth, metric="confidence", min_threshold=0.15)  # changed from 0.1 to 0.15
    rules = rules[(rules['lift'] >= 1)].sort_values(
        ['confidence', 'lift'], ascending=[False, False])

    if rules.empty:
        print("No association rules found.")
        association_rules_export = pd.DataFrame(
            columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
    else:
        # Filter rules if any part contains an excluded product_id
        def rule_has_excluded(fset):
            return any(str(tok) in excluded_tokens for tok in fset)

        rules_filtered = rules[~rules['antecedents'].apply(rule_has_excluded) & ~rules['consequents'].apply(rule_has_excluded)].copy()

        if rules_filtered.empty:
            print("No association rules remain after category filtering.")
            association_rules_export = pd.DataFrame(
                columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
        else:
            # Remove duplicate pairs where antecedent and consequent are swapped
            def remove_duplicate_pairs(df):
                seen_pairs = set()
                unique_rows = []
                for _, row in df.iterrows():
                    pair = frozenset((tuple(row['antecedents']), tuple(row['consequents'])))
                    if pair not in seen_pairs:
                        seen_pairs.add(pair)
                        unique_rows.append(row)
                return pd.DataFrame(unique_rows)

            rules_filtered = remove_duplicate_pairs(rules_filtered)

            # Remove combined_score calculation and sorting
            # rules_filtered['combined_score'] = (rules_filtered['lift'] * 0.7) + (rules_filtered['support'] / rules_filtered['support'].max() * 30)
            # rules_filtered = rules_filtered.sort_values('combined_score', ascending=False)

            rules_filtered['antecedents_sku'] = rules_filtered['antecedents'].apply(
                lambda s: ', '.join(sorted(s)))
            rules_filtered['consequents_sku'] = rules_filtered['consequents'].apply(
                lambda s: ', '.join(sorted(s)))
            association_rules_export = rules_filtered[
                ['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction']
            ].copy()

# --- Translate IDs to Names for Final Output ---
if 'id_to_name' in locals():
    print("Translating product IDs to names for final report...")

    def translate_ids_to_names(id_string):
        if not isinstance(id_string, str):
            return ''
        names = [id_to_name.get(id.strip(), id.strip())
                 for id in id_string.split(',')]
        return ', '.join(names)

    # Translate frequent itemsets
    frequent_itemsets_export.rename(
        columns={'itemsets_sku': 'itemsets_names'}, inplace=True)
    frequent_itemsets_export['itemsets_names'] = frequent_itemsets_export['itemsets_names'].apply(
        translate_ids_to_names)

    # Translate association rules
    association_rules_export.rename(columns={
        'antecedents_sku': 'antecedents_names',
        'consequents_sku': 'consequents_names'
    }, inplace=True)
    if not association_rules_export.empty:
        association_rules_export['antecedents_names'] = association_rules_export['antecedents_names'].apply(
            translate_ids_to_names)
        association_rules_export['consequents_names'] = association_rules_export['consequents_names'].apply(
            translate_ids_to_names)


# Export to Excel with multiple tabs

# excel_file_path = os.path.join(
#     results_folder, 'market_basket_analysis_results.xlsx')
# with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
#     frequent_itemsets_export.to_excel(
#         writer, sheet_name='frequent_itemsets', index=False)
#     association_rules_export.to_excel(
#         writer, sheet_name='association_rules', index=False)
#
# # Export to CSV files
# frequent_itemsets_csv_path = os.path.join(
#     results_folder, 'frequent_itemsets.csv')
# association_rules_csv_path = os.path.join(
#     results_folder, 'association_rules.csv')
#
# frequent_itemsets_export.to_csv(frequent_itemsets_csv_path, index=False)
# association_rules_export.to_csv(association_rules_csv_path, index=False)
#
# print(f"\nResults exported successfully:")
# print(f"- Excel file: {excel_file_path}")
# print(f"- Frequent itemsets CSV: {frequent_itemsets_csv_path}")
# print(f"- Association rules CSV: {association_rules_csv_path}")
# print(f"- Total frequent itemsets: {len(frequent_itemsets_export)}")
# print(f"- Total association rules: {len(association_rules_export)}")

def run_mba_for_category(category_name, output_folder, all_itemsets, all_rules):
    print(f"\n--- Running MBA for category: {category_name} ---")
    os.makedirs(output_folder, exist_ok=True)

    # Filter product dimension for the target category
    cat_rows = prod_dim[prod_dim['CATEGORY'] == category_name]
    cat_product_ids = set(cat_rows['product_id'].astype(str).str.strip())

    # Filter transactions to only those containing at least one product_id in the category
    def transaction_has_cat_product_ids(pid_string):
        if not isinstance(pid_string, str):
            return False
        return any(pid.strip() in cat_product_ids for pid in pid_string.split(','))

    df_cat = df[df['product_ids'].apply(transaction_has_cat_product_ids)].copy()

    if df_cat.empty:
        print(f"No transactions found for category {category_name}.")
        return all_itemsets, all_rules

    print("One hot encoding...")
    one_hot_cat = df_cat['product_ids'].astype(str).str.get_dummies(sep=',')
    one_hot_cat.columns = [c.strip() for c in one_hot_cat.columns]
    one_hot_cat = one_hot_cat.astype(bool)

    print("Running FP-Growth algorithm...")
    frequent_itemsets_cat = fpgrowth(one_hot_cat, min_support=0.003, use_colnames=True)

    # Only keep itemsets where all items are in the category
    def itemset_all_in_cat(itemset):
        return all(str(tok) in cat_product_ids for tok in itemset)

    filtered_itemsets_cat = frequent_itemsets_cat[frequent_itemsets_cat['itemsets'].apply(itemset_all_in_cat)].copy()

    if not filtered_itemsets_cat.empty:
        filtered_itemsets_cat['itemsets_sku'] = filtered_itemsets_cat['itemsets'].apply(lambda s: ', '.join(sorted(s)))
    else:
        frequent_itemsets_cat['itemsets_sku'] = frequent_itemsets_cat['itemsets'].apply(lambda s: ', '.join(sorted(s)))
        filtered_itemsets_cat = frequent_itemsets_cat

    if frequent_itemsets_cat.empty:
        print("No frequent itemsets found. Lower the support value.")
        frequent_itemsets_export = pd.DataFrame(columns=['support', 'itemsets_names'])
        association_rules_export = pd.DataFrame(columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
    else:
        frequent_itemsets_export = filtered_itemsets_cat[['support', 'itemsets_sku']].copy()
        rules_cat = association_rules(frequent_itemsets_cat, metric="confidence", min_threshold=0.15)  # changed from 0.1 to 0.15
        rules_cat = rules_cat[(rules_cat['lift'] >= 1)].sort_values(['confidence', 'lift'], ascending=[False, False])

        # Only keep rules where all items are in the category
        def rule_all_in_cat(fset):
            return all(str(tok) in cat_product_ids for tok in fset)

        rules_filtered_cat = rules_cat[rules_cat['antecedents'].apply(rule_all_in_cat) & rules_cat['consequents'].apply(rule_all_in_cat)].copy()

        if rules_filtered_cat.empty:
            print("No association rules found for category.")
            association_rules_export = pd.DataFrame(columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
        else:
            # Remove combined_score calculation and sorting
            # rules_filtered_cat['combined_score'] = (rules_filtered_cat['lift'] * 0.7) + (rules_filtered_cat['support'] / rules_filtered_cat['support'].max() * 30)
            # rules_filtered_cat = rules_filtered_cat.sort_values('combined_score', ascending=False)
            rules_filtered_cat['antecedents_sku'] = rules_filtered_cat['antecedents'].apply(lambda s: ', '.join(sorted(s)))
            rules_filtered_cat['consequents_sku'] = rules_filtered_cat['consequents'].apply(lambda s: ', '.join(sorted(s)))
            association_rules_export = rules_filtered_cat[['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction']].copy()

    # Translate IDs to Names
    def translate_ids_to_names(id_string):
        if not isinstance(id_string, str):
            return ''
        names = [id_to_name.get(id.strip(), id.strip()) for id in id_string.split(',')]
        return ', '.join(names)

    frequent_itemsets_export.rename(columns={'itemsets_sku': 'itemsets_names'}, inplace=True)
    frequent_itemsets_export['itemsets_names'] = frequent_itemsets_export['itemsets_names'].apply(translate_ids_to_names)
    frequent_itemsets_export['category'] = category_name
    association_rules_export.rename(columns={'antecedents_sku': 'antecedents_names', 'consequents_sku': 'consequents_names'}, inplace=True)
    if not association_rules_export.empty:
        association_rules_export['antecedents_names'] = association_rules_export['antecedents_names'].apply(translate_ids_to_names)
        association_rules_export['consequents_names'] = association_rules_export['consequents_names'].apply(translate_ids_to_names)
    association_rules_export['category'] = category_name

    def remove_reversed_rule_duplicates(df):
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

    # After translating names, remove reversed duplicates
    association_rules_export = remove_reversed_rule_duplicates(association_rules_export)

    # LIMIT RESULTS TO TOP 5 PER CATEGORY
    if not frequent_itemsets_export.empty:
        frequent_itemsets_export = frequent_itemsets_export.sort_values('support', ascending=False).head(5).reset_index(drop=True)
    if not association_rules_export.empty and {'confidence', 'lift'}.issubset(association_rules_export.columns):
        association_rules_export = association_rules_export.sort_values(['confidence', 'lift'], ascending=[False, False]).head(5).reset_index(drop=True)

    # Append to all results
    all_itemsets = pd.concat([all_itemsets, frequent_itemsets_export], ignore_index=True)
    all_rules = pd.concat([all_rules, association_rules_export], ignore_index=True)

    return all_itemsets, all_rules

def run_mba_for_meal(output_folder, all_itemsets, all_rules):
    print(f"\n--- Running MBA for MEAL (FOOD <-> DRINK) ---")
    os.makedirs(output_folder, exist_ok=True)

    # Get FOOD and DRINK product_ids
    food_ids = set(prod_dim[prod_dim['CATEGORY'] == 'FOOD']['product_id'].astype(str).str.strip())
    drink_ids = set(prod_dim[prod_dim['CATEGORY'] == 'DRINK']['product_id'].astype(str).str.strip())

    # Only keep transactions that have at least one FOOD and one DRINK item
    def has_food_and_drink(pid_string):
        if not isinstance(pid_string, str):
            return False
        pids = set(pid.strip() for pid in pid_string.split(','))
        return bool(pids & food_ids) and bool(pids & drink_ids)

    df_meal = df[df['product_ids'].apply(has_food_and_drink)].copy()
    if df_meal.empty:
        print("No transactions found with both FOOD and DRINK.")
        return all_itemsets, all_rules

    print("One hot encoding...")
    one_hot_meal = df_meal['product_ids'].astype(str).str.get_dummies(sep=',')
    one_hot_meal.columns = [c.strip() for c in one_hot_meal.columns]
    one_hot_meal = one_hot_meal.astype(bool)

    print("Running FP-Growth algorithm...")
    frequent_itemsets_meal = fpgrowth(one_hot_meal, min_support=0.003, use_colnames=True)

    # Only keep itemsets with at least one FOOD and one DRINK
    def itemset_has_food_and_drink(itemset):
        items = set(str(tok) for tok in itemset)
        return bool(items & food_ids) and bool(items & drink_ids)

    filtered_itemsets_meal = frequent_itemsets_meal[frequent_itemsets_meal['itemsets'].apply(itemset_has_food_and_drink)].copy()
    if not filtered_itemsets_meal.empty:
        filtered_itemsets_meal['itemsets_sku'] = filtered_itemsets_meal['itemsets'].apply(lambda s: ', '.join(sorted(s)))
    else:
        frequent_itemsets_meal['itemsets_sku'] = frequent_itemsets_meal['itemsets'].apply(lambda s: ', '.join(sorted(s)))
        filtered_itemsets_meal = frequent_itemsets_meal

    frequent_itemsets_export = filtered_itemsets_meal[['support', 'itemsets_sku']].copy()

    # Generate rules and filter for FOOD->DRINK or DRINK->FOOD
    rules_meal = association_rules(frequent_itemsets_meal, metric="confidence", min_threshold=0.15)  # changed from 0.1 to 0.15
    rules_meal = rules_meal[(rules_meal['lift'] >= 1)].sort_values(['confidence', 'lift'], ascending=[False, False])

    def is_meal_rule(antecedents, consequents):
        a = set(str(tok) for tok in antecedents)
        c = set(str(tok) for tok in consequents)
        # One side all FOOD, other all DRINK
        return ((a <= food_ids and c <= drink_ids) or (a <= drink_ids and c <= food_ids))

    rules_filtered_meal = rules_meal[rules_meal.apply(lambda row: is_meal_rule(row['antecedents'], row['consequents']), axis=1)].copy()

    if rules_filtered_meal.empty:
        print("No meal association rules found.")
        association_rules_export = pd.DataFrame(columns=['antecedents_names', 'consequents_names', 'support', 'confidence', 'lift', 'leverage', 'conviction'])
    else:
        # Remove combined_score calculation and sorting
        # rules_filtered_meal['combined_score'] = (rules_filtered_meal['lift'] * 0.7) + (rules_filtered_meal['support'] / rules_filtered_meal['support'].max() * 30)
        # rules_filtered_meal = rules_filtered_meal.sort_values('combined_score', ascending=False)
        rules_filtered_meal['antecedents_sku'] = rules_filtered_meal['antecedents'].apply(lambda s: ', '.join(sorted(s)))
        rules_filtered_meal['consequents_sku'] = rules_filtered_meal['consequents'].apply(lambda s: ', '.join(sorted(s)))
        association_rules_export = rules_filtered_meal[['antecedents_sku', 'consequents_sku', 'support', 'confidence', 'lift', 'leverage', 'conviction']].copy()

    # Translate IDs to Names
    def translate_ids_to_names(id_string):
        if not isinstance(id_string, str):
            return ''
        names = [id_to_name.get(id.strip(), id.strip()) for id in id_string.split(',')]
        return ', '.join(names)

    frequent_itemsets_export.rename(columns={'itemsets_sku': 'itemsets_names'}, inplace=True)
    frequent_itemsets_export['itemsets_names'] = frequent_itemsets_export['itemsets_names'].apply(translate_ids_to_names)
    frequent_itemsets_export['category'] = 'MEAL'
    association_rules_export.rename(columns={'antecedents_sku': 'antecedents_names', 'consequents_sku': 'consequents_names'}, inplace=True)
    if not association_rules_export.empty:
        association_rules_export['antecedents_names'] = association_rules_export['antecedents_names'].apply(translate_ids_to_names)
        association_rules_export['consequents_names'] = association_rules_export['consequents_names'].apply(translate_ids_to_names)
    association_rules_export['category'] = 'MEAL'

    # Remove reversed duplicates
    def remove_reversed_rule_duplicates(df):
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

    association_rules_export = remove_reversed_rule_duplicates(association_rules_export)

    # LIMIT RESULTS TO TOP 5 FOR MEAL CATEGORY
    if not frequent_itemsets_export.empty:
        frequent_itemsets_export = frequent_itemsets_export.sort_values('support', ascending=False).head(5).reset_index(drop=True)
    if not association_rules_export.empty and {'confidence', 'lift'}.issubset(association_rules_export.columns):
        association_rules_export = association_rules_export.sort_values(['confidence', 'lift'], ascending=[False, False]).head(5).reset_index(drop=True)

    # Append to all results
    all_itemsets = pd.concat([all_itemsets, frequent_itemsets_export], ignore_index=True)
    all_rules = pd.concat([all_rules, association_rules_export], ignore_index=True)

    return all_itemsets, all_rules

# --- Main execution ---
if 'prod_dim' in locals():
    output_folder = 'mba_output'
    os.makedirs(output_folder, exist_ok=True)
    all_itemsets = pd.DataFrame()
    all_rules = pd.DataFrame()
    all_itemsets, all_rules = run_mba_for_category('FOOD', output_folder, all_itemsets, all_rules)
    all_itemsets, all_rules = run_mba_for_category('DRINK', output_folder, all_itemsets, all_rules)
    all_itemsets, all_rules = run_mba_for_meal(output_folder, all_itemsets, all_rules)

    # Export combined results
    # excel_file_path = os.path.join(output_folder, 'market_basket_analysis_results.xlsx')
    # with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
    #     all_itemsets.to_excel(writer, sheet_name='frequent_itemsets', index=False)
    #     all_rules.to_excel(writer, sheet_name='association_rules', index=False)

    frequent_itemsets_csv_path = os.path.join(output_folder, 'frequent_itemsets.csv')
    association_rules_csv_path = os.path.join(output_folder, 'association_rules.csv')
    all_itemsets.to_csv(frequent_itemsets_csv_path, index=False)
    all_rules.to_csv(association_rules_csv_path, index=False)

    print(f"\nResults exported successfully to {output_folder}")
else:
    print("Product dimension not loaded; cannot run category-specific MBA.")