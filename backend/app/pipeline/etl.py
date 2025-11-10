import pandas as pd
import glob
import os
import numpy as np
import re
import time
from functools import lru_cache

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# =========================
# HELPERS: INPUT PARSING
# =========================

def process_sales_file(file_path):
    """Process individual sales file to handle varying structures (original logic)."""
    df = pd.read_excel(file_path)

    # Find the row that contains the actual column headers
    header_row = None
    for i in range(min(10, len(df))):
        row_values = df.iloc[i].astype(str).str.lower()
        if any(col in row_values.values for col in ['date', 'receipt', 'time']):
            header_row = i
            break

    if header_row is None:
        header_row = 4  # Default to row 4 if not found

    # Set headers and remove header rows
    df.columns = df.iloc[header_row]
    df = df[header_row + 1:].reset_index(drop=True)

    # Remove columns with NaN headers
    df = df.loc[:, ~df.columns.isna()]

    return df


def process_product_file(file_path):
    """Process individual product file to handle varying structures (original logic)."""
    df = pd.read_excel(file_path)

    # Find the row that contains the actual column headers
    header_row = None
    for i in range(min(10, len(df))):
        row_values = df.iloc[i].astype(str).str.lower()
        if any(col in row_values.values for col in ['date', 'receipt', 'product', 'item']):
            header_row = i
            break

    if header_row is None:
        header_row = 4  # Default to row 4 if not found

    # Set headers and remove header rows
    df.columns = df.iloc[header_row]
    df = df[header_row + 1:].reset_index(drop=True)

    # Remove columns with NaN headers
    df = df.loc[:, ~df.columns.isna()]

    return df

# =========================
# DIMENSIONS
# =========================

def create_time_dimension(_date_series):
    """
    Create a time dimension with hours (1-23) and minutes (00-59).
    Columns: time_id, time_desc, time_level, parent_id
    (Matches original behavior.)
    """
    hour_rows = []
    for h in range(1, 24):
        hour_rows.append({
            'time_id': f'H{h:02}',
            'time_desc': f'{h:02}',
            'time_level': 1,
            'parent_id': 'NA'
        })
    minute_rows = []
    for h in range(1, 24):
        for m in range(0, 60):
            minute_rows.append({
                'time_id': f'H{h:02}M{m:02}',
                'time_desc': f'{h:02}:{m:02}',
                'time_level': 0,
                'parent_id': f'H{h:02}'
            })
    return pd.DataFrame(hour_rows + minute_rows)

# =========================
# COSTING (SAFE-CACHED, SAME LOGIC)
# =========================

@lru_cache(maxsize=None)
def normalize_drink_name(name: str) -> str:
    """Normalize drink name for matching (same logic as original)."""
    if not isinstance(name, str):
        return ''
    name = name.replace('CAR ', 'CARAMEL ')
    return name.upper().replace(' ', '').replace('OZ', '').replace('CHOCO', 'CHOCOLATE')


@lru_cache(maxsize=None)
def get_drink_cost(product_name: str) -> float | None:
    """
    Get cost from costing file sheets.
    Logic matches your original implementation; we only cache per product_name.
    """
    try:
        costing_files = glob.glob('raw_costing/*.xlsx')
        if not costing_files:
            return None

        xl = pd.ExcelFile(costing_files[0])
        product_norm = normalize_drink_name(product_name)

        best_match = None
        best_score = 0

        for sheet in xl.sheet_names:
            sheet_norm = normalize_drink_name(sheet)
            score = 0

            # NOTE: original: product_norm has no spaces, so this loop is effectively a no-op.
            # Kept exactly for behavior parity.
            for word in product_norm.split():
                if len(word) >= 4 and word in sheet_norm:
                    score += 2

            # Check for size (8, 12, 16)
            for size in ['8', '12', '16']:
                if size in product_norm and size in sheet_norm:
                    score += 1
                    break

            # Check for temperature
            if 'HOT' in product_norm and 'HOT' in sheet_norm:
                score += 1
            elif ('ICED' in product_norm or 'COLD' in product_norm) and 'ICED' in sheet_norm:
                score += 1

            if score > best_score:
                best_score = score
                best_match = sheet

        if best_match and best_score >= 2:
            df = pd.read_excel(costing_files[0], sheet_name=best_match, header=None)
            if len(df) > 35:
                row_35 = df.iloc[35]
                for col in range(1, len(row_35)):
                    if pd.notna(row_35[col]) and isinstance(row_35[col], (int, float)):
                        return round(float(row_35[col]), 2)
    except Exception:
        pass

    return None

# =========================
# PRODUCT DIMENSION (ORIGINAL LOGIC)
# =========================

def create_product_dimensions(combined_df: pd.DataFrame):
    """
    Create SCD Type 4 Product Dimension tables (current and history)
    Implementation mirrors your original code so outputs match.
    """
    if 'Product ID' not in combined_df.columns or 'Product Name' not in combined_df.columns:
        print("Warning: Required product columns (Product ID, Product Name) not found")
        return None, None

    df = combined_df.copy()

    df = df.rename(columns={
        'Product ID': 'product_id',
        'Product Name': 'product_name'
    })

    product_columns = ['product_id', 'product_name', 'Price']
    available_product_columns = [c for c in product_columns if c in df.columns]
    if 'Date' in df.columns:
        available_product_columns.append('Date')

    # Current: latest record per product_id
    current_products = df.groupby('product_id').last().reset_index()
    current_product_dim = current_products[available_product_columns].copy()

    current_product_dim['record_version'] = 1
    current_product_dim['is_current'] = True

    if 'Date' in current_product_dim.columns:
        current_product_dim = current_product_dim.rename(columns={'Date': 'last_transaction_date'})

    # ----- parent_sku & CATEGORY (same rules as original) -----

    def compute_parent_sku(name: str) -> str:
        if not isinstance(name, str) or name.strip() == '':
            return ''
        original = name.upper().strip()
        work = original.replace('.', ' ')
        work = re.sub(r'\b(8|12|16)0Z\b', lambda m: m.group(1) + 'OZ', work)
        size_pattern = re.compile(r'\b(8|12|16)\s*(?:O|0)Z\.?(?=\b)')
        triggers = {'ICED', 'ICE', 'HOT', 'COLD'}
        has_trigger = any(t in work.split() for t in triggers) or bool(size_pattern.search(work))
        if has_trigger:
            work = size_pattern.sub('', work)
            tokens = [tok for tok in work.split()
                      if tok not in triggers and tok not in {'OZ'}]
            tokens = [tok for tok in tokens if tok not in {'8', '12', '16'}]
            if not tokens:
                tokens = original.split()
        else:
            tokens = original.split()
        while tokens and tokens[-1] in {'1', '2', '3'}:
            tokens.pop()
        return '-'.join(tokens)

    drink_keywords = {"ICED", "ICE", "HOT", "8OZ", "12OZ", "16OZ", "COLD",
                      "TEA", "LATTE", "SHAKE", "FRAPPE", "MOCHA", "GLASS", "PITCHER", "WATER"}
    token_pattern = re.compile(r'[A-Z0-9]+')
    others_triggers = ["CHARGING"]
    others_word_regex = re.compile(r'\bTAKE\b')
    extra_phrase_triggers = ["BOTTLED WATER", "PLAIN RICE"]
    extra_word_triggers = [re.compile(r'\bEGG\b'),
                           re.compile(r'\bDIJON\b'),
                           re.compile(r'\bEXTRA\b')]

    def classify_category_row(row) -> str:
        name = row.get('product_name', '')
        pid = row.get('product_id', '')
        if not isinstance(name, str):
            name = ''
        upper_name = name.upper()

        if any(trig in upper_name for trig in others_triggers) or others_word_regex.search(upper_name):
            return 'OTHERS'

        if any(phrase in upper_name for phrase in extra_phrase_triggers) or any(
                rgx.search(upper_name) for rgx in extra_word_triggers):
            return 'EXTRA'

        tokens = {t.upper() for t in token_pattern.findall(upper_name)}
        category = 'DRINK' if tokens & drink_keywords else 'FOOD'
        if isinstance(pid, str):
            upid = pid.upper()
            if 'DRNKS' in upid or 'DKS' in upid:
                category = 'DRINK'
        return category

    if 'product_name' in current_product_dim.columns:
        current_product_dim['parent_sku'] = current_product_dim['product_name'].apply(compute_parent_sku)
        current_product_dim['CATEGORY'] = current_product_dim.apply(classify_category_row, axis=1)

    # product_cost (same rule: DRINK→costing if available, else 60% of Price)
    def calculate_cost(row):
        if row.get('CATEGORY') == 'DRINK':
            cost = get_drink_cost(row.get('product_name', ''))
            if cost:
                return cost
        return round(row['Price'] * 0.60, 2) if pd.notna(row.get('Price')) else np.nan

    current_product_dim['product_cost'] = current_product_dim.apply(calculate_cost, axis=1)

    if 'Price' in current_product_dim.columns and 'product_cost' in current_product_dim.columns:
        cols = list(current_product_dim.columns)
        cols.remove('product_cost')
        insert_pos = cols.index('Price') + 1
        cols.insert(insert_pos, 'product_cost')
        current_product_dim = current_product_dim[cols]

    # ----- History Product Dimension (original logic) -----

    history_product_dim = df[available_product_columns].copy()
    history_product_dim = history_product_dim.drop_duplicates(
        subset=['product_id', 'product_name', 'Price']
        if 'Price' in available_product_columns
        else ['product_id', 'product_name']
    )

    history_product_dim['record_version'] = history_product_dim.groupby('product_id').cumcount() + 1
    history_product_dim['is_current'] = False

    latest_versions = history_product_dim.groupby('product_id')['record_version'].max()
    for product_id, max_version in latest_versions.items():
        mask = (history_product_dim['product_id'] == product_id) & (
            history_product_dim['record_version'] == max_version)
        history_product_dim.loc[mask, 'is_current'] = True

    if 'product_name' in history_product_dim.columns:
        history_product_dim['parent_sku'] = history_product_dim['product_name'].apply(compute_parent_sku)
        history_product_dim['CATEGORY'] = history_product_dim.apply(classify_category_row, axis=1)

    history_product_dim['product_cost'] = history_product_dim.apply(calculate_cost, axis=1)

    if 'Price' in history_product_dim.columns and 'product_cost' in history_product_dim.columns:
        cols = list(history_product_dim.columns)
        cols.remove('product_cost')
        insert_pos = cols.index('Price') + 1
        cols.insert(insert_pos, 'product_cost')
        history_product_dim = history_product_dim[cols]

    if 'Date' in history_product_dim.columns:
        history_product_dim = history_product_dim.rename(columns={'Date': 'last_transaction_date'})

    return current_product_dim, history_product_dim

# =========================
# EXTRACT (ORIGINAL + SAFE)
# =========================

def extract():
    print("=== EXTRACT PHASE ===")
    os.makedirs('cleaned_data', exist_ok=True)

    # Sales Transactions
    print("Extracting Excel Sales Transactions List...")
    sales_files = glob.glob('raw_sales/*.xlsx')
    sales_dfs = [process_sales_file(f) for f in sales_files]

    print("\nChecking for date range overlaps in sales files...")
    file_date_ranges = []
    for f_path, df in zip(sales_files, sales_dfs):
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            valid = df['Date'].dropna()
            if not valid.empty:
                file_date_ranges.append({
                    'file': os.path.basename(f_path),
                    'min_date': valid.min(),
                    'max_date': valid.max()
                })
                print(f"  {os.path.basename(f_path)}: {valid.min():%Y-%m-%d} to {valid.max():%Y-%m-%d}")

    for i in range(len(file_date_ranges)):
        for j in range(i + 1, len(file_date_ranges)):
            r1, r2 = file_date_ranges[i], file_date_ranges[j]
            if r1['min_date'] <= r2['max_date'] and r2['min_date'] <= r1['max_date']:
                overlap_start = max(r1['min_date'], r2['min_date'])
                overlap_end = min(r1['max_date'], r2['max_date'])
                print("\nWARNING: Overlap detected!")
                print(f"  Files: '{r1['file']}' and '{r2['file']}'")
                print(f"  Overlap period: {overlap_start:%Y-%m-%d} to {overlap_end:%Y-%m-%d}")
    print()

    # Align & concat
    all_cols = set()
    for df in sales_dfs:
        all_cols.update(df.columns)
    for i, df in enumerate(sales_dfs):
        for c in all_cols:
            if c not in df.columns:
                df[c] = None
        sales_dfs[i] = df[sorted(all_cols)]
    sales_df = pd.concat(sales_dfs, ignore_index=True)

    if 'Date' in sales_df.columns:
        sales_df['Date'] = pd.to_datetime(sales_df['Date'], errors='coerce')

    # Sales by Product
    print("Extracting Excel Sales Report by Product List...")
    prod_files = glob.glob('raw_sales_by_product/*.xlsx')
    prod_dfs = [process_product_file(f) for f in prod_files]

    print("\nChecking for date range overlaps in sales by product files...")
    prod_ranges = []
    for f_path, df in zip(prod_files, prod_dfs):
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            valid = df['Date'].dropna()
            if not valid.empty:
                prod_ranges.append({
                    'file': os.path.basename(f_path),
                    'min_date': valid.min(),
                    'max_date': valid.max()
                })
                print(f"  {os.path.basename(f_path)}: {valid.min():%Y-%m-%d} to {valid.max():%Y-%m-%d}")

    for i in range(len(prod_ranges)):
        for j in range(i + 1, len(prod_ranges)):
            r1, r2 = prod_ranges[i], prod_ranges[j]
            if r1['min_date'] <= r2['max_date'] and r2['min_date'] <= r1['max_date']:
                overlap_start = max(r1['min_date'], r2['min_date'])
                overlap_end = min(r1['max_date'], r2['max_date'])
                print("\nWARNING: Overlap detected!")
                print(f"  Files: '{r1['file']}' and '{r2['file']}'")
                print(f"  Overlap period: {overlap_start:%Y-%m-%d} to {overlap_end:%Y-%m-%d}")
    print()

    all_prod_cols = set()
    for df in prod_dfs:
        all_prod_cols.update(df.columns)
    for i, df in enumerate(prod_dfs):
        for c in all_prod_cols:
            if c not in df.columns:
                df[c] = None
        prod_dfs[i] = df[sorted(all_prod_cols)]
    sales_by_product_df = pd.concat(prod_dfs, ignore_index=True)

    if 'Date' in sales_by_product_df.columns:
        sales_by_product_df['Date'] = pd.to_datetime(sales_by_product_df['Date'], errors='coerce')
    if 'Date' in sales_df.columns:
        sales_df['Date'] = pd.to_datetime(sales_df['Date'], errors='coerce')

    if 'Take Out' in sales_by_product_df.columns:
        sales_by_product_df['Take Out'] = sales_by_product_df['Take Out'].apply(
            lambda x: 'True' if str(x).strip().upper() == 'Y'
            else ('False' if pd.isna(x) or str(x).strip() == '' else x)
        )

    print("Extract phase completed successfully.")
    return sales_df, sales_by_product_df

# =========================
# TRANSFORM (ORIGINAL LOGIC)
# =========================

def transform(sales_df, sales_by_product_df):
    print("=== TRANSFORM PHASE ===")
    print("Cleaning Data...")

    # Sales Transaction cleaning
    cols_drop_sales = [
        'Posted', 'Price Level', 'Branch', 'TM#', 'Customer ID', 'Customer Name', 'Cashier',
        'Serviced By', 'Dine In', 'Take Out', 'Local Tax', 'Amusement Tax', 'EWT', 'NAC',
        'Solo Parent', 'Service', 'Feedback Rating', 'Diplomat'
    ]
    sales_df = sales_df.drop(columns=[c for c in cols_drop_sales if c in sales_df.columns], errors='ignore')

    if 'Date' in sales_df.columns:
        sales_df = sales_df[sales_df['Date'].notna() &
                            (sales_df['Date'].astype(str).str.strip() != '')].reset_index(drop=True)
    if 'Time' in sales_df.columns:
        sales_df = sales_df[sales_df['Time'].notna() &
                            (sales_df['Time'].astype(str).str.strip() != '')].reset_index(drop=True)

    # Sales by Product cleaning
    cols_drop_prod = [
        'Lot/Serial', 'Posted', 'TM#', 'Unit', 'Discount ID', 'Discount', '% Discount',
        'Price ID', 'Branch', 'Customer ID', 'Customer'
    ]
    sales_by_product_df = sales_by_product_df.drop(
        columns=[c for c in cols_drop_prod if c in sales_by_product_df.columns],
        errors='ignore'
    )

    if 'Date' in sales_by_product_df.columns:
        sales_by_product_df = sales_by_product_df[
            sales_by_product_df['Date'].notna() &
            (sales_by_product_df['Date'].astype(str).str.strip() != '')
        ].reset_index(drop=True)

    if 'Time' in sales_by_product_df.columns:
        sales_by_product_df = sales_by_product_df[
            sales_by_product_df['Time'].notna() &
            (sales_by_product_df['Time'].astype(str).str.strip() != '')
        ].reset_index(drop=True)

        # IMPORTANT: Outlier/negative filter is intentionally inside this block
        # to match original behavior.
        if 'Price' in sales_by_product_df.columns:
            sales_by_product_df = sales_by_product_df[
                pd.to_numeric(sales_by_product_df['Price'], errors='coerce').between(0, 50000)
            ].reset_index(drop=True)

        for col in ['Qty', 'Line Total', 'Net Total', 'Price']:
            if col in sales_by_product_df.columns:
                sales_by_product_df = sales_by_product_df[
                    pd.to_numeric(sales_by_product_df[col], errors='coerce') >= 0
                ].reset_index(drop=True)

    # Standardization rules (exactly as original)
    if 'Product ID' in sales_by_product_df.columns and 'Product Name' in sales_by_product_df.columns:
        standardization_rules = [
            {'from_ids': ['FDS-2017-0024-W-DCS-BLMW'], 'from_names': ['DOUBLE CHOCOLATE AND STRAWBERRIES'],
                'to_id': '2024waffles4', 'to_name': 'DOUBLE CHOCS AND STRAWBERRIES'},
            {'from_ids': ['FDS-2017-0020-W-BN2-BLMW'], 'from_names': ['BANANA NUTELLA WAFFLES'],
                'to_id': '2024waffles2', 'to_name': 'BANANA NUTELLA'},
            {'from_ids': ['FDS-2017-0028-S-BCT-BLMW'], 'from_names': ['BACON, COLESLAW AND TOMATO'],
                'to_id': '2024Breads7', 'to_name': 'CLASSIC BACON COLESLAW N TOMATO'},
            {'from_ids': ['FDS-2017-0029-S-TCS-BLMW'], 'from_names': ['THE CLUB SANDWICH'],
                'to_id': '2024breads', 'to_name': 'THE CKUB'},
            {'from_ids': ['DKS-2018-0034-COOL-PEACH-16-BLMW'], 'from_names': ['PEACH'],
                'to_id': 'DKS-2018-0025-SH-CAMPCH-16-BLMW', 'to_name': 'CAMOMILE PEACH'},
            {'from_ids': ['DKS-2017-0025-SH-16-BLMW'], 'from_names': ['CHAMOMILE PEACH'],
                'to_id': 'DKS-2018-0020-FRAPPE-PCHLUCK-16-BLMW', 'to_name': 'PEACHIEST LUCK'},
            {'from_ids': ['FDS-2018-0001-GARLICBRD--BLMW'], 'from_names': ['GARLIC BREAD EXTRA'],
                'to_id': '2024BREads9', 'to_name': 'GALIC BREAD ALA CARTE'},
            {'from_ids': ['FDS-2018-0001-BEF-KOR-BLMW'], 'from_names': ['KOREAN BEEF BBQ'],
                'to_id': '2024lrgplates13', 'to_name': 'K POP BBQ BEEF'},
            {'from_ids': ['2024FDPIZSCREAM'], 'from_names': ['SPINACH & CREAM PIZZA'],
                'to_id': '2024pizza3', 'to_name': 'SPINACH N CREAM PIZZA'},
            {'from_ids': ['2024PromobundleChix steak'], 'from_names': ['CHICKEN STEAK PROMO BUNDLE'],
                'to_id': '2024PromoChixsteak', 'to_name': 'CHICKEN STEAK PROMO'},
            {'from_ids': ['FDS-2017-0030-PBB-LONG-BLMW'], 'from_names': ['LONGANISA PBB'],
                'to_id': '2024FilBfast4', 'to_name': 'FIL BFAST LONGGANISA'},
            {'from_ids': ['FDS-2018-0001-MOJOS-BLMW'], 'from_names': ['MOJOS'],
                'to_id': '2024smlplates2', 'to_name': 'MOJOJOJOS'},
            {'from_ids': ['FFDS-2020-VM-DANGGIT-BLMW', 'FDS-2017-0030-PBB-DNGGT-BLMW'], 'from_names': ['DANGGIT', 'DANGGIT PBB'],
                'to_id': '2024FilBFast3', 'to_name': 'FIL BREAKFAST DANGGIT'},
            {'from_ids': ['FDSS-2018-001-EGG-EXT-BLMW'], 'from_names': ['EXTRA EGG'],
                'to_id': 'ING-EGG', 'to_name': 'EGG'},
            {'from_ids': ['FDS-2017-009-SPAMFRT-BLMW', 'FDS-2017-009-SPAMRI-BLMW', 'FDS-2017-009-SPAMRI-BLMW'],
                'from_names': ['SPAM, FRENCH TOAST, EGGS AND HASH BROWN',
                               'SPAM, RICE, EGGS, HASH BROWN', 'SPAM, WAFFLES, EGGS, HASH BROWN'],
                'to_id': 'FDS-2017-0010-ADB-S-BLMW', 'to_name': 'SPAM WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-HUNGFRT-BLMW', 'FDS-2017-009-HUNGRI-BLMW', 'FDS-2017-009-HUNGRI-BLMW'],
                'from_names': ['HUNGARIAN SAUSAGE, FRENCH TOAST, EGGS AND HASH BROWN',
                               'HUNGARIAN SAUSAGE, RICE, EGGS, HASH BROWN',
                               'HUNGARIAN SAUSAGE, WAFFLES, EGGS AND HASH BROWN'],
                'to_id': 'FDS-2017-0011-ADB-HS-BLMW',
                'to_name': 'HUNGARIAN SAUSAGE WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-GERFRT-BLMW', 'FDS-2017-009-GERRI-BLMW', 'FDS-2017-009-GERWAF-BLMW'],
                'from_names': ['GERMAN FRANKS, FRENCH TOAST, EGGS AND HASH BROWN',
                               'GERMAN FRANKS, RICE, EGGS AND HASH BROWN',
                               'GERMAN FRANKS, WAFFLES, EGGS AND HASH BROWN'],
                'to_id': 'FDS-2017-0012-ADB-GF-BLMW',
                'to_name': 'GERMAN FRANKS WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-CBFRT-BLMW', 'FDS-2017-009-CBRI-BLMW', 'FDS-2017-009-CBWAF-BLMW'],
                'from_names': ['CORNED BEEF, FRENCH TOAST, EGGS AND HASH BROWN',
                               'CORNED BEEF, RICE, EGGS, AND HASH BROWN',
                               'CORNED BEEF, WAFFLES, EGGS AND HASH BROWN'],
                'to_id': 'FDS-2017-0013-ADB-CB-BLMW',
                'to_name': 'CORNED BEEF WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-BAFRT-BLMW', 'FDS-2017-009-BARI-BLMW', 'FDS-2017-009-BAWA-BLMW'],
                'from_names': ['BACON, FRENCH TOAST, EGGS, HASH BROWN',
                               'BACON, RICE, EGGS AND HASH BROWN',
                               'BACON, WAFFLES, EGGS, HASH BROWN'],
                'to_id': 'FDS-2017-009-ADB-B-BLMW',
                'to_name': 'BACON WITH RICE OR WAFFLES OR FRENCH TOAST'},
        ]

        for rule in standardization_rules:
            rule['from_names_upper'] = {n.strip().upper()
                                        for n in rule.get('from_names', [])}

        for rule in standardization_rules:
            mask_id = sales_by_product_df['Product ID'].astype(str).isin(rule.get('from_ids', []))
            mask_name = sales_by_product_df['Product Name'].astype(str).str.strip().str.upper().isin(
                rule.get('from_names_upper', set()))
            mask = mask_id | mask_name
            if mask.any():
                sales_by_product_df.loc[mask, 'Product ID'] = rule['to_id']
                sales_by_product_df.loc[mask, 'Product Name'] = rule['to_name']

    # Standardize Product IDs by normalized Product Name (first seen ID)
    if 'Product ID' in sales_by_product_df.columns and 'Product Name' in sales_by_product_df.columns:
        tmp = sales_by_product_df[['Product Name', 'Product ID']].dropna(subset=['Product Name', 'Product ID']).copy()
        tmp['__norm_name'] = tmp['Product Name'].astype(str).str.strip().str.casefold()
        name_to_first_id = tmp.drop_duplicates(subset='__norm_name').set_index('__norm_name')['Product ID']
        norm_names = sales_by_product_df['Product Name'].astype(str).str.strip().str.casefold()
        sales_by_product_df['Product ID'] = norm_names.map(name_to_first_id).fillna(sales_by_product_df['Product ID'])
        del tmp

    if 'Product Name' in sales_by_product_df.columns:
        sales_by_product_df['Product Name'] = sales_by_product_df['Product Name'].astype(str).str.upper().str.strip()

    if 'Receipt#' in sales_df.columns:
        sales_df = sales_df.rename(columns={'Receipt#': 'Receipt No'})

    # Deduplicate sales_df
    print("Checking Sales Transactions for duplicates...")
    before_sales = len(sales_df)
    if all(c in sales_df.columns for c in ['Date', 'Receipt No', 'Time']):
        sales_df = sales_df.drop_duplicates(subset=['Date', 'Receipt No', 'Time'], keep='first').reset_index(drop=True)
        removed = before_sales - len(sales_df)
        if removed > 0:
            print(f"Removed {removed} duplicate sales records ({removed/before_sales*100:.2f}%)")
        else:
            print(f"No duplicates found - all {before_sales} records are unique")
    else:
        print("Skipping: Required columns not found")

    print("Merging Sales Transaction and Sales by Product List")
    if 'Receipt No' in sales_df.columns and 'Receipt No' in sales_by_product_df.columns:
        combined_df = pd.merge(
            sales_df,
            sales_by_product_df,
            on='Receipt No',
            suffixes=('', '_product'),
            how='inner'
        )
        if 'Date_product' in combined_df.columns:
            combined_df = combined_df.drop(columns=['Date_product'])
    else:
        print("Warning: Receipt No column not found in one or both dataframes")
        combined_df = pd.DataFrame()

    # Save cleaned intermediates (needed for your diffs)
    sales_df.to_csv('cleaned_data/sales_transactions.csv', index=False)
    sales_by_product_df.to_csv('cleaned_data/sales_by_product.csv', index=False)

    print("Transform phase completed successfully.")
    return combined_df

# =========================
# LOAD (ORIGINAL LOGIC)
# =========================

def load(combined_df):
    print("=== LOAD PHASE ===")
    os.makedirs('etl_dimensions', exist_ok=True)

    if combined_df.empty:
        print("Warning: Combined dataframe is empty, no output files created")
        print("Load phase completed.")
        return

    # Time dimension
    if 'Date' in combined_df.columns:
        print("Creating time_dimension.csv...")
        time_dim_df = create_time_dimension(combined_df['Date'])
        time_dim_df.to_csv('etl_dimensions/time_dimension.csv', index=False)
        print("etl_dimensions/time_dimension.csv created")

    # Map time_id
    def time_to_id(time_str):
        try:
            s = str(time_str).strip()
            if ':' in s:
                h, m = s.split(':')[:2]
            elif len(s) == 4 and s.isdigit():
                h, m = s[:2], s[2:]
            else:
                return None
            return f'H{int(h):02}M{int(m):02}'
        except Exception:
            return None

    if 'Time' in combined_df.columns:
        combined_df['time_id'] = combined_df['Time'].apply(time_to_id)
    else:
        combined_df['time_id'] = None

    # Reorder columns
    priority_columns = [
        'Date', 'time_id', 'Receipt No', 'Product ID', 'Product Name',
        'Qty', 'Price', 'Line Total', 'Net Total'
    ]
    existing_priority = [c for c in priority_columns if c in combined_df.columns]
    remaining = [c for c in combined_df.columns if c not in existing_priority and c != 'Time']
    final_cols = existing_priority + remaining

    if 'Time' in combined_df.columns:
        combined_df = combined_df.drop(columns=['Time'])

    combined_df = combined_df[final_cols]

    # Remove dups
    print("Removing duplicates from fact transaction dimension...")
    combined_df = combined_df.drop_duplicates().reset_index(drop=True)
    print(f"After duplicate removal: {len(combined_df)} fact transaction records")

    # Exclude last month
    if 'Date' in combined_df.columns:
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        latest_month = combined_df['Date'].dt.to_period('M').max()
        fact_df = combined_df[combined_df['Date'].dt.to_period('M') != latest_month].reset_index(drop=True)
    else:
        fact_df = combined_df

    fact_df.to_csv('etl_dimensions/fact_transaction_dimension.csv', index=False)
    print("etl_dimensions/fact_transaction_dimension.csv created")

    # Product dimensions
    print("Creating SCD Type 4 Product Dimension tables (and parent_sku mapping)...")
    current_product_dim, history_product_dim = create_product_dimensions(combined_df)

    parent_map = {}
    if current_product_dim is not None and 'product_id' in current_product_dim.columns and 'parent_sku' in current_product_dim.columns:
        parent_map = dict(zip(current_product_dim['product_id'].astype(str),
                              current_product_dim['parent_sku'].astype(str)))

    # transaction_records.csv
    if 'Receipt No' in combined_df.columns and 'Product ID' in combined_df.columns:
        tmp = combined_df.copy()
        tmp['Product ID'] = tmp['Product ID'].astype(str)
        tmp['__parent_sku'] = tmp['Product ID'].map(parent_map).fillna(tmp['Product ID'])
        transaction_records = (
            tmp.groupby('Receipt No')['__parent_sku']
            .apply(lambda x: ','.join(x.astype(str)))
            .reset_index(name='SKU')
        )
        transaction_records.to_csv('etl_dimensions/transaction_records.csv', index=False)
        print("etl_dimensions/transaction_records.csv created with columns: Receipt No, SKU")

    if current_product_dim is not None and history_product_dim is not None:
        current_product_dim.to_csv('etl_dimensions/current_product_dimension.csv', index=False)
        print("etl_dimensions/current_product_dimension.csv created")
        history_product_dim.to_csv('etl_dimensions/history_product_dimension.csv', index=False)
        print("etl_dimensions/history_product_dimension.csv created")
        print(f"Current Product Dimension: {len(current_product_dim)} products")
        print(f"History Product Dimension: {len(history_product_dim)} product records")

    print("Load phase completed successfully.")

# =========================
# MAIN
# =========================

def main():
    print("Starting ETL Pipeline...")
    total_start = time.time()
    try:
        t0 = time.time()
        sales_df, sales_by_product_df = extract()
        print(f"Extract phase took: {time.time() - t0:.2f} seconds.\n")

        t1 = time.time()
        combined_df = transform(sales_df, sales_by_product_df)
        print(f"Transform phase took: {time.time() - t1:.2f} seconds.\n")

        t2 = time.time()
        load(combined_df)
        print(f"Load phase took: {time.time() - t2:.2f} seconds.\n")

        print("ETL Process Complete.")
    except Exception as e:
        print(f"ETL Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"Total execution time: {time.time() - total_start:.2f} seconds.")

if __name__ == "__main__":
    main()
