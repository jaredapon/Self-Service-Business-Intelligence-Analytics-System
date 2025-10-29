import pandas as pd
import glob
import os
import numpy as np
import re

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


def process_sales_file(file_path):
    """Process individual sales file to handle varying structures"""
    df = pd.read_excel(file_path)

    # Find the row that contains the actual column headers
    # Look for a row that contains key columns like 'Date', 'Receipt#', etc.
    header_row = None
    for i in range(min(10, len(df))):  # Check first 10 rows
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
    """Process individual product file to handle varying structures"""
    df = pd.read_excel(file_path)

    # Find the row that contains the actual column headers
    header_row = None
    for i in range(min(10, len(df))):  # Check first 10 rows
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


def create_time_dimension(date_series):
    """
    Create a time dimension with hours (1-23) and minutes (00-59).
    Columns: time_id, time_desc, time_level, parent_id
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
    time_dim = hour_rows + minute_rows
    import pandas as pd
    return pd.DataFrame(time_dim)


def create_product_dimensions(combined_df):
    """
    Create SCD Type 4 Product Dimension tables (current and history)

    Args:
        combined_df: DataFrame containing transaction data with product information

    Returns:
        tuple: (current_product_dim, history_product_dim) DataFrames
    """
    if 'Product ID' in combined_df.columns and 'Product Name' in combined_df.columns:
        # Create a copy to avoid modifying the original
        df = combined_df.copy()

        # Rename columns
        df = df.rename(columns={
            'Product ID': 'product_id',
            'Product Name': 'product_name'
        })

        # Get unique product information with latest data
        product_columns = ['product_id', 'product_name', 'Price']
        available_product_columns = [
            col for col in product_columns if col in df.columns]

        # Date when it was last updated
        if 'Date' in df.columns:
            available_product_columns.append('Date')

    # Get the latest record for each product (current dimension)
        current_products = df.groupby('product_id').last().reset_index()
        current_product_dim = current_products[available_product_columns].copy(
        )

        # SCD metadata for current dimension
        current_product_dim['record_version'] = 1
        current_product_dim['is_current'] = True

        # Rename Date for current dimension
        if 'Date' in current_product_dim.columns:
            current_product_dim = current_product_dim.rename(
                columns={'Date': 'last_transaction_date'})

        # ------------------------------------------------------------------
        # Derive parent_sku from product_name
        # Rules: If product_name contains any of tokens (ICED, ICE, HOT, 8OZ, 12OZ, 16OZ, COLD)
        # or size patterns like '8 OZ', '12 OZ.', remove those temperature/size tokens and
        # build parent_sku from remaining words joined by '-'. Else parent_sku is product_name
        # with spaces replaced by '-'. Keep uppercase.
        # Example: 'ICED MATCHA LATTE 12 OZ.' -> 'MATCHA-LATTE'
        # ------------------------------------------------------------------
        def compute_parent_sku(name: str) -> str:
            if not isinstance(name, str) or name.strip() == '':
                return ''
            original = name.upper().strip()
            work = original.replace('.', ' ')
            # Normalize common typos like 120Z -> 12OZ, 160Z -> 16OZ
            work = re.sub(r'\b(8|12|16)0Z\b',
                          lambda m: m.group(1) + 'OZ', work)
            # Match size tokens with possible mis-typed '0' instead of 'O'
            size_pattern = re.compile(r'\b(8|12|16)\s*(?:O|0)Z\.?(?=\b)')
            triggers = {'ICED', 'ICE', 'HOT', 'COLD'}
            has_trigger = any(t in work.split()
                              for t in triggers) or bool(size_pattern.search(work))
            if has_trigger:
                work = size_pattern.sub('', work)
                tokens = [tok for tok in work.split(
                ) if tok not in triggers and tok not in {'OZ'}]
                tokens = [tok for tok in tokens if tok not in {
                    '8', '12', '16'}]
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
        extra_word_triggers = [re.compile(r'\bEGG\b'), re.compile(
            r'\bDIJON\b'), re.compile(r'\bEXTRA\b')]  # whole words

        def classify_category_row(row) -> str:
            name = row.get('product_name', '')
            pid = row.get('product_id', '')
            if not isinstance(name, str):
                name = ''
            upper_name = name.upper()

            # Rule 1: OTHERS
            if any(trig in upper_name for trig in others_triggers) or others_word_regex.search(upper_name):
                return 'OTHERS'

            # Rule 2: EXTRA
            if any(phrase in upper_name for phrase in extra_phrase_triggers) or any(rgx.search(upper_name) for rgx in extra_word_triggers):
                return 'EXTRA'

            # Default DRINK/FOOD logic
            tokens = {t.upper() for t in token_pattern.findall(upper_name)}
            category = 'DRINK' if tokens & drink_keywords else 'FOOD'
            if isinstance(pid, str):
                upid = pid.upper()
                if 'DRNKS' in upid or 'DKS' in upid:
                    category = 'DRINK'
            return category

        if 'product_name' in current_product_dim.columns:
            current_product_dim['parent_sku'] = current_product_dim['product_name'].apply(
                compute_parent_sku)
            current_product_dim['CATEGORY'] = current_product_dim.apply(
                classify_category_row, axis=1)

        # Product History Dimension
        # Get all unique combinations of product attributes over time
        history_product_dim = df[available_product_columns].copy()

        # Remove duplicates and preserve chronological order
        history_product_dim = history_product_dim.drop_duplicates(
            subset=['product_id', 'product_name', 'Price'] if 'Price' in available_product_columns else [
                'product_id', 'product_name']
        )

        # SCD metadata for history dimension
        history_product_dim['record_version'] = history_product_dim.groupby(
            'product_id').cumcount() + 1
        history_product_dim['is_current'] = False

        # Mark the latest version as current in history table
        latest_versions = history_product_dim.groupby(
            'product_id')['record_version'].max()
        for product_id, max_version in latest_versions.items():
            mask = (history_product_dim['product_id'] == product_id) & (
                history_product_dim['record_version'] == max_version)
            history_product_dim.loc[mask, 'is_current'] = True

        # Add parent_sku to history
        if 'product_name' in history_product_dim.columns:
            history_product_dim['parent_sku'] = history_product_dim['product_name'].apply(
                compute_parent_sku)
            history_product_dim['CATEGORY'] = history_product_dim.apply(
                classify_category_row, axis=1)

        # Rename Date for history dimension
        if 'Date' in history_product_dim.columns:
            history_product_dim = history_product_dim.rename(
                columns={'Date': 'last_transaction_date'})

        return current_product_dim, history_product_dim
    else:
        print("Warning: Required product columns (Product ID, Product Name) not found")
        return None, None


def extract():
    """Extract data from raw Excel files"""
    print("=== EXTRACT PHASE ===")

    # Ensure output directory exists
    os.makedirs('cleaned_data', exist_ok=True)

    # EXTRACT & CONCATENATE SALES
    print("Extracting Excel Sales Transactions List...")
    sales_files = glob.glob('raw_sales/*.xlsx')
    sales_dfs = [process_sales_file(f) for f in sales_files]

    # Align columns across all dataframes
    all_columns = set()
    for df in sales_dfs:
        all_columns.update(df.columns)

    # Ensure all dataframes have the same columns
    for i, df in enumerate(sales_dfs):
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        # Reorder columns to be consistent
        sales_dfs[i] = df[sorted(all_columns)]

    sales_df = pd.concat(sales_dfs, ignore_index=True)

    # --- FIX: Standardize Date column to datetime ---
    if 'Date' in sales_df.columns:
        sales_df['Date'] = pd.to_datetime(sales_df['Date'], errors='coerce')

    # EXTRACT & CONCATENATE SALES BY PRODUCT
    print("Extracting Excel Sales Report by Product List...")
    sales_by_product_files = glob.glob('raw_sales_by_product/*.xlsx')
    sales_by_product_dfs = [process_product_file(
        f) for f in sales_by_product_files]

    # Align columns across all product dataframes
    all_product_columns = set()
    for df in sales_by_product_dfs:
        all_product_columns.update(df.columns)

    # Ensure all dataframes have the same columns
    for i, df in enumerate(sales_by_product_dfs):
        for col in all_product_columns:
            if col not in df.columns:
                df[col] = None
        # Reorder columns to be consistent
        sales_by_product_dfs[i] = df[sorted(all_product_columns)]

    sales_by_product_df = pd.concat(sales_by_product_dfs, ignore_index=True)

    # --- FIX: Standardize Date column to datetime ---
    if 'Date' in sales_by_product_df.columns:
        sales_by_product_df['Date'] = pd.to_datetime(
            sales_by_product_df['Date'], errors='coerce')

    if 'Date' in sales_df.columns:
        sales_df['Date'] = pd.to_datetime(sales_df['Date'], errors='coerce')

    # Standardize Takeout column into boolean
    if 'Take Out' in sales_by_product_df.columns:
        sales_by_product_df['Take Out'] = sales_by_product_df['Take Out'].apply(
            lambda x: 'True' if str(x).strip().upper() == 'Y' else (
                'False' if pd.isna(x) or str(x).strip() == '' else x)
        )

    print("Extract phase completed successfully.")
    return sales_df, sales_by_product_df


def transform(sales_df, sales_by_product_df):
    """Transform and clean the extracted data"""
    print("=== TRANSFORM PHASE ===")
    print("Cleaning Data...")

    # SALES TRANSACTION LIST Cleansing
    # Remove Blank Attributes (only drop columns that actually exist)
    columns_to_drop = [
        'Posted', 'Price Level', 'Branch', 'TM#', 'Customer ID', 'Customer Name', 'Cashier', 'Serviced By', 'Dine In', 'Take Out',
        'Local Tax', 'Amusement Tax', 'EWT', 'NAC', 'Solo Parent', 'Service', 'Feedback Rating', 'Diplomat'
    ]
    existing_columns_to_drop = [
        col for col in columns_to_drop if col in sales_df.columns]
    print(f"Sales Transaction: Dropping columns...")
    sales_df = sales_df.drop(columns=existing_columns_to_drop, errors='ignore')

    # Remove Rows with NaN or empty 'Date' values
    print("Sales Transaction: Removing empty Date and Time rows...")
    if 'Date' in sales_df.columns:
        sales_df = sales_df[sales_df['Date'].notna() & (
            sales_df['Date'].astype(str).str.strip() != '')].reset_index(drop=True)

    # Remove Rows with NaN or empty 'Time' values
    if 'Time' in sales_df.columns:
        sales_df = sales_df[sales_df['Time'].notna() & (
            sales_df['Time'].astype(str).str.strip() != '')].reset_index(drop=True)

    # SALES BY PRODUCT LIST Cleansing
    # Remove Blank Attributes (only drop columns that actually exist)
    product_columns_to_drop = [
        'Lot/Serial', 'Posted', 'TM#', 'Unit', 'Discount ID', 'Discount', '% Discount',
        'Price ID', 'Branch', 'Customer ID', 'Customer'
    ]
    existing_product_columns_to_drop = [
        col for col in product_columns_to_drop if col in sales_by_product_df.columns]
    print(f"Sales Report by Product List: Dropping columns...")
    sales_by_product_df = sales_by_product_df.drop(
        columns=existing_product_columns_to_drop, errors='ignore')

    # Remove Rows with NaN or empty 'Date' values
    print("Sales Report by Product List: Removing empty Date and Time rows...")
    if 'Date' in sales_by_product_df.columns:
        sales_by_product_df = sales_by_product_df[sales_by_product_df['Date'].notna() & (
            sales_by_product_df['Date'].astype(str).str.strip() != '')].reset_index(drop=True)

    # Remove Rows with NaN or empty 'Time' values
    if 'Time' in sales_by_product_df.columns:
        sales_by_product_df = sales_by_product_df[sales_by_product_df['Time'].notna() & (
            sales_by_product_df['Time'].astype(str).str.strip() != '')].reset_index(drop=True)

    # --- REMOVE OUTLIERS AND NEGATIVE VALUES ---
        # Remove products whose Price is greater than 50000 or negative
        if 'Price' in sales_by_product_df.columns:
            sales_by_product_df = sales_by_product_df[
                pd.to_numeric(
                    sales_by_product_df['Price'], errors='coerce').between(0, 50000)
            ].reset_index(drop=True)

        # Remove any rows with negative values in Qty, Line Total, Net Total, or Price
        for col in ['Qty', 'Line Total', 'Net Total', 'Price']:
            if col in sales_by_product_df.columns:
                sales_by_product_df = sales_by_product_df[
                    pd.to_numeric(
                        sales_by_product_df[col], errors='coerce') >= 0
                ].reset_index(drop=True)

# STANDARDIZATION

    # Products with the same name
    if 'Product ID' in sales_by_product_df.columns and 'Product Name' in sales_by_product_df.columns:
        standardization_rules = [
            {'from_ids': ['FDS-2017-0024-W-DCS-BLMW'], 'from_names': ['DOUBLE CHOCOLATE AND STRAWBERRIES'],
                'to_id': '2024waffles4', 'to_name': 'DOUBLE CHOCS AND STRAWBERRIES'},
            {'from_ids': ['FDS-2017-0020-W-BN2-BLMW'], 'from_names': [
                'BANANA NUTELLA WAFFLES'], 'to_id': '2024waffles2', 'to_name': 'BANANA NUTELLA'},
            {'from_ids': ['FDS-2017-0028-S-BCT-BLMW'], 'from_names': ['BACON, COLESLAW AND TOMATO'],
                'to_id': '2024Breads7', 'to_name': 'CLASSIC BACON COLESLAW N TOMATO'},
            {'from_ids': ['FDS-2017-0029-S-TCS-BLMW'], 'from_names': [
                'THE CLUB SANDWICH'], 'to_id': '2024breads', 'to_name': 'THE CKUB'},
            {'from_ids': ['DKS-2018-0034-COOL-PEACH-16-BLMW'], 'from_names': ['PEACH'],
                'to_id': 'DKS-2018-0025-SH-CAMPCH-16-BLMW', 'to_name': 'CAMOMILE PEACH'},
            {'from_ids': ['DKS-2017-0025-SH-16-BLMW'], 'from_names': ['CHAMOMILE PEACH'],
                'to_id': 'DKS-2018-0020-FRAPPE-PCHLUCK-16-BLMW', 'to_name': 'PEACHIEST LUCK'},
            {'from_ids': ['FDS-2018-0001-GARLICBRD--BLMW'], 'from_names': [
                'GARLIC BREAD EXTRA'], 'to_id': '2024BREads9', 'to_name': 'GALIC BREAD ALA CARTE'},
            {'from_ids': ['FDS-2018-0001-BEF-KOR-BLMW'], 'from_names': [
                'KOREAN BEEF BBQ'], 'to_id': '2024lrgplates13', 'to_name': 'K POP BBQ BEEF'},
            {'from_ids': ['2024FDPIZSCREAM'], 'from_names': [
                'SPINACH & CREAM PIZZA'], 'to_id': '2024pizza3', 'to_name': 'SPINACH N CREAM PIZZA'},
            {'from_ids': ['2024PromobundleChix steak'], 'from_names': [
                'CHICKEN STEAK PROMO BUNDLE'], 'to_id': '2024PromoChixsteak', 'to_name': 'CHICKEN STEAK PROMO'},
            {'from_ids': ['FDS-2017-0030-PBB-LONG-BLMW'], 'from_names': ['LONGANISA PBB'],
                'to_id': '2024FilBfast4', 'to_name': 'FIL BFAST LONGGANISA'},
            {'from_ids': ['FDS-2018-0001-MOJOS-BLMW'], 'from_names': ['MOJOS'],
                'to_id': '2024smlplates2', 'to_name': 'MOJOJOJOS'},
            {'from_ids': ['FFDS-2020-VM-DANGGIT-BLMW', 'FDS-2017-0030-PBB-DNGGT-BLMW'], 'from_names': [
                'DANGGIT', 'DANGGIT PBB'], 'to_id': '2024FilBFast3', 'to_name': 'FIL BREAKFAST DANGGIT'},
            {'from_ids': ['FDSS-2018-001-EGG-EXT-BLMW'],
                'from_names': ['EXTRA EGG'], 'to_id': 'ING-EGG', 'to_name': 'EGG'},
            {'from_ids': ['FDS-2017-009-SPAMFRT-BLMW', 'FDS-2017-009-SPAMRI-BLMW', 'FDS-2017-009-SPAMRI-BLMW'], 'from_names': ['SPAM, FRENCH TOAST, EGGS AND HASH BROWN',
                                                                                                                               'SPAM, RICE, EGGS, HASH BROWN', 'SPAM, WAFFLES, EGGS, HASH BROWN'], 'to_id': 'FDS-2017-0010-ADB-S-BLMW', 'to_name': 'SPAM WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-HUNGFRT-BLMW', 'FDS-2017-009-HUNGRI-BLMW', 'FDS-2017-009-HUNGRI-BLMW'], 'from_names': ['HUNGARIAN SAUSAGE, FRENCH TOAST, EGGS AND HASH BROWN',
                                                                                                                               'HUNGARIAN SAUSAGE, RICE, EGGS, HASH BROWN', 'HUNGARIAN SAUSAGE, WAFFLES, EGGS AND HASH BROWN'], 'to_id': 'FDS-2017-0011-ADB-HS-BLMW', 'to_name': 'HUNGARIAN SAUSAGE WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-GERFRT-BLMW', 'FDS-2017-009-GERRI-BLMW', 'FDS-2017-009-GERWAF-BLMW'], 'from_names': ['GERMAN FRANKS, FRENCH TOAST, EGGS AND HASH BROWN',
                                                                                                                             'GERMAN FRANKS, RICE, EGGS AND HASH BROWN', 'GERMAN FRANKS, WAFFLES, EGGS AND HASH BROWN'], 'to_id': 'FDS-2017-0012-ADB-GF-BLMW', 'to_name': 'GERMAN FRANKS WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-CBFRT-BLMW', 'FDS-2017-009-CBRI-BLMW', 'FDS-2017-009-CBWAF-BLMW'], 'from_names': ['CORNED BEEF, FRENCH TOAST, EGGS AND HASH BROWN',
                                                                                                                          'CORNED BEEF, RICE, EGGS, AND HASH BROWN', 'CORNED BEEF, WAFFLES, EGGS AND HASH BROWN'], 'to_id': 'FDS-2017-0013-ADB-CB-BLMW', 'to_name': 'CORNED BEEF WITH RICE OR WAFFLES OR FRENCH TOAST'},
            {'from_ids': ['FDS-2017-009-BAFRT-BLMW', 'FDS-2017-009-BARI-BLMW', 'FDS-2017-009-BAWA-BLMW'], 'from_names': ['BACON, FRENCH TOAST, EGGS, HASH BROWN',
                                                                                                                         'BACON, RICE, EGGS AND HASH BROWN', 'BACON, WAFFLES, EGGS, HASH BROWN'], 'to_id': 'FDS-2017-009-ADB-B-BLMW', 'to_name': 'BACON WITH RICE OR WAFFLES OR FRENCH TOAST'},
        ]

        # Pre-compute uppercase variant name lists for comparison
        for rule in standardization_rules:
            rule['from_names_upper'] = {n.strip().upper()
                                        for n in rule.get('from_names', [])}

        # Apply rules (vectorized masks per rule)
        for rule in standardization_rules:
            mask_id = sales_by_product_df['Product ID'].astype(
                str).isin(rule.get('from_ids', []))
            mask_name = sales_by_product_df['Product Name'].astype(
                str).str.strip().str.upper().isin(rule.get('from_names_upper', set()))
            mask = mask_id | mask_name
            if mask.any():
                sales_by_product_df.loc[mask, 'Product ID'] = rule['to_id']
                sales_by_product_df.loc[mask, 'Product Name'] = rule['to_name']

    # Standardize Product IDs: if same Product Name has multiple IDs, keep the first encountered ID
    if 'Product ID' in sales_by_product_df.columns and 'Product Name' in sales_by_product_df.columns:
        # Create a canonical mapping: first Product ID per cleaned Product Name
        temp_df = sales_by_product_df[['Product Name', 'Product ID']].dropna(
            subset=['Product Name', 'Product ID']).copy()
        # Normalize product name for matching (trim + casefold)
        temp_df['__norm_name'] = temp_df['Product Name'].astype(
            str).str.strip().str.casefold()
        # Get first ID per normalized name
        name_to_first_id = (
            temp_df.drop_duplicates(subset=['__norm_name'])
                   .set_index('__norm_name')['Product ID']
        )

        # Apply mapping using normalized name
        norm_names = sales_by_product_df['Product Name'].astype(
            str).str.strip().str.casefold()
        mapped_ids = norm_names.map(name_to_first_id)

        # Preserve original where mapping not found
        sales_by_product_df['Product ID'] = mapped_ids.fillna(
            sales_by_product_df['Product ID'])

        # Cleanup
        del temp_df

    # Make Product Name uppercase (standardized presentation)
    if 'Product Name' in sales_by_product_df.columns:
        sales_by_product_df['Product Name'] = sales_by_product_df['Product Name'].astype(
            str).str.upper().str.strip()

    # Rename Receipt Attribute of Sales Transaction List to Match Sales By Product List
    if 'Receipt#' in sales_df.columns:
        sales_df = sales_df.rename(columns={'Receipt#': 'Receipt No'})

    print("Merging Sales Transaction and Sales by Product List")
    # Merge the two DataFrames on Receipt Number
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

    # CSV OF CLEANED DATA
    sales_df.to_csv('cleaned_data/sales_transactions.csv', index=False)
    sales_by_product_df.to_csv(
        'cleaned_data/sales_by_product.csv', index=False)

    print("Transform phase completed successfully.")
    return combined_df


def load(combined_df):
    """Load transformed data into final output files"""
    print("=== LOAD PHASE ===")

    # Ensure etl_dimensions directory exists
    os.makedirs('etl_dimensions', exist_ok=True)

    if not combined_df.empty:
        # --- Create time dimension first ---
        if 'Date' in combined_df.columns:
            print("Creating time_dimension.csv...")
            time_dim_df = create_time_dimension(combined_df['Date'])
            time_dim_df.to_csv(
                'etl_dimensions/time_dimension.csv', index=False)
            print("etl_dimensions/time_dimension.csv created")

        # --- Map each transaction's time to its time_id ---
        def time_to_id(time_str):
            try:
                # Accept formats like "13:24" or "1324"
                s = str(time_str).strip()
                if ':' in s:
                    h, m = s.split(':')
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

        # Reorder columns to start with the specified order, replacing 'Time' with 'time_id'
        priority_columns = [
            'Date', 'time_id', 'Receipt No', 'Product ID', 'Product Name',
            'Qty', 'Price', 'Line Total', 'Net Total'
        ]
        existing_priority = [
            col for col in priority_columns if col in combined_df.columns]
        remaining_columns = [
            col for col in combined_df.columns if col not in existing_priority and col != 'Time']
        final_column_order = existing_priority + remaining_columns

        # Remove the original 'Time' column
        if 'Time' in combined_df.columns:
            combined_df = combined_df.drop(columns=['Time'])

        # Reorder the dataframe
        combined_df = combined_df[final_column_order]

        # Remove duplicates from fact transaction dimension
        print("Removing duplicates from fact transaction dimension...")
        combined_df = combined_df.drop_duplicates().reset_index(drop=True)
        print(
            f"After duplicate removal: {len(combined_df)} fact transaction records")

        # --- EXCLUDE LAST MONTH ---
        if 'Date' in combined_df.columns:
            # Ensure Date is datetime
            combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
            # Find the latest month in the data
            latest_month = combined_df['Date'].dt.to_period('M').max()
            # Filter out rows from the latest month
            filtered_df = combined_df[combined_df['Date'].dt.to_period('M') != latest_month].reset_index(drop=True)
        else:
            filtered_df = combined_df

        # Create fact transaction dimension
        filtered_df.to_csv(
            'etl_dimensions/fact_transaction_dimension.csv', index=False)
        print("etl_dimensions/fact_transaction_dimension.csv created")
        # SCD Type 4 Product Dimension (needed to derive parent_sku mapping)
        print("Creating SCD Type 4 Product Dimension tables (and parent_sku mapping)...")
        current_product_dim, history_product_dim = create_product_dimensions(
            combined_df)

        parent_map = {}
        if current_product_dim is not None and 'product_id' in current_product_dim.columns and 'parent_sku' in current_product_dim.columns:
            parent_map = dict(zip(current_product_dim['product_id'].astype(
                str), current_product_dim['parent_sku'].astype(str)))

        # Create transaction_records.csv: one row per receipt with aggregated parent_sku list
        if 'Receipt No' in combined_df.columns and 'Product ID' in combined_df.columns:
            combined_df = combined_df.copy()
            parent_sku_series = (
                combined_df['Product ID'].astype(str)
                .map(parent_map)
                .fillna(combined_df['Product ID'].astype(str))
            )
            combined_df = combined_df.assign(__parent_sku=parent_sku_series)
            transaction_records = (
                combined_df
                .groupby('Receipt No')['__parent_sku']
                .apply(lambda x: ','.join(x.astype(str)))
                .reset_index(name='SKU')
            )
            transaction_records.to_csv(
                'etl_dimensions/transaction_records.csv', index=False)
            print("etl_dimensions/transaction_records.csv created with columns: Receipt No, SKU (parent_sku values)")
            combined_df = combined_df.drop(columns=['__parent_sku'])
        else:
            print("Warning: Could not create transaction_records.csv (missing columns)")

        if current_product_dim is not None and history_product_dim is not None:
            current_product_dim.to_csv(
                'etl_dimensions/current_product_dimension.csv', index=False)
            print("etl_dimensions/current_product_dimension.csv created")
            history_product_dim.to_csv(
                'etl_dimensions/history_product_dimension.csv', index=False)
            print("etl_dimensions/history_product_dimension.csv created")
            print(
                f"Current Product Dimension: {len(current_product_dim)} products")
            print(
                f"History Product Dimension: {len(history_product_dim)} product records")

    else:
        print("Warning: Combined dataframe is empty, no output files created")

    print("Load phase completed successfully.")


def main():
    """Main ETL pipeline orchestrator"""
    print("Starting ETL Pipeline...")

    try:
        # Extract
        sales_df, sales_by_product_df = extract()

        # Transform
        combined_df = transform(sales_df, sales_by_product_df)

        # Load
        load(combined_df)

        print("\nETL Process Complete.")

    except Exception as e:
        print(f"ETL Pipeline failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
