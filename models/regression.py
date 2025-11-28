import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.metrics import mean_squared_error, r2_score
import sys
import os

# Only one association rule file from mba_output
rules_file = ('mba_output/association_rules.csv', 'Output')

try:
    fact_df = pd.read_csv('etl_dimensions/fact_transaction_dimension.csv')
    product_df = pd.read_csv('etl_dimensions/current_product_dimension.csv')
except Exception as e:
    print(f"Error loading data: {e}")
    sys.exit()

file_path, label = rules_file
try:
    rules_df = pd.read_csv(file_path)
except Exception as e:
    print(f"Error loading association rules from {file_path}: {e}")
    sys.exit()

for bundle_index, bundle in rules_df.iterrows():
    product_a_name = bundle['antecedents_names']
    product_b_name = bundle['consequents_names']

    product_a_match = product_df[product_df['product_name'] == product_a_name]
    product_b_match = product_df[product_df['product_name'] == product_b_name]

    if product_a_match.empty or product_b_match.empty:
        print(f"Could not find product IDs for '{product_a_name}' or '{product_b_name}' in {label}.")
        continue

    product_a_id = product_a_match['product_id'].iloc[0]
    product_b_id = product_b_match['product_id'].iloc[0]

    pair_transactions = fact_df[fact_df['Product ID'].isin([product_a_id, product_b_id])]
    receipt_products = pair_transactions.groupby('Receipt No')['Product ID'].apply(set)
    receipts_with_both = receipt_products[
        receipt_products.apply(lambda s: product_a_id in s and product_b_id in s)
    ].index

    if len(receipts_with_both) == 0:
        print(f"No receipts found containing BOTH '{product_a_name}' and '{product_b_name}' in {label}.")
        continue

    ab_transactions = fact_df[
        (fact_df['Receipt No'].isin(receipts_with_both)) &
        (fact_df['Product ID'].isin([product_a_id, product_b_id]))
    ]

    ab_price_per_receipt = ab_transactions.groupby('Receipt No').agg(
        Combined_AB_Price=('Line Total', 'sum'),
        Date=('Date', 'first')
    )

    demand_summary = ab_price_per_receipt.groupby('Combined_AB_Price').agg(
        Num_Transactions=('Date', 'count')
    ).reset_index().sort_values('Combined_AB_Price')

    print(f"\n--- {label} Bundle {bundle_index}: {product_a_name} + {product_b_name} ---")
    if len(demand_summary) > 1:
        X = demand_summary[['Combined_AB_Price']]
        y = demand_summary['Num_Transactions']

        degrees = [1, 2, 3]
        r_squared_values = []

        for degree in degrees:
            poly_model = make_pipeline(PolynomialFeatures(degree), LinearRegression())
            poly_model.fit(X, y)
            r_squared_values.append(poly_model.score(X, y))

        best_degree_index = np.argmax(r_squared_values)
        best_degree = degrees[best_degree_index]

        best_model = make_pipeline(PolynomialFeatures(best_degree), LinearRegression())
        best_model.fit(X, y)

        y_pred = best_model.predict(X)
        mse = mean_squared_error(y, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y, y_pred)
        mae = np.mean(np.abs(y - y_pred))
        wmape = np.sum(np.abs(y - y_pred)) / np.sum(y) * 100
        if len(y) > 1:
            naive_errors = np.abs(y.values[1:] - y.values[:-1])
            mase_denom = np.mean(naive_errors) if np.mean(naive_errors) != 0 else np.nan
            mase = mae / mase_denom if mase_denom else np.nan
        else:
            mase = np.nan

        print(f"Model Evaluation for degree {best_degree}:")
        print(f"  R²: {r2:.4f}")
        print(f"  Mean Squared Error (MSE):       {mse:.4f}")
        print(f"  Root Mean Squared Error (RMSE): {rmse:.4f}")
        print(f"  Mean Absolute Error (MAE):      {mae:.4f}")
        print(f"  Weighted MAPE (WMAPE):          {wmape:.2f}%")
        print(f"  Mean Absolute Scaled Error (MASE): {mase:.4f}")

        min_price = demand_summary['Combined_AB_Price'].min()
        max_price = demand_summary['Combined_AB_Price'].max()
        price_range = max_price - min_price
        extended_min = max(0, min_price - price_range * 0.2)
        extended_max = max_price + price_range * 0.2

        price_values = np.linspace(extended_min, extended_max, 200)
        price_range_df = pd.DataFrame(price_values, columns=['Combined_AB_Price'])
        predicted_demand = best_model.predict(price_range_df)

        plt.figure(figsize=(10, 7))
        plt.scatter(demand_summary['Combined_AB_Price'], demand_summary['Num_Transactions'], color='blue', s=50)
        plt.plot(price_range_df, predicted_demand, color='red', linewidth=2)

        plt.xlim(extended_min, extended_max)
        plt.xlabel('Price (₱)', fontsize=12)
        plt.ylabel('Total Transactions', fontsize=12)
        plt.title(f'({label}) Polynomial Regression: {product_a_name} + {product_b_name} ', fontsize=14)

        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Save plot as image
        results_folder = "regression_results"
        os.makedirs(results_folder, exist_ok=True)
        plot_filename = f"pr_{label.lower()}_bundle_{bundle_index}.png"
        plot_path = os.path.join(results_folder, plot_filename)
        plt.savefig(plot_path)
        plt.show()
    else:
        print("Only one price point found. Cannot fit regression.")