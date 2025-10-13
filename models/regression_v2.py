import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.metrics import mean_squared_error, r2_score
import sys
import os

rules_files = [
    ('mba_meal/association_rules.csv', 'Meal'),
    ('mba_foods/association_rules.csv', 'Foods'),
    ('mba_drinks/association_rules.csv', 'Drinks')
]

try:
    fact_df = pd.read_csv('etl_dimensions/fact_transaction_dimension.csv')
    product_df = pd.read_csv('etl_dimensions/current_product_dimension.csv')
except Exception as e:
    print(f"Error loading data: {e}")
    sys.exit()

for file_path, label in rules_files:
    try:
        rules_df = pd.read_csv(file_path)
        if rules_df.empty:
            print(f"No association rules found in {file_path}.")
            continue
        bundle_index = 0
        bundle = rules_df.iloc[bundle_index]
        product_a_name = bundle['antecedents_names']
        product_b_name = bundle['consequents_names']
    except Exception as e:
        print(f"Error loading association rules from {file_path}: {e}")
        continue

    product_a_match = product_df[product_df['product_name'] == product_a_name]
    product_b_match = product_df[product_df['product_name'] == product_b_name]

    if product_a_match.empty or product_b_match.empty:
        print(f"Could not find product IDs for '{product_a_name}' or '{product_b_name}' in {label}.")
        continue

    product_a_id = product_a_match['product_id'].iloc[0]
    product_b_id = product_b_match['product_id'].iloc[0]
    
    # Get the latest price from the current product dimension
    price_a = product_a_match['Price'].iloc[0]
    price_b = product_b_match['Price'].iloc[0]
    latest_bundle_price = price_a + price_b

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

    print(f"\n--- {label} Top Bundle: {product_a_name} + {product_b_name} ---")
    if len(demand_summary) > 1:
        X = demand_summary[['Combined_AB_Price']]
        y = demand_summary['Num_Transactions']

        degrees = [1, 2, 3]
        best_model = None
        best_r2 = -np.inf
        best_degree = 0

        for degree in degrees:
            model = make_pipeline(PolynomialFeatures(degree), LinearRegression())
            model.fit(X, y)
            r2 = model.score(X, y)
            if r2 > best_r2:
                best_r2 = r2
                best_model = model
                best_degree = degree

        y_pred = best_model.predict(X)
        mse = mean_squared_error(y, y_pred)
        rmse = np.sqrt(mse)
        wmape = np.sum(np.abs(y - y_pred)) / np.sum(np.abs(y)) * 100

        print(f"Model Evaluation for degree {best_degree}:")
        print(f"  R²: {best_r2:.4f}")
        print(f"  MSE: {mse:.4f}")
        print(f"  RMSE: {rmse:.4f}")
        print(f"  WMAPE: {wmape:.4f}%")

        min_price = demand_summary['Combined_AB_Price'].min()
        max_price = demand_summary['Combined_AB_Price'].max()
        price_range = max_price - min_price
        extended_min = max(0, min_price - price_range * 0.2)
        extended_max = max_price + price_range * 0.2

        price_values = np.linspace(extended_min, extended_max, 200)
        price_range_df = pd.DataFrame(price_values, columns=['Combined_AB_Price'])
        predicted_demand = best_model.predict(price_range_df)

        plt.figure(figsize=(10, 7))
        plt.scatter(X, y, color='blue', s=50, label='Actual Data')
        plt.plot(price_range_df, predicted_demand, color='red', linewidth=2, label=f'Polynomial Fit (degree={best_degree})')

        plt.xlim(extended_min, extended_max)
        plt.xlabel('Price (₱)', fontsize=12)
        plt.ylabel('Total Transactions', fontsize=12)
        plt.title(f'({label}) Polynomial Regression: {product_a_name} + {product_b_name} ', fontsize=14)
        plt.legend()
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.tight_layout()

        results_folder = "regression_results"
        os.makedirs(results_folder, exist_ok=True)
        plot_filename = f"pr_{label.lower()}_bundle_{bundle_index}.png"
        plot_path = os.path.join(results_folder, plot_filename)
        plt.savefig(plot_path)
        plt.show()

        # --- Interactive "What-If" Analysis ---
        print("\n--- Interactive Demand Prediction ---")
        print(f"Reference: The current bundle price is ₱{latest_bundle_price:.2f}")
        
        # Predict demand at the latest price to use as a baseline
        price_df = pd.DataFrame([[latest_bundle_price]], columns=['Combined_AB_Price'])
        demand_at_latest_price = best_model.predict(price_df)[0]
        print(f"Predicted transactions at this price: {demand_at_latest_price:.0f}")

        while True:
            try:
                new_price_str = input("\nEnter a new price to predict demand (or press Enter to quit): ")
                if not new_price_str:
                    break
                
                new_price = float(new_price_str)
                price_df = pd.DataFrame([[new_price]], columns=['Combined_AB_Price'])
                predicted_demand_new = best_model.predict(price_df)[0]
                
                if demand_at_latest_price > 0:
                    percent_change = ((predicted_demand_new - demand_at_latest_price) / demand_at_latest_price) * 100
                    change_str = f", a change of {percent_change:+.2f}%"
                else:
                    change_str = " (percentage change cannot be calculated from zero baseline)."

                print(f"-> If we set the price at ₱{new_price:.2f}, there will be ~{max(0, predicted_demand_new):.0f} transactions{change_str}")

            except ValueError:
                print("Invalid input. Please enter a number.")
            except Exception as e:
                print(f"An error occurred: {e}")
                break

    else:
        print("Only one price point found. Cannot fit regression.")