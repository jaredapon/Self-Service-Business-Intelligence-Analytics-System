import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import NullFormatter, StrMethodFormatter

os.makedirs('descriptive_results', exist_ok=True)
df = pd.read_csv('etl_dimensions/fact_transaction_dimension.csv', parse_dates=['Date'])
product_dim = pd.read_csv('etl_dimensions/current_product_dimension.csv')
df_merged = df.merge(product_dim, left_on='Product ID', right_on='product_id', how='left')


# Daily sales
daily_sales = df.groupby('Date')['Net Total'].sum()

plt.figure(figsize=(12, 4))
ax = plt.gca()
ax.plot(daily_sales.index, daily_sales.values, color='tab:blue')

ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

ax.xaxis.set_minor_locator(mdates.MonthLocator())
ax.xaxis.set_minor_formatter(NullFormatter())

ax.tick_params(axis='x', which='major', labelsize=10)
ax.tick_params(axis='x', which='minor', labelbottom=False, length=4)

ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

plt.title('Daily Sales Trend')
plt.ylabel('Net Total')
plt.xlabel('Date (years major)')
plt.tight_layout()
plt.savefig('descriptive_results/daily_sales_trend.png')
plt.close()

# Monthly sales
monthly_sales = df.groupby(df['Date'].dt.to_period('M'))['Net Total'].sum()

periods = monthly_sales.index
# keep month labels only (no year in the xtick label so it won't be rotated)
month_labels = [p.to_timestamp().strftime('%b') for p in periods]

years_to_show = [
    (i, int(p.year))
    for i, p in enumerate(periods)
    if (i == 0) or (p.year != periods[i - 1].year)
]

plt.figure(figsize=(14, 5))
ax = plt.gca()
ax.plot(range(len(monthly_sales)), monthly_sales.values, marker='o')

ax.set_xticks(range(len(month_labels)))
ax.set_xticklabels(month_labels, rotation=45, ha='right', rotation_mode='anchor', fontsize=9)

for pos, year in years_to_show:
    ax.text(pos, -0.12, str(year), transform=ax.get_xaxis_transform(), ha='center', va='top', fontsize=9)

ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

plt.gcf().subplots_adjust(bottom=0.45)

plt.title('Monthly Sales Trend')
plt.ylabel('Net Total')
ax.set_xlabel('Month', labelpad=20)
plt.tight_layout()
plt.savefig('descriptive_results/monthly_sales_trend.png')
plt.close()


# Quarterly sales
quarterly_sales = df.groupby(df['Date'].dt.to_period('Q'))['Net Total'].sum()

periods = quarterly_sales.index
labels = []
for i, p in enumerate(periods):
    show_year = (i == 0) or (p.year != periods[i - 1].year)
    year_str = str(int(p.year)) if show_year else ''
    labels.append(f"Q{p.quarter}\n{year_str}")

plt.figure(figsize=(12, 4))
plt.plot(range(len(quarterly_sales)), quarterly_sales.values, marker='o')
ax = plt.gca()
ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
plt.xticks(range(len(labels)), labels, rotation=0, ha='center')
plt.title('Quarterly Sales Trend')
plt.ylabel('Net Total')
plt.xlabel('Quarter')
plt.tight_layout()
plt.savefig('descriptive_results/quarterly_sales_trend.png')
plt.close()


# Annual sales
annual_sales = df.groupby(df['Date'].dt.year)['Net Total'].sum()
years = annual_sales.index.astype(int).astype(str)

plt.figure(figsize=(12, 4))
plt.plot(years, annual_sales.values, marker='o')
ax = plt.gca()
ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
plt.title('Annual Sales Trend')
plt.ylabel('Net Total')
plt.xlabel('Year')
plt.tight_layout()
plt.savefig('descriptive_results/annual_sales_trend.png')
plt.close()

# Transaction Count vs. Revenue (Monthly)
monthly_group = df.groupby(df['Date'].dt.to_period('M'))
monthly_revenue = monthly_group['Net Total'].sum()
monthly_count = monthly_group.size()

fig, ax1 = plt.subplots(figsize=(12, 5))

months = monthly_count.index.astype(str)
ax1.bar(months, monthly_count, color='lightblue', label='Transaction Count')
ax1.set_ylabel('Transaction Count', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')
ax1.set_xlabel('Month')

step = 3
ax1.set_xticks(months[::step])
ax1.set_xticklabels(months[::step], rotation=45, ha='right')

ax2 = ax1.twinx()
ax2.plot(months, monthly_revenue, color='red', marker='o', label='Revenue')
ax2.set_ylabel('Revenue', color='red')
ax2.tick_params(axis='y', labelcolor='red')

plt.title('Monthly Transaction Count vs. Revenue')
fig.tight_layout()
plt.savefig('descriptive_results/monthly_transaction_vs_revenue.png')
plt.close()

# Top 10 FOOD by revenue
food_revenue = (
    df_merged[df_merged['CATEGORY'] == 'FOOD']
    .groupby('Product Name')['Net Total']
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

# Top 10 FOOD by revenue (horizontal bar)
plt.figure(figsize=(10, 6))
food_revenue.plot(kind='barh', color='orange')
plt.title('Top 10 Food Products by Revenue')
plt.xlabel('Revenue')
plt.ylabel('Product Name')
plt.tight_layout()
plt.savefig('descriptive_results/top10_food_by_revenue.png')
plt.close()

# Top 10 DRINK by revenue
drink_revenue = (
    df_merged[df_merged['CATEGORY'] == 'DRINK']
    .groupby('Product Name')['Net Total']
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

# Top 10 DRINK by revenue (horizontal bar)
plt.figure(figsize=(10, 6))
drink_revenue.plot(kind='barh', color='skyblue')
plt.title('Top 10 Drink Products by Revenue')
plt.xlabel('Revenue')
plt.ylabel('Product Name')
plt.tight_layout()
plt.savefig('descriptive_results/top10_drink_by_revenue.png')
plt.close()

# Top 10 FOOD by quantity sold
food_qty = (
    df_merged[df_merged['CATEGORY'] == 'FOOD']
    .groupby('Product Name')['Qty']
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 6))
food_qty.plot(kind='barh', color='green')
plt.title('Top 10 Food Products by Quantity Sold')
plt.xlabel('Quantity Sold')
plt.ylabel('Product Name')
plt.tight_layout()
plt.savefig('descriptive_results/top10_food_by_quantity.png')
plt.close()

# Top 10 DRINK by quantity sold
drink_qty = (
    df_merged[df_merged['CATEGORY'] == 'DRINK']
    .groupby('Product Name')['Qty']
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 6))
drink_qty.plot(kind='barh', color='purple')
plt.title('Top 10 Drink Products by Quantity Sold')
plt.xlabel('Quantity Sold')
plt.ylabel('Product Name')
plt.tight_layout()
plt.savefig('descriptive_results/top10_drink_by_quantity.png')
plt.close()

# Revenue by CATEGORY
category_revenue = (
    df_merged.groupby('CATEGORY')['Net Total']
    .sum()
    .sort_values(ascending=False)
)

plt.figure(figsize=(8, 5))
category_revenue.plot(
    kind='pie',
    color='teal',
    autopct='%1.1f%%'
)
plt.title('Revenue by Category')
plt.savefig('descriptive_results/revenue_by_category.png')
plt.close()