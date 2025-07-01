import pandas as pd

# Load cleaned sales data
df = pd.read_csv('cleaned_sales_data.csv')

# Calculate revenue and profit
df['revenue'] = df['quantity'] * df['price']
df['profit'] = df['revenue'] - df['cost']

# Group by store to get summary
store_summary = df.groupby('store_id').agg(
    total_revenue=('revenue', 'sum'),
    total_profit=('profit', 'sum'),
    total_sales=('quantity', 'sum')
).reset_index()

# Save summary
store_summary.to_csv('store_summary.csv', index=False)

# Find top 5 lowest performing stores by revenue
low_perf_stores = store_summary.sort_values('total_revenue').head(5)

# Save low performing stores report
low_perf_stores.to_csv('low_performing_stores.csv', index=False)

print("ETL pipeline executed successfully. Outputs saved.")
