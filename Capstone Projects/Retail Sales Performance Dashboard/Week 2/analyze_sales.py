import pandas as pd
import numpy as np

# 1. Load Data
df = pd.read_csv('python/sales_data.csv')  # Update path as needed

# 2. Clean Missing Values
df.dropna(inplace=True)  # Or use df.fillna() if needed

# 3. Convert Data Types (if necessary)
df['quantity'] = df['quantity'].astype(int)
df['price'] = df['price'].astype(float)
df['cost'] = df['cost'].astype(float)

# 4. Add Calculated Fields
df['revenue'] = df['quantity'] * df['price']
df['profit'] = df['revenue'] - (df['quantity'] * df['cost'])
df['discount_percent'] = ((df['price'] - df['cost']) / df['price']) * 100

# 5. Summarize Total Revenue & Profit by Store and Product
summary_by_store = df.groupby('store_id')[['revenue', 'profit']].sum()
summary_by_product = df.groupby('product_id')[['revenue', 'profit']].sum()

# 6. Save Cleaned Dataset
df.to_csv('python/cleaned_data.csv', index=False)

# 7. Print Summary
print("Summary by Store:")
print(summary_by_store)
print("\nSummary by Product:")
print(summary_by_product)
