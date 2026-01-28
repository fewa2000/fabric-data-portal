```python
import pandas as pd

input_path = "/lakehouse/default/Files/import/sales_orders_fact.xlsx"
output_dir = "/lakehouse/default/Files/results"

output_parquet = f"{output_dir}/sales_cleaned.parquet"
output_csv = f"{output_dir}/sales_cleaned.csv"

# Read Excel from OneLake
df = pd.read_excel(input_path)

print("Rows:", len(df))
display(df.head(10))

# Simple normalization
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# Write cleaned data back to OneLake
df.to_parquet(output_parquet, index=False)
df.to_csv(output_csv, index=False)

print("Written:")
print(output_parquet)
print(output_csv)
```