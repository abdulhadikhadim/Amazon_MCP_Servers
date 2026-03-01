import pandas as pd

df = pd.read_csv("data/Amazon Sale Report.csv", low_memory=False)

print(f"Total rows: {len(df)}")
print(f"Unique Order IDs: {df['Order ID'].nunique()}")
print(f"\nOrder ID is unique: {len(df) == df['Order ID'].nunique()}")

print(f"\nSample Order ID duplicates:")
order_counts = df['Order ID'].value_counts()
print(order_counts[order_counts > 1].head(10))

print(f"\nColumns in dataset:")
print(df.columns.tolist())

print(f"\nSample data:")
print(df[["Order ID", "SKU", "Qty", "Amount", "Date"]].head(15))
