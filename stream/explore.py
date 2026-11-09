import pandas as pd

# 1. Load the dataset
df = pd.read_hdf('pems-bay.h5', key='speed')

# 2. Inspect Overview (Works fine)
print("--- DATASET OVERVIEW ---")
print(f"Shape of dataset (Rows, Columns): {df.shape}")
print(f"Total time steps (Rows): {len(df.index)}")
print(f"Total sensors/nodes (Columns): {len(df.columns)}")

# 3. FIX FOR THE ERROR: Safely access the index values without triggering .freqstr
print("\n--- SAMPLE TIMESTAMPS (axis0) ---")
# Convert just the first few elements to a plain string array to view them safely
sample_timestamps = df.index[:5].astype(str)
print(sample_timestamps)

print("\n--- SAMPLE SENSOR IDs / NODES (axis1) ---")
print(list(df.columns[:5]))

print("\n--- FIRST 5 ROWS AND COLUMNS OF TRAFFIC SPEEDS ---")
# Using .values strips pandas-specific formatting overhead to display safely
print(df.iloc[:5, :5].values)
