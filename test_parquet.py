import pandas as pd
from pathlib import Path

p = Path(r"C:\Users\HP\OneDrive\Desktop\m-cube_version1\catalog\data\bar\EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL")
files = list(p.glob("*.parquet"))
if files:
    print(f"Found {len(files)} files")
    df = pd.read_parquet(files[0])
    print("Columns:", df.columns.tolist())
    print("\nDtypes:")
    print(df.dtypes)
    print("\nShape:", df.shape)
    print("\nFirst row (sample data):")
    for col in df.columns:
        print(f"  {col}: {df.iloc[0][col]} (type: {type(df.iloc[0][col]).__name__})")
else:
    print("No parquet files found")
