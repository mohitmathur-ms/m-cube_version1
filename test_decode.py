import pandas as pd
from pathlib import Path
from nautilus_trader.model.objects import Price, Quantity

# Load a sample
p = Path(r"C:\Users\HP\OneDrive\Desktop\m-cube_version1\catalog\data\bar\EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL")
files = list(p.glob("*.parquet"))
df = pd.read_parquet(files[0])

# Decode first row
row = df.iloc[0]
print("Decoding row 0:")

# Price and Quantity use from_bytes classmethod
open_price = Price.from_bytes(row["open"])
print(f"Open: {open_price} (type: {type(open_price).__name__})")
print(f"Open float: {float(open_price)}")

high_price = Price.from_bytes(row["high"])
print(f"High: {high_price} -> {float(high_price)}")

volume_qty = Quantity.from_bytes(row["volume"])
print(f"Volume: {volume_qty} -> {float(volume_qty)}")

# Show it's consistent
print(f"\nCheck: {float(open_price):.10f}")
