from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
system = sys.path.append(str(ROOT/"src"))
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, write_parquet
from bootcamp_data.transforms import enforce_schema


paths = make_paths(ROOT)
orders_csv_path = paths.raw / "orders.csv"
df = read_orders_csv(orders_csv_path)
schema = enforce_schema(df)
parquet = write_parquet(schema, paths.processed / "orders.parquet")

print(f'Number of Rows: {len(df)}')
print(f'data types: \n{schema.dtypes}')