from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
system = sys.path.append(str(ROOT/"src"))
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, write_parquet
from bootcamp_data.transforms import enforce_schema_orders
import pandas as pd
import re

#def assert_unique_key(df: pd.DataFrame, key: str, allow_na = False) -> None:
#    na_count = df[key].isna().sum()
#    dupe_count = df.duplicated(subset=[key]).sum()
#
#    assert na_count == 0, f"There are missing items"
#    assert dupe_count == 0, f"There are duplicate values"
#    
#
#def missingness_report(df):
#    n = len(df)
#    return (
#        df.isna().sum()
#        .rename("n_missing")
#        .to_frame()
#        .assign(p_missing = lambda t:t["n_missing"]/n)
#        .sort_values("p_missing",ascending=False)
#    )
#
#def add_missing_flags(df,cols):
#    out = df.copy()
#    for c in cols:
#        out[f"{c}_isna"] = out[c].isna()
#    return out
#
#

def main() -> None:
    #week2 - day1 "read CSV, enforce schema and turn to parquet. 
    #then print number of rows and datatype of each column"
    paths = make_paths(ROOT)
    orders_csv_path = paths.raw / "orders.csv"
    df = read_orders_csv(orders_csv_path)
    schema = enforce_schema_orders(df)
    parquet = write_parquet(schema, paths.processed / "orders.parquet")

    print(f'Number of Rows: {len(df)}')
    print(f'data types: \n{schema.dtypes}')
    
#    #Week2 - Day2 "method to show if keys have dupes or any missing items"
#    assert_unique_key(df, "order_id", allow_na = False)
#   
#    #Week2 - Day2 "read parquet, write missing report and print first 5"
#    parquet_file = pd.read_parquet(paths.processed / "orders.parquet")
#    t = missingness_report(parquet_file)
#    print(t.head())

if __name__ == "__main__":
    main()