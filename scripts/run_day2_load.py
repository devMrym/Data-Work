from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
system = sys.path.append(str(ROOT/"src"))
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, write_parquet,read_users_csv
from bootcamp_data.transforms import enforce_schema_orders, enforce_schema_users, missingness_report, normalize_text, add_missing_flags, apply_mapping
from bootcamp_data.quality import require_columns,assert_non_empty
import pandas as pd


def main() -> None:

#1. loads raw CSVs (orders + users)

    paths = make_paths(ROOT)
    orders_csv_path = paths.raw / "orders.csv"
    users_csv_path = paths.raw / "users.csv"
    df_orders = read_orders_csv(orders_csv_path)
    df_users = read_orders_csv(users_csv_path)
    print(f"Number Of rows: \norders_raw: {len(df_orders)} \nusers_raw: {len(df_users)} ")


#2. runs basic checks (columns + non-empty)

    require_columns(df_orders,["order_id","user_id","amount","quantity","created_at","status"])
    require_columns(df_users, ["user_id","country","signup_date"])
    assert_non_empty(df_orders,"df_orders")
    assert_non_empty(df_users,"df_users")

#3. enforces schema (from Day 1)
    schema_orders = enforce_schema_orders(df_orders)
    schema_users = enforce_schema_users(df_users)

#4. creates a missingness report and saves it to reports/
    orders_missing_report = missingness_report(df_orders)
    users_missing_report = missingness_report(df_users)

    orders_missing_report.to_csv(paths.reports / "orders_missing.csv", index=True)
    users_missing_report.to_csv(paths.reports / "users_missing.csv", index=True)

    #write_parquet(orders_missing_report, paths.reports / "orders_missing.parquet")
    #write_parquet(users_missing_report, paths.reports / "users_missing.parquet")
    

#5. normalizes status into status_clean + adds missing flags for amount and quantity
    status_norm = normalize_text(schema_orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = (
        schema_orders.assign(status_clean=status_clean)
        .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

    


#7. writes orders_clean.parquet
    
    write_parquet(orders_clean, paths.processed / "orders_clean.parquet")
    write_parquet(schema_users, paths.processed / "users.parquet")

    print(pd.read_parquet(paths.processed / "orders_clean.parquet"))
    print(pd.read_parquet(paths.processed / "users.parquet"))

if __name__ == "__main__":
    main()