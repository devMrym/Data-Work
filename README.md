# Data Work (A project For SDAIA AI Bootcamp):


## Setup
### CMD command to run virtual environment:
```bash
uv venv -p 3.11
```

#### activate venv, then install requirements:
```bash
uv pip install -r requirements.txt
```

## Run ETL
```bash
uv run python scripts/run_etl.py 
```
### After running ETL you should find these files:
1. data/processed/orders_clean.parquet
2. data/processed/users.parquet
3. data/processed/analytics_table.parquet
4. data/processed/_run_meta.json

## EDA
### Open notebooks/eda.ipynb and run all cells:
for any figure in the notebook you can find it in reports/figures

## Summary Report
you can find at reports/summary.md
