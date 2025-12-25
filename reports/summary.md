# Summary Report — ETL + EDA
## Key findings
⋅⋅⋅⋅* Market Dominance: Saudi Arabia (SA) is the leading market, generating over $2,500 in total revenue, which is roughly 4x the revenue of the UAE (AE).

⋅⋅⋅⋅* Heavy Reliance on "Whales": A small number of high-value orders (e.g., $200+) significantly drive total revenue, while the vast majority (40+ orders) are small purchases under $20.

⋅⋅⋅⋅* Weekly Peak: Revenue is highest on Thursdays (over $600) and lowest on Saturdays (under $200).

⋅⋅⋅⋅* Refund Variance: The average refund rate in SA is 9% lower than in AE, though this difference is not statistically certain due to the small sample size in the UAE.

## Definitions
⋅⋅⋅⋅* Revenue: The sum of the amount column for all orders within a specific category or time frame.

⋅⋅⋅⋅* Refund Rate: The number of orders with status_clean == "refund" divided by the total number of orders.

⋅⋅⋅⋅* Winsorized Amount: A version of the amount column where extreme outliers are capped to prevent them from overly distorting the data visual.

⋅⋅⋅⋅* Time Window: The dataset spans from December 1, 2025, to January 21, 2026.

## Data quality caveats
⋅⋅⋅⋅* Missingness: The ETL pipeline identified several records with empty values for amount and quantity (flagged as amount_isna and quantity_isna). Additionally, several orders are missing created_at timestamps, preventing them from appearing in daily or hourly revenue trends.

⋅⋅⋅⋅* Join coverage: The pipeline uses a safe_left_join to connect orders to users. Any order with a user_id that does not exist in the user database will result in a "null" country, which lowers the accuracy of the Revenue by Country chart.

⋅⋅⋅⋅* Outliers: We use a winsorize function to cap extreme values and an add_outlier_flag to identify "Big Spenders". For example, orders like A0024 ($200) are flagged as outliers because they are significantly higher than the typical $10–$20 purchase, which can skew average calculations.