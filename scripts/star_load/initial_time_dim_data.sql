INSERT INTO dim_time (
    time_key,
    date_actual,
    year,
    quarter,
    month,
    month_name,
    week,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    fiscal_year,
    fiscal_quarter
)
SELECT 
    CAST(TO_CHAR(date_val, 'YYYYMMDD') AS INTEGER) as time_key,
    date_val,
    CAST(TO_CHAR(date_val, 'YYYY') AS INTEGER) as year,
    CASE 
        WHEN CAST(TO_CHAR(date_val, 'MM') AS INTEGER) BETWEEN 1 AND 3 THEN 1
        WHEN CAST(TO_CHAR(date_val, 'MM') AS INTEGER) BETWEEN 4 AND 6 THEN 2
        WHEN CAST(TO_CHAR(date_val, 'MM') AS INTEGER) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END as quarter,
    CAST(TO_CHAR(date_val, 'MM') AS INTEGER) as month,
    TO_CHAR(date_val, 'Month') as month_name,
    WEEK(date_val) as week,
    CAST(TO_CHAR(date_val, 'DD') AS INTEGER) as day_of_month,
    DAYOFWEEK(date_val) as day_of_week,
    TO_CHAR(date_val, 'Day') as day_name,
    CASE WHEN DAYOFWEEK(date_val) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
    CAST(TO_CHAR(date_val, 'YYYY') AS INTEGER) as fiscal_year,
    CASE 
        WHEN CAST(TO_CHAR(date_val, 'MM') AS INTEGER) BETWEEN 1 AND 3 THEN 1
        WHEN CAST(TO_CHAR(date_val, 'MM') AS INTEGER) BETWEEN 4 AND 6 THEN 2
        WHEN CAST(TO_CHAR(date_val, 'MM') AS INTEGER) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END as fiscal_quarter
FROM (
    WITH RECURSIVE date_series AS (
        SELECT DATE '2020-01-01' AS date_val
        UNION ALL
        SELECT date_val + INTERVAL '1 day'
        FROM date_series
        WHERE date_val < DATE '2025-12-31'
    )
    SELECT date_val FROM date_series
) AS dates;
