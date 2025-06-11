CREATE OR REPLACE VIEW extend_time_dimension AS
WITH future_dates AS (
    SELECT DATEADD(day, 1, (SELECT MAX(date_actual) FROM dim_time)) AS date_val
    UNION ALL
    SELECT DATEADD(day, 1, date_val)
    FROM future_dates
    WHERE date_val < DATE '2025-12-31'
)
SELECT 
    TO_NUMBER(TO_CHAR(date_val, 'YYYYMMDD')) AS time_key,
    date_val,
    TO_NUMBER(TO_CHAR(date_val, 'YYYY')) AS year,
    CASE 
        WHEN TO_NUMBER(TO_CHAR(date_val, 'MM')) BETWEEN 1 AND 3 THEN 1
        WHEN TO_NUMBER(TO_CHAR(date_val, 'MM')) BETWEEN 4 AND 6 THEN 2
        WHEN TO_NUMBER(TO_CHAR(date_val, 'MM')) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END AS quarter,
    TO_NUMBER(TO_CHAR(date_val, 'MM')) AS month,
    TO_CHAR(date_val, 'Month') AS month_name,
    WEEK(date_val) AS week,
    TO_NUMBER(TO_CHAR(date_val, 'DD')) AS day_of_month,
    DAYOFWEEK(date_val) AS day_of_week,
    TO_CHAR(date_val, 'Day') AS day_name,
    CASE WHEN DAYOFWEEK(date_val) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    TO_NUMBER(TO_CHAR(date_val, 'YYYY')) AS fiscal_year,
    CASE 
        WHEN TO_NUMBER(TO_CHAR(date_val, 'MM')) BETWEEN 1 AND 3 THEN 1
        WHEN TO_NUMBER(TO_CHAR(date_val, 'MM')) BETWEEN 4 AND 6 THEN 2
        WHEN TO_NUMBER(TO_CHAR(date_val, 'MM')) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END AS fiscal_quarter
FROM future_dates;
