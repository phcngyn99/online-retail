{{ config(materialized='table') }}
WITH R1 AS (
  SELECT
    DISTINCT(InvoiceDate) AS datetime_id,
    PARSE_DATETIME("%m/%d/%y %H:%M", InvoiceDate) AS datetime
  FROM
    {{ source('online_retail','raw') }}
  WHERE
    InvoiceDate IS NOT NULL
)
SELECT
  datetime_id,
  datetime,
  EXTRACT(YEAR FROM datetime) AS year,
  EXTRACT(MONTH FROM datetime) AS month,
  EXTRACT(DAY FROM datetime) AS day,
  EXTRACT(HOUR FROM datetime) AS hour,
  EXTRACT(MINUTE FROM datetime) AS minute,
  FORMAT_DATE("%a", datetime) AS weekday
FROM R1