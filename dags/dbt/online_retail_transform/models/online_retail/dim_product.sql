{{ config(materialized='table') }}
SELECT
    DISTINCT {{ dbt_utils.generate_surrogate_key(['StockCode','Description','UnitPrice']) }} AS product_id,
    Description AS product_name,
    UnitPrice AS product_price
FROM
    {{ source('online_retail','raw') }}
WHERE
    StockCode IS NOT NULL AND UnitPrice > 0
