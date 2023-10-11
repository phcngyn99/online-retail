{{ config(materialized='table') }}
SELECT
  DISTINCT {{ dbt_utils.generate_surrogate_key(['CustomerID','Country']) }} as customer_id,
  Country as customer_country
FROM 
  {{ source('online_retail','raw') }}
WHERE
  Quantity*UnitPrice != 0
