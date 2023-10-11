SELECT 
  DISTINCT(CustomerID) as customer_id,
  Country as customer_country
FROM 
  {{ source('online_retail','raw') }}
WHERE
  CustomerID IS NOT NULL
