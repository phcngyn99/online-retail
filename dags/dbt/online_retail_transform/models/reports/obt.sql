{{ config(materialized='table') }}

SELECT 
    fact.*,
    cus.customer_country,
    dt.datetime,
    dt.year,
    dt.month,
    dt.day,
    dt.weekday,
    product.product_name
FROM
    {{ref("fct_invoice")}} fact 
    JOIN {{ref("dim_customer")}} cus 
        ON fact.customer_id = cus.customer_id
    JOIN {{ref("dim_datetime")}} dt 
        ON fact.datetime_id = dt.datetime_id
    JOIN {{ref("dim_product")}} product 
        ON fact.product_id = product.product_id