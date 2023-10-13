{{ config(materialized='table') }}
WITH fact AS
(
    SELECT
        InvoiceNo AS invoice_id,
        InvoiceDate AS datetime_id,
        {{ dbt_utils.generate_surrogate_key(['CustomerID','Country']) }} AS customer_id,
        StockCode AS product_id,
        Quantity * UnitPrice AS total_value,
        Quantity AS quantity,
        CASE
            WHEN UPPER(LEFT(InvoiceNo,1)) = "C" THEN "Cancel"
            ELSE "Succeed"
        END AS status
    FROM
        {{ source('online_retail','raw') }}
    WHERE Quantity * UnitPrice != 0
)
SELECT
    fact.invoice_id,
    dim_datetime.datetime_id,
    dim_customer.customer_id,
    dim_product.product_id,
    fact.total_value,
    fact.quantity,
    fact.status
FROM 
    fact 
        JOIN {{ref("dim_datetime")}} dim_datetime
        ON fact.datetime_id = dim_datetime.datetime_id
        JOIN {{ref("dim_customer")}} dim_customer
        ON fact.customer_id = dim_customer.customer_id
        JOIN {{ref("dim_product")}} dim_product
        ON fact.product_id = dim_product.product_id