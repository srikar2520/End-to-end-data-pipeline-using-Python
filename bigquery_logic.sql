/*
Project: Real-Time Retail Data Ingestion Pipeline
File: bigquery_logic.sql
Description: Schema definition and data quality validation queries for the retail_dataset.
*/

-- =============================================
-- 1. Table Schema Definition (DDL)
-- =============================================
CREATE OR REPLACE TABLE `retail_dataset.raw_orders_5k` (
    order_id STRING,
    timestamp TIMESTAMP,
    customer_id STRING,
    store_city STRING,
    item_id STRING,
    price FLOAT64,
    payment_method STRING,
    data_quality_tag STRING
);

-- =============================================
-- 2. Data Quality Analysis (Trust Score Logic)
-- =============================================
-- Break down records by quality tag to calculate the "Trust Score"
SELECT 
    data_quality_tag,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage_share
FROM 
    `retail_dataset.raw_orders_5k`
GROUP BY 
    data_quality_tag
ORDER BY 
    record_count DESC;

-- =============================================
-- 3. Revenue Risk Assessment (Financial Impact)
-- =============================================
-- Calculate total revenue blocked due to data quality issues
SELECT 
    SUM(price) as total_revenue_at_risk
FROM 
    `retail_dataset.raw_orders_5k`
WHERE 
    data_quality_tag != 'CLEAN';

-- =============================================
-- 4. High-Priority Error Log
-- =============================================
-- Extract high-value errors (> $1500) for immediate engineering review
SELECT 
    order_id,
    store_city,
    price,
    data_quality_tag,
    timestamp
FROM 
    `retail_dataset.raw_orders_5k`
WHERE 
    data_quality_tag != 'CLEAN' 
    AND price > 1500
ORDER BY 
    price DESC;