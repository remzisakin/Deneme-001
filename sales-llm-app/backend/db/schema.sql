CREATE TABLE IF NOT EXISTS fact_sales (
    date TIMESTAMP,
    order_id VARCHAR,
    product VARCHAR,
    category VARCHAR,
    region VARCHAR,
    customer VARCHAR,
    salesperson VARCHAR,
    quantity DOUBLE,
    unit_price DOUBLE,
    sales_amount DOUBLE,
    currency VARCHAR,
    source_file VARCHAR,
    ingestion_id VARCHAR
);
