-- Connect to the 'booklatte' database to ensure tables are created in the correct place.
\c booklatte;

-- Dimensions
CREATE TABLE IF NOT EXISTS current_product_dimension (
    product_id VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(255),
    "Price" DECIMAL(10,2),
    last_transaction_date DATE,
    record_version INT,
    is_current BOOLEAN,
    parent_sku VARCHAR(255),
    "CATEGORY" VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS history_product_dimension (
    product_id VARCHAR(64),
    product_name VARCHAR(255),
    "Price" DECIMAL(10,2),
    last_transaction_date DATE,
    record_version INT,
    is_current VARCHAR(10),
    parent_sku VARCHAR(255),
    "CATEGORY" VARCHAR(64),
    PRIMARY KEY (product_id, record_version)
);

CREATE TABLE IF NOT EXISTS time_dimension (
    time_id VARCHAR(32) PRIMARY KEY,
    time_desc VARCHAR(255),
    time_level INT,
    parent_id VARCHAR(32)
);

-- Market Basket Analysis Tables
CREATE TABLE IF NOT EXISTS transaction_records (
    "Receipt No" INT PRIMARY KEY NOT NULL,
    "SKU" TEXT
);

--Fact Tables   
CREATE TABLE IF NOT EXISTS fact_transaction_dimension (
    "Date" DATE,
    time_id VARCHAR(32),
    "Receipt No" INT,
    "Product ID" VARCHAR(64),
    "Product Name" VARCHAR(255),
    qty INT,
    "Price" DECIMAL(10,2),
    "Line Total" DECIMAL(12,2),
    "Net Total" DECIMAL(12,2),
    discount DECIMAL(12,2),
    evat DECIMAL(12,2),
    pwd DECIMAL(12,2),
    senior DECIMAL(12,2),
    tax DECIMAL(12,2),
    "Total Gross" DECIMAL(12,2),
    void INT,
    "Base Qty" INT,
    "Take Out" VARCHAR(10),
    PRIMARY KEY ("Receipt No", "Date", "Product ID"),
    FOREIGN KEY ("Product ID") REFERENCES current_product_dimension(product_id)
);