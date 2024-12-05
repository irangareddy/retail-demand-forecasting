```sql
-- Create Database
CREATE DATABASE IF NOT EXISTS RETAIL_DATA_WAREHOUSE;

-- Create Schema
CREATE SCHEMA IF NOT EXISTS RETAIL_DATA_WAREHOUSE.RAW_DATA;

-- Create Table
CREATE TABLE IF NOT EXISTS RETAIL_DATA_WAREHOUSE.RAW_DATA.RETAIL_SALES (
    month VARCHAR(10) NOT NULL,
    state VARCHAR(2) NOT NULL,
    category_445_sales FLOAT NOT NULL,
    category_445_share FLOAT NOT NULL,
    category_448_sales FLOAT NOT NULL,
    category_448_share FLOAT NOT NULL,
    national_445_total FLOAT NOT NULL,
    national_448_total FLOAT NOT NULL,
    last_updated TIMESTAMP_NTZ NOT NULL,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (month, state)
);
```

## Table: RETAIL_SALES

This table stores monthly retail sales data from the Census Bureau's API for different states and retail categories.

### Columns

- `month`: Month of the data (format: YYYY-MM)
- `state`: Two-letter state code (e.g., CA, NY)
- `category_445_sales`: Sales value for Food and Beverage Stores (NAICS code 445)
- `category_445_share`: State's share of national sales for Food and Beverage Stores
- `category_448_sales`: Sales value for Clothing and Accessories Stores (NAICS code 448)
- `category_448_share`: State's share of national sales for Clothing Stores
- `national_445_total`: National total sales for Food and Beverage Stores
- `national_448_total`: National total sales for Clothing Stores
- `last_updated`: Timestamp when the data was last updated in the Census API
- `loaded_at`: Timestamp when the record was loaded into Snowflake

### Key Features

- Primary key on (month, state) ensures no duplicate entries
- Uses FLOAT for monetary values to handle large sales numbers
- Includes both state-level and national-level metrics
- Tracks data lineage with timestamps
- Raw data storage in RETAIL_DATA_WAREHOUSE database

The table structure supports tracking retail sales trends over time and comparing state performance against national totals.
