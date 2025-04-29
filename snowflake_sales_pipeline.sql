-- Project: Real-Time Sales Ingestion and Change Tracking Pipeline using Snowflake

-- STEP 1: Create required tables

USE DATABASE RetailDB;
USE SCHEMA Sales;

-- Staging table where raw sales data is loaded
CREATE OR REPLACE TABLE Sales_Staging (
    Sale_ID INT,
    Product STRING,
    Quantity INT,
    Amount FLOAT
);

-- Final table to hold curated sales data
CREATE OR REPLACE TABLE Sales_Final (
    Sale_ID INT,
    Product STRING,
    Quantity INT,
    Amount FLOAT
);

-- Table to simulate raw data errors
CREATE OR REPLACE TABLE Sales_Error_Log (
    FileName STRING,
    ErrorLine STRING,
    ErrorType STRING,
    ErrorTime TIMESTAMP
);

-- STEP 2: Create a stream on the staging table
CREATE OR REPLACE STREAM Sales_Stream ON TABLE Sales_Staging;

-- STEP 3: Create a task that runs daily and captures INSERTs
CREATE OR REPLACE TASK Sales_CDC_Task
  WAREHOUSE = MY_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- daily at midnight UTC
AS
INSERT INTO Sales_Final
SELECT Sale_ID, Product, Quantity, Amount
FROM Sales_Stream
WHERE METADATA$ACTION = 'INSERT';

-- STEP 4: Resume the task to start execution
ALTER TASK Sales_CDC_Task RESUME;

-- STEP 5: Secure view for controlled reporting access
CREATE OR REPLACE SECURE VIEW Sales_Report_View AS
SELECT Product, SUM(Amount) AS Total_Revenue
FROM Sales_Final
GROUP BY Product;

-- STEP 6: Time Travel Example (Recovery)
-- Drop and recover a table (simulate accidental drop)
-- DROP TABLE Sales_Final;
-- UNDROP TABLE Sales_Final;

-- STEP 7: Error Handling Scenario (Preview errors before loading from stage)
-- Assuming file 'broken_sales.csv' has wrong data formats
COPY INTO Sales_Staging
FROM @my_customer_stage/broken_sales.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
VALIDATION_MODE = 'RETURN_ERRORS';

-- Log errors into error log table manually (optional post-check)
INSERT INTO Sales_Error_Log
SELECT 'broken_sales.csv', 'Row 3', 'Invalid INT format', CURRENT_TIMESTAMP;

-- End of Snowflake Sales Pipeline Project
