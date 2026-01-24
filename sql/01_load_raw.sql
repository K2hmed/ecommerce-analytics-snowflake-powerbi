-- ============================================================
-- 01_load_raw.sql
-- Loads CSVs from @"ECOM_DB"."RAW"."STG_OLIST"/<file>.csv
-- Uses COPY transforms (no MATCH_BY_COLUMN_NAME, no PARSE_HEADER)
-- Safe to re-run (TRUNCATE + reload)
-- ============================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE ECOM_DB;
USE SCHEMA RAW;

-- 0) Confirm stage contents
LIST @ECOM_DB.RAW.STG_OLIST;

-- 1) Quick preview read (should return rows)
SELECT $1, $2, $3
FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_orders_dataset.csv
(FILE_FORMAT => 'ECOM_DB.RAW.FF_OLIST_CSV')
LIMIT 5;

-- ------------------------------------------------------------
-- TRUNCATE
-- ------------------------------------------------------------
TRUNCATE TABLE ORDERS_RAW;
TRUNCATE TABLE ORDER_ITEMS_RAW;
TRUNCATE TABLE PAYMENTS_RAW;
TRUNCATE TABLE REVIEWS_RAW;
TRUNCATE TABLE CUSTOMERS_RAW;
TRUNCATE TABLE SELLERS_RAW;
TRUNCATE TABLE PRODUCTS_RAW;
TRUNCATE TABLE GEOLOCATION_RAW;
TRUNCATE TABLE CATEGORY_TRANSLATION_RAW;

-- ------------------------------------------------------------
-- ORDERS
-- ------------------------------------------------------------
COPY INTO ORDERS_RAW
FROM (
  SELECT
    $1::STRING                         AS ORDER_ID,
    $2::STRING                         AS CUSTOMER_ID,
    $3::STRING                         AS ORDER_STATUS,
    TRY_TO_TIMESTAMP_NTZ($4)           AS ORDER_PURCHASE_TIMESTAMP,
    TRY_TO_TIMESTAMP_NTZ($5)           AS ORDER_APPROVED_AT,
    TRY_TO_TIMESTAMP_NTZ($6)           AS ORDER_DELIVERED_CARRIER_DATE,
    TRY_TO_TIMESTAMP_NTZ($7)           AS ORDER_DELIVERED_CUSTOMER_DATE,
    TRY_TO_TIMESTAMP_NTZ($8)           AS ORDER_ESTIMATED_DELIVERY_DATE,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_orders_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- ORDER ITEMS
-- ------------------------------------------------------------
COPY INTO ORDER_ITEMS_RAW
FROM (
  SELECT
    $1::STRING                         AS ORDER_ID,
    TRY_TO_NUMBER($2)                  AS ORDER_ITEM_ID,
    $3::STRING                         AS PRODUCT_ID,
    $4::STRING                         AS SELLER_ID,
    TRY_TO_TIMESTAMP_NTZ($5)           AS SHIPPING_LIMIT_DATE,
    TRY_TO_NUMBER($6, 38, 2)           AS PRICE,
    TRY_TO_NUMBER($7, 38, 2)           AS FREIGHT_VALUE,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_order_items_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- PAYMENTS
-- ------------------------------------------------------------
COPY INTO PAYMENTS_RAW
FROM (
  SELECT
    $1::STRING                         AS ORDER_ID,
    TRY_TO_NUMBER($2)                  AS PAYMENT_SEQUENTIAL,
    $3::STRING                         AS PAYMENT_TYPE,
    TRY_TO_NUMBER($4)                  AS PAYMENT_INSTALLMENTS,
    TRY_TO_NUMBER($5, 38, 2)           AS PAYMENT_VALUE,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_order_payments_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- REVIEWS
-- ------------------------------------------------------------
COPY INTO REVIEWS_RAW
FROM (
  SELECT
    $1::STRING                         AS REVIEW_ID,
    $2::STRING                         AS ORDER_ID,
    TRY_TO_NUMBER($3)                  AS REVIEW_SCORE,
    $4::STRING                         AS REVIEW_COMMENT_TITLE,
    $5::STRING                         AS REVIEW_COMMENT_MESSAGE,
    TRY_TO_TIMESTAMP_NTZ($6)           AS REVIEW_CREATION_DATE,
    TRY_TO_TIMESTAMP_NTZ($7)           AS REVIEW_ANSWER_TIMESTAMP,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_order_reviews_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- CUSTOMERS
-- ------------------------------------------------------------
COPY INTO CUSTOMERS_RAW
FROM (
  SELECT
    $1::STRING                         AS CUSTOMER_ID,
    $2::STRING                         AS CUSTOMER_UNIQUE_ID,
    $3::STRING                         AS CUSTOMER_ZIP_CODE_PREFIX,
    $4::STRING                         AS CUSTOMER_CITY,
    $5::STRING                         AS CUSTOMER_STATE,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_customers_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- SELLERS
-- ------------------------------------------------------------
COPY INTO SELLERS_RAW
FROM (
  SELECT
    $1::STRING                         AS SELLER_ID,
    $2::STRING                         AS SELLER_ZIP_CODE_PREFIX,
    $3::STRING                         AS SELLER_CITY,
    $4::STRING                         AS SELLER_STATE,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_sellers_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- PRODUCTS
-- ------------------------------------------------------------
COPY INTO PRODUCTS_RAW
FROM (
  SELECT
    $1::STRING                         AS PRODUCT_ID,
    $2::STRING                         AS PRODUCT_CATEGORY_NAME,
    TRY_TO_NUMBER($3)                  AS PRODUCT_NAME_LENGHT,
    TRY_TO_NUMBER($4)                  AS PRODUCT_DESCRIPTION_LENGHT,
    TRY_TO_NUMBER($5)                  AS PRODUCT_PHOTOS_QTY,
    TRY_TO_NUMBER($6)                  AS PRODUCT_WEIGHT_G,
    TRY_TO_NUMBER($7)                  AS PRODUCT_LENGTH_CM,
    TRY_TO_NUMBER($8)                  AS PRODUCT_HEIGHT_CM,
    TRY_TO_NUMBER($9)                  AS PRODUCT_WIDTH_CM,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_products_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- GEOLOCATION
-- ------------------------------------------------------------
COPY INTO GEOLOCATION_RAW
FROM (
  SELECT
    $1::STRING                         AS GEOLOCATION_ZIP_CODE_PREFIX,
    TRY_TO_DOUBLE($2)                  AS GEOLOCATION_LAT,
    TRY_TO_DOUBLE($3)                  AS GEOLOCATION_LNG,
    $4::STRING                         AS GEOLOCATION_CITY,
    $5::STRING                         AS GEOLOCATION_STATE,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/olist_geolocation_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- CATEGORY TRANSLATION
-- ------------------------------------------------------------
COPY INTO CATEGORY_TRANSLATION_RAW
FROM (
  SELECT
    $1::STRING                         AS PRODUCT_CATEGORY_NAME,
    $2::STRING                         AS PRODUCT_CATEGORY_NAME_ENGLISH,
    METADATA$FILENAME::STRING          AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOADED_AT
  FROM @"ECOM_DB"."RAW"."STG_OLIST"/product_category_name_translation.csv
)
FILE_FORMAT = (FORMAT_NAME = 'ECOM_DB.RAW.FF_OLIST_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ------------------------------------------------------------
-- Verification
-- ------------------------------------------------------------
SELECT 'ORDERS_RAW' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ORDERS_RAW
UNION ALL SELECT 'ORDER_ITEMS_RAW', COUNT(*) FROM ORDER_ITEMS_RAW
UNION ALL SELECT 'PAYMENTS_RAW', COUNT(*) FROM PAYMENTS_RAW
UNION ALL SELECT 'CUSTOMERS_RAW', COUNT(*) FROM CUSTOMERS_RAW
UNION ALL SELECT 'SELLERS_RAW', COUNT(*) FROM SELLERS_RAW
UNION ALL SELECT 'PRODUCTS_RAW', COUNT(*) FROM PRODUCTS_RAW
UNION ALL SELECT 'REVIEWS_RAW', COUNT(*) FROM REVIEWS_RAW
UNION ALL SELECT 'GEOLOCATION_RAW', COUNT(*) FROM GEOLOCATION_RAW
UNION ALL SELECT 'CATEGORY_TRANSLATION_RAW', COUNT(*) FROM CATEGORY_TRANSLATION_RAW;
