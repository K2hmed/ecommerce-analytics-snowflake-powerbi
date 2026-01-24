-- ============================================================
-- 02_build_stg.sql
-- Builds STG (typed + cleaned + deduped)
-- ============================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE ECOM_DB;

CREATE SCHEMA IF NOT EXISTS STG;

-- ------------------------------------------------------------
-- STG_ORDERS
-- Dedupe: ORDER_ID latest LOADED_AT wins
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.ORDERS AS
SELECT
  ORDER_ID,
  CUSTOMER_ID,
  LOWER(TRIM(ORDER_STATUS))                                  AS ORDER_STATUS,
  ORDER_PURCHASE_TIMESTAMP                                   AS ORDER_PURCHASE_TS,
  ORDER_APPROVED_AT                                          AS ORDER_APPROVED_TS,
  ORDER_DELIVERED_CARRIER_DATE                               AS ORDER_DELIVERED_CARRIER_TS,
  ORDER_DELIVERED_CUSTOMER_DATE                              AS ORDER_DELIVERED_CUSTOMER_TS,
  ORDER_ESTIMATED_DELIVERY_DATE                              AS ORDER_ESTIMATED_DELIVERY_TS,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.ORDERS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ORDER_ID
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_ORDER_ITEMS
-- Dedupe: (ORDER_ID, ORDER_ITEM_ID) latest LOADED_AT wins
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.ORDER_ITEMS AS
SELECT
  ORDER_ID,
  ORDER_ITEM_ID,
  PRODUCT_ID,
  SELLER_ID,
  SHIPPING_LIMIT_DATE                                       AS SHIPPING_LIMIT_TS,
  PRICE,
  FREIGHT_VALUE,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.ORDER_ITEMS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ORDER_ID, ORDER_ITEM_ID
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_PAYMENTS
-- Dedupe: (ORDER_ID, PAYMENT_SEQUENTIAL) latest LOADED_AT wins
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.PAYMENTS AS
SELECT
  ORDER_ID,
  PAYMENT_SEQUENTIAL,
  LOWER(TRIM(PAYMENT_TYPE))                                 AS PAYMENT_TYPE,
  PAYMENT_INSTALLMENTS,
  PAYMENT_VALUE,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.PAYMENTS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ORDER_ID, PAYMENT_SEQUENTIAL
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_REVIEWS
-- Dedupe: REVIEW_ID latest LOADED_AT wins
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.REVIEWS AS
SELECT
  REVIEW_ID,
  ORDER_ID,
  REVIEW_SCORE,
  NULLIF(TRIM(REVIEW_COMMENT_TITLE), '')                    AS REVIEW_COMMENT_TITLE,
  NULLIF(TRIM(REVIEW_COMMENT_MESSAGE), '')                  AS REVIEW_COMMENT_MESSAGE,
  REVIEW_CREATION_DATE                                      AS REVIEW_CREATION_TS,
  REVIEW_ANSWER_TIMESTAMP                                   AS REVIEW_ANSWER_TS,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.REVIEWS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY REVIEW_ID
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_CUSTOMERS
-- Dedupe: CUSTOMER_ID latest LOADED_AT wins
-- Cleaning: ZIP as string, trim city/state
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.CUSTOMERS AS
SELECT
  CUSTOMER_ID,
  CUSTOMER_UNIQUE_ID,
  LPAD(TRIM(CUSTOMER_ZIP_CODE_PREFIX), 5, '0')              AS CUSTOMER_ZIP_PREFIX,
  INITCAP(TRIM(CUSTOMER_CITY))                              AS CUSTOMER_CITY,
  UPPER(TRIM(CUSTOMER_STATE))                               AS CUSTOMER_STATE,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.CUSTOMERS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CUSTOMER_ID
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_SELLERS
-- Dedupe: SELLER_ID latest LOADED_AT wins
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.SELLERS AS
SELECT
  SELLER_ID,
  LPAD(TRIM(SELLER_ZIP_CODE_PREFIX), 5, '0')                AS SELLER_ZIP_PREFIX,
  INITCAP(TRIM(SELLER_CITY))                                AS SELLER_CITY,
  UPPER(TRIM(SELLER_STATE))                                 AS SELLER_STATE,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.SELLERS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY SELLER_ID
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_PRODUCTS
-- Dedupe: PRODUCT_ID latest LOADED_AT wins
-- Cleaning: category normalized
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.PRODUCTS AS
SELECT
  PRODUCT_ID,
  NULLIF(LOWER(TRIM(PRODUCT_CATEGORY_NAME)), '')            AS PRODUCT_CATEGORY_NAME,
  PRODUCT_NAME_LENGHT                                       AS PRODUCT_NAME_LENGTH,
  PRODUCT_DESCRIPTION_LENGHT                                AS PRODUCT_DESCRIPTION_LENGTH,
  PRODUCT_PHOTOS_QTY,
  PRODUCT_WEIGHT_G,
  PRODUCT_LENGTH_CM,
  PRODUCT_HEIGHT_CM,
  PRODUCT_WIDTH_CM,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.PRODUCTS_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY PRODUCT_ID
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_CATEGORY_TRANSLATION
-- Dedupe: PRODUCT_CATEGORY_NAME latest LOADED_AT wins
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.CATEGORY_TRANSLATION AS
SELECT
  LOWER(TRIM(PRODUCT_CATEGORY_NAME))                        AS PRODUCT_CATEGORY_NAME,
  LOWER(TRIM(PRODUCT_CATEGORY_NAME_ENGLISH))                AS PRODUCT_CATEGORY_NAME_ENGLISH,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.CATEGORY_TRANSLATION_RAW
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY LOWER(TRIM(PRODUCT_CATEGORY_NAME))
  ORDER BY LOADED_AT DESC
) = 1;

-- ------------------------------------------------------------
-- STG_GEOLOCATION (raw-level, huge)
-- Dedupe: keep all rows but clean and type (lat/lng are already numeric)
-- Then create a ZIP-level rollup for analytics joins.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE STG.GEOLOCATION AS
SELECT
  LPAD(TRIM(GEOLOCATION_ZIP_CODE_PREFIX), 5, '0')           AS ZIP_PREFIX,
  GEOLOCATION_LAT                                           AS LAT,
  GEOLOCATION_LNG                                           AS LNG,
  INITCAP(TRIM(GEOLOCATION_CITY))                           AS CITY,
  UPPER(TRIM(GEOLOCATION_STATE))                            AS STATE,
  SOURCE_FILE,
  LOADED_AT
FROM RAW.GEOLOCATION_RAW
WHERE GEOLOCATION_LAT IS NOT NULL
  AND GEOLOCATION_LNG IS NOT NULL;

-- ZIP rollup (centroid-ish): average lat/lng per zip
CREATE OR REPLACE TABLE STG.GEO_ZIP AS
SELECT
  ZIP_PREFIX,
  ANY_VALUE(STATE)                                          AS STATE,
  ANY_VALUE(CITY)                                           AS CITY,
  AVG(LAT)                                                  AS LAT_AVG,
  AVG(LNG)                                                  AS LNG_AVG,
  COUNT(*)                                                  AS GEO_ROWS
FROM STG.GEOLOCATION
GROUP BY ZIP_PREFIX;

-- ------------------------------------------------------------
-- STG: sanity checks
-- ------------------------------------------------------------
SELECT 'STG.ORDERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM STG.ORDERS
UNION ALL SELECT 'STG.ORDER_ITEMS', COUNT(*) FROM STG.ORDER_ITEMS
UNION ALL SELECT 'STG.PAYMENTS', COUNT(*) FROM STG.PAYMENTS
UNION ALL SELECT 'STG.REVIEWS', COUNT(*) FROM STG.REVIEWS
UNION ALL SELECT 'STG.CUSTOMERS', COUNT(*) FROM STG.CUSTOMERS
UNION ALL SELECT 'STG.SELLERS', COUNT(*) FROM STG.SELLERS
UNION ALL SELECT 'STG.PRODUCTS', COUNT(*) FROM STG.PRODUCTS
UNION ALL SELECT 'STG.GEOLOCATION', COUNT(*) FROM STG.GEOLOCATION
UNION ALL SELECT 'STG.GEO_ZIP', COUNT(*) FROM STG.GEO_ZIP
UNION ALL SELECT 'STG.CATEGORY_TRANSLATION', COUNT(*) FROM STG.CATEGORY_TRANSLATION;
