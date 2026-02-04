/*

DROP EXTERNAL DATA SOURCE AzureBlobStorage;

*/

/*
DROP DATABASE SCOPED CREDENTIAL AzureBlobCredential;
*/


/*

CREATE DATABASE SCOPED CREDENTIAL AzureBlobCredential
WITH
    IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = 'sv=2024-11-04&ss=b&srt=sco&sp=rwdlaciytfx&se=2026-02-23T00:38:40Z&st=2026-01-19T16:23:40Z&spr=https&sig=6Mf%2F02h%2BpB3uKteyNdILnemLs5MV7gIfT90ZC1GSvWM%3D';
*/


/*
--STEP 5 — Recreate the External Data Source

CREATE EXTERNAL DATA SOURCE AzureBlobStorage
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://sttlcbi.blob.core.windows.net/raw',
    CREDENTIAL = AzureBlobCredential
);
*/


/*
--✅ FINAL STEP — Retry BULK INSERT (THIS WILL WORK)

TRUNCATE TABLE dbo.dim_zone;

BULK INSERT dbo.dim_zone
FROM 'taxi_zone_lookup.csv'
WITH (
    DATA_SOURCE = 'AzureBlobStorage',
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);
*/


/*
--1) Confirm you are connected to the right database

SELECT DB_NAME() AS current_db;
*/


/*
2) Re-run BULK INSERT using the supported syntax (NO FORMAT='CSV')

TRUNCATE TABLE dbo.dim_zone;

BULK INSERT dbo.dim_zone
FROM 'taxi_zone_lookup.csv'
WITH (
    DATA_SOURCE = 'AzureBlobStorage',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0d0a',  -- Windows line ending
    TABLOCK,
    CODEPAGE = '65001'         -- UTF-8 safe
);
*/


/*
-- 3) Verify it loaded

SELECT COUNT(*) AS row_count FROM dbo.dim_zone;
SELECT TOP 10 * FROM dbo.dim_zone;
*/

--


/*
DELETE FROM dbo.stg_yellow_trip
WHERE source_year = 2025 AND source_month = 11;
*/


/*
-- 4.3 BULK INSERT into staging (supported Azure SQL syntax)

BULK INSERT dbo.stg_yellow_trip
FROM 'yellow_tripdata_2025-11.csv'
WITH (
    DATA_SOURCE = 'AzureBlobStorage',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0d0a',
    TABLOCK,
    CODEPAGE = '65001'
);
*/


/*
-- PHASE 4A — Create a RAW staging table (TEXT ONLY)
-- Step 1 — Create raw staging table

IF OBJECT_ID('dbo.stg_yellow_trip_raw','U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_yellow_trip_raw (
        VendorID                varchar(50),
        tpep_pickup_datetime    varchar(50),
        tpep_dropoff_datetime   varchar(50),
        passenger_count         varchar(50),
        trip_distance           varchar(50),
        RatecodeID              varchar(50),
        store_and_fwd_flag      varchar(10),
        PULocationID            varchar(50),
        DOLocationID            varchar(50),
        payment_type            varchar(50),
        fare_amount             varchar(50),
        extra                   varchar(50),
        mta_tax                 varchar(50),
        tip_amount              varchar(50),
        tolls_amount            varchar(50),
        improvement_surcharge   varchar(50),
        total_amount            varchar(50),
        congestion_surcharge    varchar(50),
        Airport_fee             varchar(50)
    );
END
*/


/*
-- PHASE 4B — BULK INSERT into RAW table (THIS WILL WORK)
-- Step 2 — Clear raw table

TRUNCATE TABLE dbo.stg_yellow_trip_raw;
*/


/*
-- Step 3 — BULK INSERT raw CSV

-- Run exactly this:

BULK INSERT dbo.stg_yellow_trip_raw
FROM 'yellow_tripdata_2025-11.csv'
WITH (
    DATA_SOURCE = 'AzureBlobStorage',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
);
*/


/*
-- PHASE 4C — Verify raw load (DO NOT SKIP)

SELECT COUNT(*) AS raw_rows
FROM dbo.stg_yellow_trip_raw;

SELECT TOP 5 *
FROM dbo.stg_yellow_trip_raw;
*/


/*
-- DELETE FROM dbo.stg_yellow_trip
-- WHERE source_year = 2025 AND source_month = 11;

DELETE FROM dbo.stg_yellow_trip
WHERE source_year = 2025 AND source_month = 11;
*/


/*
-- Step 5 — Insert cleaned data (THIS IS THE IMPORTANT PART)

-- Run this exact SQL:

INSERT INTO dbo.stg_yellow_trip (
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    source_year,
    source_month
)
SELECT
    TRY_CAST(VendorID AS int),
    TRY_CAST(tpep_pickup_datetime AS datetime2),
    TRY_CAST(tpep_dropoff_datetime AS datetime2),
    TRY_CAST(passenger_count AS int),
    TRY_CAST(trip_distance AS float),
    TRY_CAST(RatecodeID AS int),
    store_and_fwd_flag,
    TRY_CAST(PULocationID AS int),
    TRY_CAST(DOLocationID AS int),
    TRY_CAST(payment_type AS int),
    TRY_CAST(fare_amount AS float),
    TRY_CAST(extra AS float),
    TRY_CAST(mta_tax AS float),
    TRY_CAST(tip_amount AS float),
    TRY_CAST(tolls_amount AS float),
    TRY_CAST(improvement_surcharge AS float),
    TRY_CAST(total_amount AS float),
    TRY_CAST(congestion_surcharge AS float),
    TRY_CAST(Airport_fee AS float),
    2025,
    11
FROM dbo.stg_yellow_trip_raw
WHERE
    TRY_CAST(tpep_pickup_datetime AS datetime2) IS NOT NULL
    AND TRY_CAST(tpep_dropoff_datetime AS datetime2) IS NOT NULL;
*/


/*
-- PHASE 4E — Verify typed staging

SELECT COUNT(*) AS stg_rows
FROM dbo.stg_yellow_trip
WHERE source_year = 2025 AND source_month = 11;

SELECT TOP 5
    tpep_pickup_datetime,
    passenger_count,
    trip_distance,
    total_amount
FROM dbo.stg_yellow_trip
WHERE source_year = 2025 AND source_month = 11;
*/


/*
-- Next steps (same as earlier)
-- 1️⃣ Log the file

EXEC dbo.usp_log_download_start
    @taxi_type = 'yellow',
    @year = 2025,
    @month = 11,
    @file_name = 'yellow_tripdata_2025-11.csv',
    @file_url = 'manual-blob-upload',
    @blob_path = 'raw/yellow_tripdata_2025-11.csv',
    @status = 'downloaded',
    @message = 'Loaded via BULK INSERT (raw + typed staging)';
*/


/*
-- 2️⃣ Load FACT table

EXEC dbo.usp_load_month_from_staging
    @taxi_type = 'yellow',
    @year = 2025,
    @month = 11;
*/


/*
-- 3️⃣ Validate views

SELECT TOP 20 * FROM dbo.vw_trips_revenue_daily;
SELECT TOP 20 * FROM dbo.vw_monthly_kpis;
*/


/*
-- Tell me the result of this one query:

SELECT COUNT(*) FROM dbo.stg_yellow_trip_raw;
*/


/*
-- STEP 1 — Verify cleaned staging (typed table)

-- Run this to confirm rows exist:

SELECT COUNT(*) AS cleaned_staging_rows
FROM dbo.stg_yellow_trip
WHERE source_year = 2025 AND source_month = 11;
*/


/*
-- You should see a large number (>0).

-- Spot-check data:

SELECT TOP 10
    tpep_pickup_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    source_year,
    source_month
FROM dbo.stg_yellow_trip
WHERE source_year = 2025 AND source_month = 11;
*/


/*
-- STEP 2 — Log the file load (control table)

-- This marks November 2025 as ingested.

EXEC dbo.usp_log_download_start
    @taxi_type = 'yellow',
    @year = 2025,
    @month = 11,
    @file_name = 'yellow_tripdata_2025-11.csv',
    @file_url = 'manual-blob-upload',
    @blob_path = 'raw/yellow_tripdata_2025-11.csv',
    @status = 'downloaded',
    @message = 'Raw + typed staging load succeeded';
*/


/*
-- Verify:

SELECT *
FROM dbo.download_log
WHERE taxi_type='yellow' AND [year]=2025 AND [month]=11;
*/


/*
-- STEP 3 — Load FACT table (SQL-heavy transformation)

-- Now move clean data into analytics-ready fact table.

EXEC dbo.usp_load_month_from_staging
    @taxi_type = 'yellow',
    @year = 2025,
    @month = 11;
*/


/*
-- STEP 4 — Verify FACT table

SELECT COUNT(*) AS fact_rows
FROM dbo.fact_trip
WHERE source_year = 2025 AND source_month = 11;
*/


/*
-- Spot-check:

SELECT TOP 10
    pickup_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    trip_duration_seconds,
    fare_per_mile
FROM dbo.fact_trip
WHERE source_year = 2025 AND source_month = 11
ORDER BY pickup_datetime;
*/


/*
-- STEP 5 — Validate BI Views (critical milestone)
-- Daily view

SELECT TOP 20 *
FROM dbo.vw_trips_revenue_daily
ORDER BY [date] DESC, borough;
*/


/*
-- Monthly KPI view (MoM / YoY)

SELECT TOP 20 *
FROM dbo.vw_monthly_kpis
ORDER BY month_start DESC, borough;
*/


/*
-- PHASE 4D — Clean + Convert into typed staging table
-- Step 4 — Clear typed staging for this month

DELETE FROM dbo.stg_yellow_trip
WHERE source_year = 2024 AND source_month = 11;
*/


/*
-- Insert cleaned data (THIS IS THE IMPORTANT PART)

-- Run this exact SQL:

INSERT INTO dbo.stg_yellow_trip (
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    source_year,
    source_month
)
SELECT
    TRY_CAST(VendorID AS int),
    TRY_CAST(tpep_pickup_datetime AS datetime2),
    TRY_CAST(tpep_dropoff_datetime AS datetime2),
    TRY_CAST(passenger_count AS int),
    TRY_CAST(trip_distance AS float),
    TRY_CAST(RatecodeID AS int),
    store_and_fwd_flag,
    TRY_CAST(PULocationID AS int),
    TRY_CAST(DOLocationID AS int),
    TRY_CAST(payment_type AS int),
    TRY_CAST(fare_amount AS float),
    TRY_CAST(extra AS float),
    TRY_CAST(mta_tax AS float),
    TRY_CAST(tip_amount AS float),
    TRY_CAST(tolls_amount AS float),
    TRY_CAST(improvement_surcharge AS float),
    TRY_CAST(total_amount AS float),
    TRY_CAST(congestion_surcharge AS float),
    TRY_CAST(Airport_fee AS float),
    2024,
    11
FROM dbo.stg_yellow_trip_raw
WHERE
    TRY_CAST(tpep_pickup_datetime AS datetime2) IS NOT NULL
    AND TRY_CAST(tpep_dropoff_datetime AS datetime2) IS NOT NULL;
*/




/*
-- PHASE 4E — Verify typed staging

SELECT COUNT(*) AS stg_rows
FROM dbo.stg_yellow_trip
WHERE source_year = 2024 AND source_month = 11;

SELECT TOP 5
    tpep_pickup_datetime,
    passenger_count,
    trip_distance,
    total_amount
FROM dbo.stg_yellow_trip
WHERE source_year = 2024 AND source_month = 11;
*/



/*
-- 1️⃣ Log the file

EXEC dbo.usp_log_download_start
    @taxi_type = 'yellow',
    @year = 2024,
    @month = 11,
    @file_name = 'yellow_tripdata_2024-11.csv',
    @file_url = 'manual-blob-upload',
    @blob_path = 'raw/yellow_tripdata_2024-11.csv',
    @status = 'downloaded',
    @message = 'Loaded via BULK INSERT (raw + typed staging)';
*/


/*
--
-- A4) Clean RAW → TYPED staging for 2024-11
-- Step 1 — Clear typed staging for that month (safe)

-- Run this first to avoid duplicates:

DELETE FROM dbo.stg_yellow_trip
WHERE source_year = 2024
  AND source_month = 11;
*/

/*
-- Step 2 — Insert cleaned data from RAW into typed staging

INSERT INTO dbo.stg_yellow_trip (
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    source_year,
    source_month
)
SELECT
    TRY_CAST(VendorID AS int),
    TRY_CAST(tpep_pickup_datetime AS datetime2),
    TRY_CAST(tpep_dropoff_datetime AS datetime2),
    TRY_CAST(passenger_count AS int),
    TRY_CAST(trip_distance AS float),
    TRY_CAST(RatecodeID AS int),
    store_and_fwd_flag,
    TRY_CAST(PULocationID AS int),
    TRY_CAST(DOLocationID AS int),
    TRY_CAST(payment_type AS int),
    TRY_CAST(fare_amount AS float),
    TRY_CAST(extra AS float),
    TRY_CAST(mta_tax AS float),
    TRY_CAST(tip_amount AS float),
    TRY_CAST(tolls_amount AS float),
    TRY_CAST(improvement_surcharge AS float),
    TRY_CAST(total_amount AS float),
    TRY_CAST(congestion_surcharge AS float),
    TRY_CAST(Airport_fee AS float),
    2024 AS source_year,
    11   AS source_month
FROM dbo.stg_yellow_trip_raw
WHERE
    TRY_CAST(tpep_pickup_datetime AS datetime2) >= '2024-11-01'
    AND TRY_CAST(tpep_pickup_datetime AS datetime2) <  '2024-12-01'
    AND TRY_CAST(tpep_pickup_datetime AS datetime2) IS NOT NULL
    AND TRY_CAST(tpep_dropoff_datetime AS datetime2) IS NOT NULL;
*/












 /*
--Step 3 — Verify typed staging (DO NOT SKIP)

SELECT COUNT(*) AS stg_rows_2024_11
FROM dbo.stg_yellow_trip
WHERE source_year = 2024
  AND source_month = 11;

-- Optional spot check:

SELECT TOP 10
    tpep_pickup_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    source_year,
    source_month
FROM dbo.stg_yellow_trip
WHERE source_year = 2024
  AND source_month = 11
ORDER BY tpep_pickup_datetime;

*/


/*
-- Next steps (in order)
-- A5) Load FACT for 2024-11

EXEC dbo.usp_load_month_from_staging
    @taxi_type = 'yellow',
    @year = 2024,
    @month = 11;
*/




/*

TRUNCATE TABLE dbo.stg_yellow_trip_raw;
*/


/*
-- Verify it’s empty

-- Run:

SELECT COUNT(*) AS raw_count
FROM dbo.stg_yellow_trip_raw;
*/


/*
-- Step 2 — BULK INSERT the 2024-11 file (important: file name must match)

-- Run:

BULK INSERT dbo.stg_yellow_trip_raw
FROM 'yellow_tripdata_2024-11.csv'
WITH (
    DATA_SOURCE = 'AzureBlobStorage',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
);
*/



/*
--
SELECT
    MIN(TRY_CAST(tpep_pickup_datetime AS datetime2)) AS min_pickup,
    MAX(TRY_CAST(tpep_pickup_datetime AS datetime2)) AS max_pickup,
    COUNT(*) AS total_rows
FROM dbo.stg_yellow_trip_raw;
*/



/*
--

DELETE FROM dbo.stg_yellow_trip
WHERE source_year = 2024 AND source_month = 11;
*/


/*
-- 

EXEC dbo.usp_load_month_from_staging
  @taxi_type='yellow', @year=2024, @month=11;
*/


/*
SELECT borough, revenue_yoy_pct
FROM dbo.vw_monthly_kpis
WHERE [year] = 2025 AND [month] = 11;
*/


/*
SELECT source_year, source_month, COUNT(*) AS fact_rows
FROM dbo.fact_trip
WHERE (source_year = 2024 AND source_month = 11)
   OR (source_year = 2025 AND source_month = 11)
GROUP BY source_year, source_month
ORDER BY source_year, source_month;
*/


/*
SELECT
  source_year,
  source_month,
  SUM(CASE WHEN z.borough IS NULL THEN 1 ELSE 0 END) AS null_borough_rows,
  COUNT(*) AS total_rows
FROM dbo.fact_trip f
LEFT JOIN dbo.dim_zone z ON z.location_id = f.pickup_location_id
WHERE (source_year = 2024 AND source_month = 11)
   OR (source_year = 2025 AND source_month = 11)
GROUP BY source_year, source_month
ORDER BY source_year, source_month;
*/



/*
CREATE OR ALTER VIEW dbo.vw_monthly_kpis AS
WITH m AS (
    SELECT
        d.[year],
        d.[month],
        DATEFROMPARTS(d.[year], d.[month], 1) AS month_start,
        COALESCE(z.borough, 'Unknown') AS borough,
        COUNT_BIG(*) AS trips,
        SUM(COALESCE(f.total_amount,0)) AS revenue
    FROM dbo.fact_trip f
    JOIN dbo.dim_date d ON d.date_key = f.pickup_date_key
    LEFT JOIN dbo.dim_zone z ON z.location_id = f.pickup_location_id
    GROUP BY d.[year], d.[month], COALESCE(z.borough, 'Unknown')
)
SELECT
    [year],[month],month_start,borough,trips,revenue,

    (trips - LAG(trips,1) OVER (PARTITION BY borough ORDER BY month_start))
        * 1.0 / NULLIF(LAG(trips,1) OVER (PARTITION BY borough ORDER BY month_start),0) AS trips_mom_pct,

    (revenue - LAG(revenue,1) OVER (PARTITION BY borough ORDER BY month_start))
        * 1.0 / NULLIF(LAG(revenue,1) OVER (PARTITION BY borough ORDER BY month_start),0) AS revenue_mom_pct,

    (trips - LAG(trips,12) OVER (PARTITION BY borough ORDER BY month_start))
        * 1.0 / NULLIF(LAG(trips,12) OVER (PARTITION BY borough ORDER BY month_start),0) AS trips_yoy_pct,

    (revenue - LAG(revenue,12) OVER (PARTITION BY borough ORDER BY month_start))
        * 1.0 / NULLIF(LAG(revenue,12) OVER (PARTITION BY borough ORDER BY month_start),0) AS revenue_yoy_pct
FROM m;
*/




/*
SELECT TOP 50
  borough, revenue, revenue_yoy_pct
FROM dbo.vw_monthly_kpis
WHERE [year]=2025 AND [month]=11
ORDER BY revenue DESC;
*/


/*
SELECT borough, [year], [month], revenue
FROM dbo.vw_monthly_kpis
WHERE ( [year]=2024 AND [month]=11 )
   OR ( [year]=2025 AND [month]=11 )
ORDER BY borough, [year], [month];
*/

/*
-- B) SQL additions (2 tiny helpers)
-- B1) Log “file exists/processed” check relies on download_log

-- You already have download_log and procedures.

-- B2) Create a SQL proc to “clean raw → typed staging” (optional but best)

-- Right now you manually run the INSERT…TRY_CAST.
-- Make it a stored procedure so automation can call it:

CREATE OR ALTER PROCEDURE dbo.usp_stage_from_raw_yellow
    @year int,
    @month int
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM dbo.stg_yellow_trip
    WHERE source_year=@year AND source_month=@month;

    INSERT INTO dbo.stg_yellow_trip (
        vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
        pulocationid, dolocationid, payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
        source_year, source_month
    )
    SELECT
        TRY_CAST(VendorID AS int),
        TRY_CAST(tpep_pickup_datetime AS datetime2),
        TRY_CAST(tpep_dropoff_datetime AS datetime2),
        TRY_CAST(passenger_count AS int),
        TRY_CAST(trip_distance AS float),
        TRY_CAST(RatecodeID AS int),
        store_and_fwd_flag,
        TRY_CAST(PULocationID AS int),
        TRY_CAST(DOLocationID AS int),
        TRY_CAST(payment_type AS int),
        TRY_CAST(fare_amount AS float),
        TRY_CAST(extra AS float),
        TRY_CAST(mta_tax AS float),
        TRY_CAST(tip_amount AS float),
        TRY_CAST(tolls_amount AS float),
        TRY_CAST(improvement_surcharge AS float),
        TRY_CAST(total_amount AS float),
        TRY_CAST(congestion_surcharge AS float),
        TRY_CAST(Airport_fee AS float),
        @year, @month
    FROM dbo.stg_yellow_trip_raw
    WHERE
        TRY_CAST(tpep_pickup_datetime AS datetime2) >= DATEFROMPARTS(@year,@month,1)
        AND TRY_CAST(tpep_pickup_datetime AS datetime2) <  DATEADD(month,1,DATEFROMPARTS(@year,@month,1));
END
*/



/*
-- A) ONE-TIME SQL STEP (required for automation)
-- A1) Create the proc that converts RAW → Typed staging (run once)

-- Open Azure Data Studio / SSMS and run:

CREATE OR ALTER PROCEDURE dbo.usp_stage_from_raw_yellow
    @year int,
    @month int
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM dbo.stg_yellow_trip
    WHERE source_year=@year AND source_month=@month;

    INSERT INTO dbo.stg_yellow_trip (
        vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
        pulocationid, dolocationid, payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
        source_year, source_month
    )
    SELECT
        TRY_CAST(VendorID AS int),
        TRY_CAST(tpep_pickup_datetime AS datetime2),
        TRY_CAST(tpep_dropoff_datetime AS datetime2),
        TRY_CAST(passenger_count AS int),
        TRY_CAST(trip_distance AS float),
        TRY_CAST(RatecodeID AS int),
        store_and_fwd_flag,
        TRY_CAST(PULocationID AS int),
        TRY_CAST(DOLocationID AS int),
        TRY_CAST(payment_type AS int),
        TRY_CAST(fare_amount AS float),
        TRY_CAST(extra AS float),
        TRY_CAST(mta_tax AS float),
        TRY_CAST(tip_amount AS float),
        TRY_CAST(tolls_amount AS float),
        TRY_CAST(improvement_surcharge AS float),
        TRY_CAST(total_amount AS float),
        TRY_CAST(congestion_surcharge AS float),
        TRY_CAST(Airport_fee AS float),
        @year, @month
    FROM dbo.stg_yellow_trip_raw
    WHERE
        TRY_CAST(tpep_pickup_datetime AS datetime2) >= DATEFROMPARTS(@year,@month,1)
        AND TRY_CAST(tpep_pickup_datetime AS datetime2) <  DATEADD(month,1,DATEFROMPARTS(@year,@month,1));
END
*/


/*
SELECT name, location
FROM sys.external_data_sources
WHERE name = 'AzureBlobStorage';
*/


/*
-- 1) Validate the warehouse load (SQL checks)
-- 1.1 Row counts by month (FACT)

-- Run in Azure SQL / SSMS:

SELECT
  source_year,
  source_month,
  COUNT_BIG(*) AS fact_rows,
  MIN(pickup_datetime) AS min_pickup,
  MAX(pickup_datetime) AS max_pickup
FROM dbo.fact_trip
GROUP BY source_year, source_month
ORDER BY source_year, source_month;
*/



/*
SELECT
* 
FROM dbo.fact_trip
*/








/*
*/



/*
-- 1.2 Check duplicates (should be 0)

-- (Use your natural key if you have one; if not, this is a practical proxy.)

SELECT TOP 20
  source_year, source_month,
  pickup_datetime, dropoff_datetime,
  PULocationID, DOLocationID,
  total_amount,
  COUNT(*) AS dup_count
FROM dbo.fact_trip
GROUP BY
  source_year, source_month,
  pickup_datetime, dropoff_datetime,
  PULocationID, DOLocationID,
  total_amount
HAVING COUNT(*) > 5
ORDER BY dup_count DESC;
*/


/*
-- Step 1 — Check the actual column names (DO THIS FIRST)

-- Run exactly this in Azure SQL / SSMS:

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_trip'
ORDER BY COLUMN_NAME;
*/


/*
-- 1) Corrected duplicate check (using YOUR real columns)

-- Run this in Azure SQL / SSMS:

SELECT TOP 20
  source_year,
  source_month,
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  total_amount,
  COUNT(*) AS dup_count
FROM dbo.fact_trip
GROUP BY
  source_year,
  source_month,
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  total_amount
HAVING COUNT(*) > 5
ORDER BY dup_count DESC;

-- Expected result

-- Ideally: 0 rows returned

-- If you see rows:

-- Small numbers (2–3) are normal for taxi data

-- Large numbers → we add a dedupe step in the load proc
*/


/*
-- 2) Row-count sanity check (month-by-month)

SELECT
  source_year,
  source_month,
  COUNT_BIG(*) AS trips,
  MIN(pickup_datetime) AS min_pickup,
  MAX(pickup_datetime) AS max_pickup
FROM dbo.fact_trip
GROUP BY source_year, source_month
ORDER BY source_year, source_month;
*/


/*
-- 3) Add a clean analytics view (recommended for Power BI)

-- Instead of connecting Power BI directly to fact_trip, expose a stable, business-friendly view

CREATE OR ALTER VIEW dbo.vw_trip_fact AS
SELECT
  trip_id,
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  passenger_count,
  trip_distance,
  trip_duration_seconds,
  fare_amount,
  tip_amount,
  total_amount,
  fare_per_mile,
  payment_type,
  source_year,
  source_month,
  created_at
FROM dbo.fact_trip;
*/


/*
CREATE OR ALTER VIEW dbo.vw_trips_revenue_daily AS
SELECT
    CAST(t.pickup_datetime AS date) AS date,
    z.borough,
    COUNT(*) AS trips,
    SUM(t.total_amount) AS total_revenue
FROM dbo.fact_trip t
LEFT JOIN dbo.dim_zone z
    ON t.pickup_location_id = z.location_id
GROUP BY
    CAST(t.pickup_datetime AS date),
    z.borough;
*/


/*
    CREATE OR ALTER VIEW dbo.vw_monthly_kpis AS
SELECT
    source_year AS year,
    source_month AS month,
    DATEFROMPARTS(source_year, source_month, 1) AS month_start,
    z.borough,
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    -- YoY logic already present in your version
    revenue_yoy_pct,
    trips_yoy_pct
FROM dbo.fact_trip t
LEFT JOIN dbo.dim_zone z
    ON t.pickup_location_id = z.location_id
GROUP BY
    source_year,
    source_month,
    z.borough,
    revenue_yoy_pct,
    trips_yoy_pct;
*/


/*
CREATE OR ALTER VIEW dbo.vw_monthly_kpis AS
WITH monthly AS (
    SELECT
        t.source_year AS [year],
        t.source_month AS [month],
        DATEFROMPARTS(t.source_year, t.source_month, 1) AS month_start,
        z.borough,
        COUNT_BIG(*) AS trips,
        SUM(t.total_amount) AS revenue,
        AVG(t.trip_distance * 1.0) AS avg_distance,
        AVG(t.trip_duration_seconds * 1.0) AS avg_duration_seconds
    FROM dbo.fact_trip t
    LEFT JOIN dbo.dim_zone z
        ON t.pickup_location_id = z.location_id
    GROUP BY
        t.source_year,
        t.source_month,
        z.borough
),
calc AS (
    SELECT
        *,
        LAG(revenue, 1) OVER (PARTITION BY borough ORDER BY month_start) AS revenue_prev_month,
        LAG(trips,   1) OVER (PARTITION BY borough ORDER BY month_start) AS trips_prev_month,
        LAG(revenue, 12) OVER (PARTITION BY borough ORDER BY month_start) AS revenue_prev_year,
        LAG(trips,   12) OVER (PARTITION BY borough ORDER BY month_start) AS trips_prev_year
    FROM monthly
)
SELECT
    [year],
    [month],
    month_start,
    borough,
    trips,
    revenue,
    avg_distance,
    avg_duration_seconds,

    -- MoM %
    CAST(
        CASE WHEN revenue_prev_month IS NULL OR revenue_prev_month = 0 THEN NULL
             ELSE (revenue - revenue_prev_month) / revenue_prev_month
        END
    AS float) AS revenue_mom_pct,

    CAST(
        CASE WHEN trips_prev_month IS NULL OR trips_prev_month = 0 THEN NULL
             ELSE (trips - trips_prev_month) * 1.0 / trips_prev_month
        END
    AS float) AS trips_mom_pct,

    -- YoY %
    CAST(
        CASE WHEN revenue_prev_year IS NULL OR revenue_prev_year = 0 THEN NULL
             ELSE (revenue - revenue_prev_year) / revenue_prev_year
        END
    AS float) AS revenue_yoy_pct,

    CAST(
        CASE WHEN trips_prev_year IS NULL OR trips_prev_year = 0 THEN NULL
             ELSE (trips - trips_prev_year) * 1.0 / trips_prev_year
        END
    AS float) AS trips_yoy_pct
FROM calc;
GO
*/


/*
CREATE OR ALTER VIEW dbo.vw_trips_revenue_daily AS
SELECT
    CAST(t.pickup_datetime AS date) AS [date],
    z.borough,
    COUNT_BIG(*) AS trips,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.trip_distance * 1.0) AS avg_distance,
    AVG(t.trip_duration_seconds * 1.0) AS avg_duration_seconds
FROM dbo.fact_trip t
LEFT JOIN dbo.dim_zone z
    ON t.pickup_location_id = z.location_id
GROUP BY
    CAST(t.pickup_datetime AS date),
    z.borough;
GO
*/


/*
CREATE OR ALTER VIEW dbo.dim_borough AS
SELECT DISTINCT
    borough
FROM dbo.dim_zone
WHERE borough IS NOT NULL;
*/


/*
CREATE OR ALTER VIEW dbo.dim_risk_flag AS
SELECT 'High Risk' AS risk_flag
UNION ALL
SELECT 'Stable' AS risk_flag;
GO
*/


/*
CREATE OR ALTER VIEW dbo.vw_monthly_kpis AS
WITH monthly AS (
    SELECT
        t.source_year AS [year],
        t.source_month AS [month],
        DATEFROMPARTS(t.source_year, t.source_month, 1) AS month_start,
        LTRIM(RTRIM(REPLACE(z.borough, '"', ''))) AS borough,
        COUNT_BIG(*) AS trips,
        SUM(t.total_amount) AS revenue
    FROM dbo.fact_trip t
    LEFT JOIN dbo.dim_zone z
        ON t.pickup_location_id = z.location_id
    GROUP BY
        t.source_year,
        t.source_month,
        LTRIM(RTRIM(REPLACE(z.borough, '"', '')))
),
calc AS (
    SELECT
        *,
        LAG(revenue, 12) OVER (PARTITION BY borough ORDER BY month_start) AS revenue_prev_year,
        LAG(trips, 12)   OVER (PARTITION BY borough ORDER BY month_start) AS trips_prev_year
    FROM monthly
),
final AS (
    SELECT
        [year],
        [month],
        month_start,
        borough,
        trips,
        revenue,
        CAST(CASE WHEN revenue_prev_year IS NULL OR revenue_prev_year = 0 THEN NULL
                  ELSE (revenue - revenue_prev_year) / revenue_prev_year END AS float) AS revenue_yoy_pct,
        CAST(CASE WHEN trips_prev_year IS NULL OR trips_prev_year = 0 THEN NULL
                  ELSE (trips - trips_prev_year) * 1.0 / trips_prev_year END AS float) AS trips_yoy_pct
    FROM calc
)
SELECT
    *,
    CASE
        WHEN revenue_yoy_pct < 0 OR trips_yoy_pct < 0 THEN 'High Risk'
        ELSE 'Stable'
    END AS risk_flag
FROM final;
GO
*/


CREATE OR ALTER VIEW dbo.vw_monthly_kpis AS
WITH base AS (
    SELECT
        z.borough,
        YEAR(d.pickup_datetime)  AS [year],
        MONTH(d.pickup_datetime) AS [month],
        DATEFROMPARTS(YEAR(d.pickup_datetime), MONTH(d.pickup_datetime), 1) AS month_start,

        COUNT_BIG(*) AS trips,
        SUM(d.total_amount) AS revenue,

        AVG(CAST(d.trip_distance AS float)) AS avg_distance,
        AVG(CAST(d.trip_duration_seconds AS float)) AS avg_duration_seconds
    FROM dbo.vw_trip_fact d
    LEFT JOIN dbo.dim_zone z
        ON d.pickup_location_id = z.location_id
    GROUP BY
        z.borough,
        YEAR(d.pickup_datetime),
        MONTH(d.pickup_datetime),
        DATEFROMPARTS(YEAR(d.pickup_datetime), MONTH(d.pickup_datetime), 1)
),
calc AS (
    SELECT
        *,
        LAG(revenue, 1) OVER (PARTITION BY borough ORDER BY month_start) AS revenue_prev_month,
        LAG(trips,   1) OVER (PARTITION BY borough ORDER BY month_start) AS trips_prev_month,

        LAG(revenue, 12) OVER (PARTITION BY borough ORDER BY month_start) AS revenue_prev_year,
        LAG(trips,   12) OVER (PARTITION BY borough ORDER BY month_start) AS trips_prev_year
    FROM base
)
SELECT
    borough,
    [year],
    [month],
    month_start,
    revenue,
    trips,

    avg_distance,
    avg_duration_seconds,

    -- MoM %
    CASE WHEN revenue_prev_month IS NULL OR revenue_prev_month = 0 THEN NULL
         ELSE (revenue - revenue_prev_month) * 1.0 / revenue_prev_month
    END AS revenue_mom_pct,

    CASE WHEN trips_prev_month IS NULL OR trips_prev_month = 0 THEN NULL
         ELSE (trips - trips_prev_month) * 1.0 / trips_prev_month
    END AS trips_mom_pct,

    -- YoY %
    CASE WHEN revenue_prev_year IS NULL OR revenue_prev_year = 0 THEN NULL
         ELSE (revenue - revenue_prev_year) * 1.0 / revenue_prev_year
    END AS revenue_yoy_pct,

    CASE WHEN trips_prev_year IS NULL OR trips_prev_year = 0 THEN NULL
         ELSE (trips - trips_prev_year) * 1.0 / trips_prev_year
    END AS trips_yoy_pct
FROM calc;
GO










/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/
/*
*/



















/*
*/