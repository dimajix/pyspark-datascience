--------------------------------------------------------------------------------------------
-- Weather Analysis with Hive
--
-- This notebook will perform the same weatehr analysis like the exercise with Apache
-- Spark, but it will use only Hive. You need to use a Hive client, like for example Hue.
--
--------------------------------------------------------------------------------------------
-- 
-- 1. Create a Hive Database
--
-- First we need to create a database where we will create tables and views.
--
CREATE DATABASE IF NOT EXISTS training;
USE training;

--------------------------------------------------------------------------------------------
-- 
-- 2. Create a Hive table
--
-- Now we create a Hive table which points at the raw data. In order to perform some simple
-- transformations, we will first create a table for a single year. Later, we will create a
-- new table, which contains all years.
--

-- Load some data to play with
CREATE EXTERNAL TABLE training.weather_2011(data STRING)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/weather/2011';

-- Look inside
SELECT * FROM training.weather_2011 limit 10;
SELECT SUBSTR(data,5,6) AS usaf FROM weather_2011 LIMIT 10;

-- Extract relevant measurements
SELECT
    SUBSTR(data,5,6) AS usaf,
    SUBSTR(data,11,5) AS wban, 
    SUBSTR(data,16,8) AS `date`, 
    SUBSTR(data,24,4) AS `time`,
    SUBSTR(data,42,5) AS report_type,
    SUBSTR(data,61,3) AS wind_direction, 
    SUBSTR(data,64,1) AS wind_direction_qual, 
    SUBSTR(data,65,1) AS wind_observation, 
    SUBSTR(data,66,4) AS wind_speed,
    SUBSTR(data,70,1) AS wind_speed_qual,
    SUBSTR(data,88,5) AS air_temperature, 
    SUBSTR(data,93,1) AS air_temperature_qual 
FROM weather_2011
LIMIT 10;

-- Tidy up again
DROP TABLE weather_2011;


--------------------------------------------------------------------------------------------
-- 
-- 3. Create a Hive table for all years
--
-- So far, we have created a simple table for a single year. But we want to work with a 
-- table, which contains all years. This can be achieved by using a "partitoned" table.
-- A so called "partition column" is a virtual column, where a different directory is 
-- attached the table for each value. In our case, we will create a partiton column "year"
-- and then attach all the year subdirectories as separate partitons.

-- Create new table for all weather data. The table is partitioned by year.
CREATE EXTERNAL TABLE IF NOT EXISTS training.weather_raw(
    data STRING
)
PARTITIONED BY(year STRING)
STORED AS TEXTFILE;

-- Add all partitions
ALTER TABLE training.weather_raw ADD PARTITION(year=2003) LOCATION 's3://dimajix-training/data/weather/2003';
ALTER TABLE training.weather_raw ADD PARTITION(year=2004) LOCATION 's3://dimajix-training/data/weather/2004';
ALTER TABLE training.weather_raw ADD PARTITION(year=2005) LOCATION 's3://dimajix-training/data/weather/2005';
ALTER TABLE training.weather_raw ADD PARTITION(year=2006) LOCATION 's3://dimajix-training/data/weather/2006';
ALTER TABLE training.weather_raw ADD PARTITION(year=2007) LOCATION 's3://dimajix-training/data/weather/2007';
ALTER TABLE training.weather_raw ADD PARTITION(year=2008) LOCATION 's3://dimajix-training/data/weather/2008';
ALTER TABLE training.weather_raw ADD PARTITION(year=2009) LOCATION 's3://dimajix-training/data/weather/2009';
ALTER TABLE training.weather_raw ADD PARTITION(year=2010) LOCATION 's3://dimajix-training/data/weather/2010';
ALTER TABLE training.weather_raw ADD PARTITION(year=2011) LOCATION 's3://dimajix-training/data/weather/2011';
ALTER TABLE training.weather_raw ADD PARTITION(year=2012) LOCATION 's3://dimajix-training/data/weather/2012';
ALTER TABLE training.weather_raw ADD PARTITION(year=2013) LOCATION 's3://dimajix-training/data/weather/2013';
ALTER TABLE training.weather_raw ADD PARTITION(year=2014) LOCATION 's3://dimajix-training/data/weather/2014';
ALTER TABLE training.weather_raw ADD PARTITION(year=2015) LOCATION 's3://dimajix-training/data/weather/2015';
ALTER TABLE training.weather_raw ADD PARTITION(year=2016) LOCATION 's3://dimajix-training/data/weather/2016';
ALTER TABLE training.weather_raw ADD PARTITION(year=2017) LOCATION 's3://dimajix-training/data/weather/2017';
ALTER TABLE training.weather_raw ADD PARTITION(year=2018) LOCATION 's3://dimajix-training/data/weather/2018';
ALTER TABLE training.weather_raw ADD PARTITION(year=2019) LOCATION 's3://dimajix-training/data/weather/2019';

-- Workign with the raw data is complicated, so we create a VIEW on top of the table. The view will take
-- care of extracting all the values from the raw data and can later be accessed as if it was a simple
-- table.

-- Create View for extracting relevant measurements
CREATE VIEW training.weather AS
    SELECT 
        year,
        SUBSTR(`data`,5,6) AS `usaf`,
        SUBSTR(`data`,11,5) AS `wban`, 
        SUBSTR(`data`,16,8) AS `date`, 
        SUBSTR(`data`,24,4) AS `time`,
        SUBSTR(`data`,42,5) AS report_type,
        SUBSTR(`data`,61,3) AS wind_direction, 
        SUBSTR(`data`,64,1) AS wind_direction_qual, 
        SUBSTR(`data`,65,1) AS wind_observation, 
        CAST(SUBSTR(`data`,66,4) AS FLOAT)/10 AS wind_speed,
        SUBSTR(`data`,70,1) AS wind_speed_qual,
        CAST(SUBSTR(`data`,88,5) AS FLOAT)/10 AS air_temperature, 
        SUBSTR(`data`,93,1) AS air_temperature_qual 
    FROM training.weather_raw;

-- Look into VIEW
SELECT * FROM training.weather LIMIT 10;


----------------------------------------------------------------------------------------------
-- 
-- 4. Create Table for Stations Data
--
-- So far we only took care of the measurement data. But we also need to create a Hive table
-- containing the stations master data, which then can be joined using the two columns
-- `USAF` and `WBAN`.

-- Create Hive table for the master data. The columns need to match the structure of the CSV
-- files.
CREATE EXTERNAL TABLE training.stations(
    usaf STRING,
    wban STRING,
    name STRING,
    country STRING,
    state STRING,
    icao STRING,
    latitude FLOAT,
    longitude FLOAT,
    elevation FLOAT,
    date_begin STRING,
    date_end STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/weather/isd-history'
TBLPROPERTIES ("skip.header.line.count"="1");


----------------------------------------------------------------------------------------------
-- 
-- 5. Perform analysis
--
-- Finally, we can now conduct the same analysis (minimum and maximum temperature and wind 
-- speed per year and country), as we did before with Spark.

-- Performing Query
SELECT
    w.year,
    isd.country,
    MIN(CASE WHEN w.air_temperature_qual="1" THEN w.air_temperature END) as temp_min,
    MAX(CASE WHEN w.air_temperature_qual="1" THEN w.air_temperature END) as temp_max,
    MIN(CASE WHEN w.wind_speed_qual="1" THEN w.wind_speed END) as wind_min,
    MAX(CASE WHEN w.wind_speed_qual="1" THEN w.wind_speed END) as wind_max
FROM training.weather w
INNER JOIN training.stations isd
    ON w.usaf=isd.usaf 
    AND w.wban=isd.wban
GROUP BY year, isd.country;

