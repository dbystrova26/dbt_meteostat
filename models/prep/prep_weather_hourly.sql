WITH hourly_data AS ( 
    SELECT * 
    FROM {{ ref('staging_weather_hourly') }}
),

add_features AS (
    SELECT *
        , timestamp::DATE AS date                             -- only date
        , timestamp::TIME AS time                             -- only time (TIME data type)
        , TO_CHAR(timestamp, 'HH24:MI') AS hour               -- hour:minute as text
        , TO_CHAR(timestamp, 'FMmonth') AS month_name         -- month name as text
        , TO_CHAR(timestamp, 'Day') AS weekday                -- weekday name as text
        , DATE_PART('day', timestamp) AS date_day             -- numeric day of month
        , DATE_PART('month', timestamp) AS date_month         -- numeric month
        , DATE_PART('year', timestamp) AS date_year           -- year
        , DATE_PART('week', timestamp) AS cw                  -- calendar week number
    FROM hourly_data
),

add_more_features AS (
    SELECT *
        , CASE 
            WHEN time BETWEEN TIME '00:00:00' AND TIME '06:00:00' THEN 'night'
            WHEN time BETWEEN TIME '06:00:01' AND TIME '17:00:00' THEN 'day'
            WHEN time BETWEEN TIME '17:00:01' AND TIME '23:59:59' THEN 'evening'
          END AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features