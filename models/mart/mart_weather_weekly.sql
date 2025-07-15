WITH daily AS (
    SELECT 
        *,
        DATE_PART('week', date) AS calendar_week,
        DATE_PART('year', date) AS date_year
    FROM {{ ref('prep_weather_daily') }}
)

SELECT
    airport_code,
    date_year,
    calendar_week,

    -- Aggregated metrics
    AVG(avg_temp_c) AS avg_temp_c,
    MIN(min_temp_c) AS min_temp_c,
    MAX(max_temp_c) AS max_temp_c,
    SUM(precipitation_mm) AS total_precipitation_mm,
    MAX(max_snow_mm) AS max_snow_mm,
    SUM(sun_minutes) AS total_sun_minutes,
    AVG(avg_wind_direction) AS avg_wind_direction,
    AVG(avg_wind_speed_kmh) AS avg_wind_speed_kmh,
    MAX(wind_peakgust_kmh) AS max_wind_peakgust_kmh

FROM daily
GROUP BY airport_code, date_year, calendar_week
ORDER BY airport_code, date_year, calendar_week