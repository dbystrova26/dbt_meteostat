WITH daily AS (
    SELECT 
        *,
        DATE_PART('week', date) AS calendar_week,
        DATE_PART('year', date) AS year_extracted
    FROM {{ ref('prep_weather_daily') }}
)

SELECT
    d.airport_code,
    d.year_extracted AS date_year,
    d.calendar_week,

    -- Aggregated metrics
    AVG(d.avg_temp_c) AS avg_temp_c,
    MIN(d.min_temp_c) AS min_temp_c,
    MAX(d.max_temp_c) AS max_temp_c,
    SUM(d.precipitation_mm) AS total_precipitation_mm,
    MAX(d.max_snow_mm) AS max_snow_mm,
    SUM(d.sun_minutes) AS total_sun_minutes,
    AVG(d.avg_wind_direction) AS avg_wind_direction,
    AVG(d.avg_wind_speed_kmh) AS avg_wind_speed_kmh,
    MAX(d.wind_peakgust_kmh) AS max_wind_peakgust_kmh

FROM daily d
GROUP BY d.airport_code, d.year_extracted, d.calendar_week
ORDER BY d.airport_code, d.year_extracted, d.calendar_week