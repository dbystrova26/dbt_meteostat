-- only the airports we collected the weather data for
-- unique number of departures connections
-- unique number of arrival connections
-- how many flight were planned in total (departures & arrivals)
-- how many flights were canceled in total (departures & arrivals)
-- how many flights were diverted in total (departures & arrivals)
-- how many flights actually occured in total (departures & arrivals)
-- *(optional) how many unique airplanes travelled on average*
-- *(optional) how many unique airlines were in service  on average* 
-- (optional) add city, country and name of the airport
-- daily min temperature
-- daily max temperature
-- daily precipitation 
-- daily snow fall
-- daily average wind direction 
-- daily average wind speed
-- daily wnd peakgust

WITH flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

weather AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
),

-- Departures: from origin airport
departures AS (
    SELECT 
        origin AS airport_code,
        flight_date AS date,
        COUNT(DISTINCT dest) AS unique_departures,
        COUNT(*) AS total_departures,
        COUNT(*) FILTER (WHERE cancelled = 1) AS cancelled_departures,
        COUNT(*) FILTER (WHERE diverted = 1) AS diverted_departures,
        COUNT(*) FILTER (WHERE cancelled = 0 AND diverted = 0) AS completed_departures,
        COUNT(DISTINCT tail_number) AS unique_planes_dep,
        COUNT(DISTINCT airline) AS unique_airlines_dep
    FROM flights
    GROUP BY origin, flight_date
),

-- Arrivals: to destination airport
arrivals AS (
    SELECT 
        dest AS airport_code,
        flight_date AS date,
        COUNT(DISTINCT origin) AS unique_arrivals,
        COUNT(*) AS total_arrivals,
        COUNT(*) FILTER (WHERE cancelled = 1) AS cancelled_arrivals,
        COUNT(*) FILTER (WHERE diverted = 1) AS diverted_arrivals,
        COUNT(*) FILTER (WHERE cancelled = 0 AND diverted = 0) AS completed_arrivals,
        COUNT(DISTINCT tail_number) AS unique_planes_arr,
        COUNT(DISTINCT airline) AS unique_airlines_arr
    FROM flights
    GROUP BY dest, flight_date
),

-- Combine both departures and arrivals for each airport-date
combined AS (
    SELECT 
        d.airport_code,
        d.date,
        d.unique_departures,
        a.unique_arrivals,
        d.total_departures + a.total_arrivals AS total_flights,
        d.cancelled_departures + a.cancelled_arrivals AS total_cancelled,
        d.diverted_departures + a.diverted_arrivals AS total_diverted,
        d.completed_departures + a.completed_arrivals AS total_completed,
        ROUND((d.unique_planes_dep + a.unique_planes_arr) / 2.0, 0) AS avg_unique_planes,
        ROUND((d.unique_airlines_dep + a.unique_airlines_arr) / 2.0, 0) AS avg_unique_airlines
    FROM departures d
    JOIN arrivals a 
      ON d.airport_code = a.airport_code AND d.date = a.date

    UNION

    -- Only departures (no arrivals)
    SELECT 
        d.airport_code,
        d.date,
        d.unique_departures,
        NULL,
        d.total_departures,
        d.cancelled_departures,
        d.diverted_departures,
        d.completed_departures,
        d.unique_planes_dep,
        d.unique_airlines_dep
    FROM departures d
    LEFT JOIN arrivals a 
      ON d.airport_code = a.airport_code AND d.date = a.date
    WHERE a.airport_code IS NULL

    UNION

    -- Only arrivals (no departures)
    SELECT 
        a.airport_code,
        a.date,
        NULL,
        a.unique_arrivals,
        a.total_arrivals,
        a.cancelled_arrivals,
        a.diverted_arrivals,
        a.completed_arrivals,
        a.unique_planes_arr,
        a.unique_airlines_arr
    FROM arrivals a
    LEFT JOIN departures d 
      ON a.airport_code = d.airport_code AND a.date = d.date
    WHERE d.airport_code IS NULL
),

-- Combine with weather
weather_mart AS (
    SELECT 
        w.date,
        w.airport_code,
        c.unique_departures,
        c.unique_arrivals,
        c.total_flights,
        c.total_cancelled,
        c.total_diverted,
        c.total_completed,
        c.avg_unique_planes,
        c.avg_unique_airlines,
        w.min_temp_c,
        w.max_temp_c,
        w.precipitation_mm,
        w.max_snow_mm,
        w.avg_wind_direction,
        w.avg_wind_speed_kmh,
        w.wind_peakgust_kmh
    FROM prep_weather_daily w
    LEFT JOIN combined c 
      ON w.airport_code = c.airport_code AND w.date = c.date
),

-- Add metadata
final AS (
    SELECT 
        wm.*,
        a.country,
        a.region
    FROM weather_mart wm
    LEFT JOIN {{ ref('prep_airports') }} a 
      ON wm.airport_code = a.faa
)

SELECT *
FROM final
ORDER BY date, airport_code