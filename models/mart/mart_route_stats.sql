-- origin airport code
-- destination airport code 
-- total flights on this route
-- unique airplanes
-- unique airlines
-- on average what is the actual elapsed time
-- on average what is the delay on arrival
-- what was the max delay?
-- what was the min delay?
-- total number of cancelled 
--total number of diverted
-- add city, country and name for both, origin and destination, airports

WITH all_flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

route_stats AS (
    SELECT 
        origin, -- origin airport code
        dest,-- destination airport code
        COUNT(*) AS total_flights,-- total flights on this route
        COUNT(DISTINCT tail_number) AS unique_airplanes,-- unique airplanes
        COUNT(DISTINCT airline) AS unique_airlines,-- unique airlines
        ROUND(AVG(actual_elapsed_time), 1) AS avg_actual_elapsed_time,-- on average what is the actual elapsed time
        ROUND(AVG(arr_delay), 1) AS avg_arrival_delay,-- on average what is the delay on arrival
        MAX(arr_delay) AS max_arrival_delay,-- what was the max delay?
        MIN(arr_delay) AS min_arrival_delay,-- what was the min delay?
        COUNT(*) FILTER (WHERE cancelled = 1) AS total_cancelled,-- total number of cancelled 
        COUNT(*) FILTER (WHERE diverted = 1) AS total_diverted--total number of diverted
    FROM all_flights
    GROUP BY origin, dest
),

joined_with_airports AS (
    SELECT 
        route_stats.*, -- bring all columns from route_stats
        ao.country AS origin_country,--bring in additional columns from the joined tables
        ao.region AS origin_region,
        ad.country AS dest_country,
        ad.region AS dest_region
    FROM route_stats r
    LEFT JOIN {{ ref('prep_airports') }} ao ON r.origin = ao.faa
    LEFT JOIN {{ ref('prep_airports') }} ad ON r.dest = ad.faa
)

SELECT *
FROM joined_with_airports