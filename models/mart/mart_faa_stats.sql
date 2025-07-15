-- unique number of departures connections
--  unique number of arrival connections
-- how many flight were planned in total (departures & arrivals)
--  how many flights were canceled in total (departures & arrivals)
--  how many flights were diverted in total (departures & arrivals)
--  how many flights actually occured in total (departures & arrivals)
--  *(optional) how many unigique airplanes travelled on average*
--  *(optional) how many unique airlines were in service  on average* 
--  add city, country and name of the airport

WITH all_flights AS (
   SELECT *
   FROM {{ ref('prep_flights') }}
),
departures AS (
   SELECT origin AS faa
        , COUNT(DISTINCT dest) AS unique_departures
        , COUNT(*) AS total_departures
        , COUNT(*) FILTER (WHERE cancelled = 1) AS cancelled_departures
        , COUNT(*) FILTER (WHERE diverted = 1) AS diverted_departures
        , COUNT(*) FILTER (WHERE cancelled = 0 AND diverted = 0) AS completed_departures
        , COUNT(DISTINCT tail_number) AS unique_planes_dep
        , COUNT(DISTINCT airline) AS unique_airlines_dep
   FROM all_flights
   GROUP BY origin
),
arrivals AS (
   SELECT dest AS faa
        , COUNT(DISTINCT origin) AS unique_arrivals
        , COUNT(*) AS total_arrivals
        , COUNT(*) FILTER (WHERE cancelled = 1) AS cancelled_arrivals
        , COUNT(*) FILTER (WHERE diverted = 1) AS diverted_arrivals
        , COUNT(*) FILTER (WHERE cancelled = 0 AND diverted = 0) AS completed_arrivals
        , COUNT(DISTINCT tail_number) AS unique_planes_arr
        , COUNT(DISTINCT airline) AS unique_airlines_arr
   FROM all_flights
   GROUP BY dest
),
combined AS (
   SELECT d.faa
        , unique_departures
        , unique_arrivals
        , total_departures + total_arrivals AS total_flights
        , cancelled_departures + cancelled_arrivals AS total_cancelled
        , diverted_departures + diverted_arrivals AS total_diverted
        , completed_departures + completed_arrivals AS total_completed
        , ROUND((unique_planes_dep + unique_planes_arr) / 2.0, 0) AS avg_unique_planes
        , ROUND((unique_airlines_dep + unique_airlines_arr) / 2.0, 0) AS avg_unique_airlines
   FROM departures d
   LEFT JOIN arrivals a ON d.faa = a.faa


   UNION


   SELECT a.faa
        , unique_departures
        , unique_arrivals
        , total_departures + total_arrivals AS total_flights
        , cancelled_departures + cancelled_arrivals AS total_cancelled
        , diverted_departures + diverted_arrivals AS total_diverted
        , completed_departures + completed_arrivals AS total_completed
        , ROUND((unique_planes_dep + unique_planes_arr) / 2.0, 0) AS avg_unique_planes
        , ROUND((unique_airlines_dep + unique_airlines_arr) / 2.0, 0) AS avg_unique_airlines
   FROM arrivals a
   LEFT JOIN departures d ON a.faa = d.faa
   WHERE d.faa IS NULL
),
final AS (
   SELECT c.*
        , a.region
        , a.country
        , a.name
   FROM combined c
   LEFT JOIN {{ ref('prep_airports') }} a USING (faa)
)
SELECT *
FROM final
