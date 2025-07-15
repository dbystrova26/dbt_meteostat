WITH airline_stats AS (
SELECT airline 
, ROUND(AVG(dep_delay),2) AS avg_dep_delay
, ROUND(AVG(arr_delay),2) AS avg_arr_delay
, ROUND(AVG(cancelled::INT)*100,2) AS cancelled_rate_pct
, ROUND(AVG(diverted::INT)*100,2) AS diversion_rate_pct
FROM {{ ref('prep_flights') }}
GROUP BY airlines
)

SELECT *
FROM airline_stats
ORDER BY cancelled_rate_pct DESC