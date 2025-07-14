WITH daily_data AS (
        SELECT * 
        FROM {{ref('staging_weather_daily')}}
    ),
    add_features AS (
        SELECT *
    		, EXTRACT(DAY FROM date) AS date_day 		-- number of the day of month
    		, EXTRACT(MONTH FROM date) AS date_month 	-- number of the month of year
    		, EXTRACT(YEAR FROM date) AS date_year 		-- number of year
    		, EXTRACT(WEEK FROM date) AS cw 			-- number of the week of year
    		, TRIM(TO_CHAR(date, 'Month')) AS month_name 	-- name of the month
    		, TRIM(TO_CHAR(date, 'Day')) AS weekday 		-- name of the weekday
        FROM daily_data 
    ),
    add_more_features AS (
        SELECT *
    		, (CASE 
    			WHEN month_name IN ('December', 'January', 'February')  THEN 'winter'
    			WHEN month_name IN ('March', 'April', 'May') THEN 'spring'
                WHEN month_name IN ('June', 'July', 'August') THEN 'summer'
                WHEN month_name IN ('September', 'October', 'November') THEN 'autumn'
    		END) AS season
        FROM add_features
    )
    SELECT *
    FROM add_more_features
    ORDER BY date