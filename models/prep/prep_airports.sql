WITH airports_reorder AS (
        SELECT faa
        	   , country
               , region
        FROM {{ref('staging_airports')}}
    )
    SELECT * FROM airports_reorder