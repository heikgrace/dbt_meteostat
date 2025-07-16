WITH flights_cleaned AS (
    SELECT 
        origin AS airport_code,
        season,
        arr_delay,
        cancelled,
        diverted
    FROM {{ ref('prep_flights') }}
    WHERE arr_delay IS NOT NULL OR cancelled = 1 OR diverted = 1
),

seasonal_stats AS (
    SELECT 
        airport_code,
        season,
        COUNT(*) AS total_flights,
        AVG(arr_delay) AS avg_arrival_delay,
        MAX(arr_delay) AS max_arrival_delay,
        MIN(arr_delay) AS min_arrival_delay,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,
        ROUND(100.0 * SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancel_rate_percent,
        ROUND(100.0 * SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS diversion_rate_percent
    FROM flights_cleaned
    GROUP BY airport_code, season
),

airport_info AS (
    SELECT 
        faa AS airport_code,
        name AS airport_name,
        city,
        country
    FROM {{ ref('prep_airports') }}
)

SELECT 
    s.airport_code,
    ai.airport_name,
    ai.city,
    ai.country,
    s.season,
    s.total_flights,
    ROUND(s.avg_arrival_delay, 2) AS avg_arrival_delay,
    s.max_arrival_delay,
    s.min_arrival_delay,
    s.total_cancelled,
    s.total_diverted,
    s.cancel_rate_percent,
    s.diversion_rate_percent
FROM seasonal_stats s
LEFT JOIN airport_info ai 
    ON s.airport_code = ai.airport_code
ORDER BY s.airport_code, s.season