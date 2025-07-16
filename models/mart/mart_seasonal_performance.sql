WITH weather_season_by_airport_date AS (
    SELECT
        airport_code,
        date,
        CASE 
            WHEN month IN (12, 1, 2) THEN 'winter'
            WHEN month IN (3, 4, 5) THEN 'spring'
            WHEN month IN (6, 7, 8) THEN 'summer'
            WHEN month IN (9, 10, 11) THEN 'autumn'
        END AS season
    FROM (
        SELECT
            airport_code,
            date,
            EXTRACT(MONTH FROM date) AS month
        FROM {{ ref('prep_weather_daily') }}
    ) sub
),

flights_with_season AS (
    SELECT 
        f.origin AS airport_code,
        f.flight_date,
        w.season,
        f.arr_delay,
        f.cancelled,
        f.diverted
    FROM {{ ref('prep_flights') }} f
    LEFT JOIN weather_season_by_airport_date w
        ON f.flight_date = w.date
        AND f.origin = w.airport_code
    WHERE w.season IS NOT NULL -- ensures each flight has a season
),

seasonal_stats AS (
    SELECT 
        airport_code,
        season,
        COUNT(*) AS total_flights,
        ROUND(AVG(arr_delay), 2) AS avg_arrival_delay,
        MAX(arr_delay) AS max_arrival_delay,
        MIN(arr_delay) AS min_arrival_delay,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,
        ROUND(100.0 * SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancel_rate_percent,
        ROUND(100.0 * SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS diversion_rate_percent
    FROM flights_with_season
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
    s.avg_arrival_delay,
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
