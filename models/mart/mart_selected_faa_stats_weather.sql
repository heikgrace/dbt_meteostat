WITH flights_cleaned AS (
    SELECT * 
    FROM {{ ref('prep_flights') }}
),
weather_daily AS (
    SELECT * 
    FROM {{ ref('prep_weather_daily') }}
),
airports AS (
    SELECT faa, city, country, name
    FROM {{ ref('prep_airports') }}
),
flights_per_airport_day AS (
    SELECT
        flight_date,
        airport AS airport_code,
        COUNT(DISTINCT CASE WHEN direction = 'dep' THEN dest END) AS num_unique_departures,
        COUNT(DISTINCT CASE WHEN direction = 'arr' THEN origin END) AS num_unique_arrivals,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS canceled_flights,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS diverted_flights,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS actual_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines
    FROM (
        -- Departures
        SELECT 
            flight_date,
            origin AS airport,
            dest,
            NULL::TEXT AS origin, -- needed for symmetry in UNION
            cancelled,
            diverted,
            tail_number,
            airline,
            'dep' AS direction
        FROM flights_cleaned

        UNION ALL

        -- Arrivals
        SELECT 
            flight_date,
            dest AS airport,
            NULL::TEXT AS dest,
            origin,
            cancelled,
            diverted,
            tail_number,
            airline,
            'arr' AS direction
        FROM flights_cleaned
    ) AS unioned_flights
    GROUP BY flight_date, airport
),
final AS (
    SELECT
        weather_daily.date AS flight_date,
        weather_daily.airport_code,
        flights_per_airport_day.num_unique_departures,
        flights_per_airport_day.num_unique_arrivals,
        flights_per_airport_day.total_flights,
        flights_per_airport_day.canceled_flights,
        flights_per_airport_day.diverted_flights,
        flights_per_airport_day.actual_flights,
        flights_per_airport_day.unique_airplanes,
        flights_per_airport_day.unique_airlines,
        weather_daily.min_temp_c,
        weather_daily.max_temp_c,
        weather_daily.precipitation_mm,
        weather_daily.max_snow_mm,
        weather_daily.avg_wind_direction,
        weather_daily.avg_wind_speed_kmh,
        weather_daily.wind_peakgust_kmh,
        airports.city,
        airports.country,
        airports.name AS airport_name
    FROM weather_daily
    LEFT JOIN flights_per_airport_day 
        ON weather_daily.airport_code = flights_per_airport_day.airport_code AND weather_daily.date = flights_per_airport_day.flight_date
    LEFT JOIN airports 
        ON weather_daily.airport_code = airports.faa
)

SELECT *
FROM final
ORDER BY flight_date, airport_code