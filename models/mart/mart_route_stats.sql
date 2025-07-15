WITH flights_cleaned AS (
    SELECT * 
    FROM {{ ref('prep_flights') }}
),
airports AS (
    SELECT faa, city, country, name
    FROM {{ ref('prep_airports') }}
),
route_stats AS (
    SELECT
        origin,
        dest,
        COUNT(*) AS total_flights, -- total number of flights
        COUNT(DISTINCT tail_number) AS unique_airplanes, -- number of unique airplanes
        COUNT(DISTINCT airline) AS unique_airlines, -- number of unique airlines
        ROUND(AVG(actual_elapsed_time), 2) AS avg_actual_elapsed_time, -- average actual flight time
        ROUND(AVG(arr_delay), 2) AS avg_arrival_delay, -- average arrival delay
        MAX(arr_delay) AS max_arrival_delay, -- maximum arrival delay
        MIN(arr_delay) AS min_arrival_delay, -- minimum arrival delay
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled, -- total cancelled flights
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted -- total diverted flights
    FROM flights_cleaned
    GROUP BY origin, dest
),
joined_with_airports AS (
    SELECT 
        route_stats.*,
        airports_origin.city AS origin_city,
        airports_origin.country AS origin_country,
        airports_origin.name AS origin_airport_name,
        airports_dest.city AS dest_city,
        airports_dest.country AS dest_country,
        airports_dest.name AS dest_airport_name
    FROM route_stats
    LEFT JOIN airports airports_origin ON route_stats.origin = airports_origin.faa
    LEFT JOIN airports airports_dest ON route_stats.dest = airports_dest.faa
)

SELECT *
FROM joined_with_airports
ORDER BY total_flights DESC