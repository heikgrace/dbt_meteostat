WITH flights_cleaned AS (
    SELECT * 
    FROM {{ref('prep_flights')}}
),
unique_departures AS (
    SELECT COUNT(DISTINCT origin) AS num_unique_departures -- unique number of departures connections
    FROM flights_cleaned
),
unique_arrivals AS ( 
    SELECT COUNT(DISTINCT dest) AS num_unique_arrivals -- unique number of arrival connections
    FROM flights_cleaned
),
total_planned AS (
    SELECT COUNT(*) AS total_flights -- how many flight were planned in total (departures & arrivals)
    FROM flights_cleaned
),
total_cancelled AS (
    SELECT COUNT(*) AS cancelled_flights -- how many flights were canceled in total (departures & arrivals)
    FROM flights_cleaned
    WHERE cancelled = 1
),
total_diverted AS (
    SELECT COUNT(*) AS diverted_flights -- how many flights were diverted in total (departures & arrivals)
    FROM flights_cleaned
    WHERE diverted = 1
),
total_actual AS ( 
    SELECT COUNT(*) AS actual_flights -- how many flights actually occured in total (departures & arrivals)
    FROM flights_cleaned
    WHERE cancelled = 0 AND diverted = 0 
),
avg_unique_airplanes_travelled AS (
    SELECT ROUND(AVG(unique_planes), 2) AS avg_unique_airplanes_per_day
    FROM (  
        SELECT flight_date, COUNT(DISTINCT tail_number) AS unique_planes -- (optional) how many unique airplanes travelled on average
        FROM flights_cleaned 
        WHERE tail_number IS NOT NULL
        GROUP BY flight_date
    ) subquery
),
avg_unique_airlines_per_day AS ( 
    SELECT ROUND(AVG(unique_airline), 2) AS avg_unique_airlines_per_day
    FROM (  
        SELECT flight_date, COUNT(DISTINCT airline) AS unique_airline -- (optional) how many unique airlines were in service on average
        FROM flights_cleaned 
        GROUP BY flight_date
    ) subquery
),
airports_cleaned AS (
    SELECT faa, name, city, country -- add city, country and name of the airport
    FROM {{ref('prep_airports')}}
)
SELECT 
    airports_cleaned.city,
    airports_cleaned.country,
    airports_cleaned.faa,
    airports_cleaned.name,
    num_unique_departures,
    num_unique_arrivals,
    total_flights,
    cancelled_flights,
    diverted_flights,
    actual_flights,
    avg_unique_airplanes_per_day,
    avg_unique_airlines_per_day
FROM unique_departures, airports_cleaned
CROSS JOIN unique_arrivals 
CROSS JOIN total_planned 
CROSS JOIN total_cancelled 
CROSS JOIN total_diverted 
CROSS JOIN total_actual 
CROSS JOIN avg_unique_airplanes_travelled 
CROSS JOIN avg_unique_airlines_per_day