WITH daily AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
),

weekly_weather AS (
    SELECT
        airport_code,
        date_year,
        cw AS calendar_week,
        MIN(date) AS week_start,
        MAX(date) AS week_end,

        ROUND(AVG(avg_temp_c), 1) AS avg_temp_c,
        MIN(min_temp_c) AS min_temp_c,
        MAX(max_temp_c) AS max_temp_c,

        SUM(precipitation_mm) AS total_precipitation_mm,
        MAX(max_snow_mm) AS max_snow_mm,

        ROUND(AVG(avg_wind_speed_kmh), 1) AS avg_wind_speed_kmh,
        MAX(wind_peakgust_kmh) AS max_wind_gust_kmh,

        ROUND(AVG(avg_pressure_hpa), 1) AS avg_pressure_hpa,

        SUM(sun_minutes) AS total_sun_minutes

        -- Windrichtung separat (siehe unten)
    FROM daily
    GROUP BY airport_code, date_year, cw
),

wind_mode AS (
    SELECT
        airport_code,
        date_year,
        cw AS calendar_week,
        avg_wind_direction AS most_common_wind_direction
    FROM (
        SELECT 
            airport_code,
            date_year,
            cw,
            avg_wind_direction,
            ROW_NUMBER() OVER (PARTITION BY airport_code, date_year, cw ORDER BY COUNT(*) DESC) AS rn
        FROM daily
        WHERE avg_wind_direction IS NOT NULL
        GROUP BY airport_code, date_year, cw, avg_wind_direction
    ) ranked
    WHERE rn = 1
)

SELECT 
    weekly_weather.*,
    wind_mode.most_common_wind_direction
FROM weekly_weather 
LEFT JOIN wind_mode
  ON weekly_weather.airport_code = wind_mode.airport_code
 AND weekly_weather.date_year = wind_mode.date_year
 AND weekly_weather.calendar_week = wind_mode.calendar_week
