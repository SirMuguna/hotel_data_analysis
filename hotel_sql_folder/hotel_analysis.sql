-- Database: hotel

-- DROP DATABASE IF EXISTS hotel;

CREATE DATABASE hotel
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Create table meal_cost and import the table to postgres data base.
CREATE TABLE meal_cost(
    cost double precision,
    meal text
);
COPY year_2018 FROM 'C:/Program Files/PostgreSQL/15/data/projects/Hotel_Analysis/meal_cost.csv' WITH (FORMAT csv, HEADER true);

-- Create table market_segment and import the table to postgres data base.
CREATE TABLE .market_segment(
    discount double precision,
    market_segment text
);
COPY year_2018 FROM 'C:/Program Files/PostgreSQL/15/data/projects/Hotel_Analysis/market_segment.csv' WITH (FORMAT csv, HEADER true);

-- Create table year_2018 and import the table to postgres data base.
create table year_2018(
    hotel text,
    is_canceled integer,
    lead_time integer,
    arrival_date_year integer,
    arrival_date_month text,
    arrival_date_week_number integer,
    arrival_date_day_of_month integer,
    stays_in_weekend_nights integer,
    stays_in_week_nights integer,
    adults integer,
    children text,
    babies integer,
    meal text,
    country text,
    market_segment text,
    distribution_channel text,
    is_repeated_guest integer,
    previous_cancellations integer,
    previous_bookings_not_canceled integer,
    reserved_room_type text,
    assigned_room_type text,
    booking_changes integer,
    deposit_type text,
    agent text,
    company text,
    days_in_waiting_list integer,
    customer_type text,
    adr text,
    required_car_parking_spaces integer,
    total_of_special_requests integer,
    reservation_status text,
    reservation_status_date date

);

COPY year_2018 FROM 'C:/Program Files/PostgreSQL/15/data/projects/Hotel_Analysis/year_2018.csv' WITH (FORMAT csv, HEADER true);

-- Create table year_2019 and import the table to postgres data base.
create table year_2019(
    hotel text,
    is_canceled integer,
    lead_time integer,
    arrival_date_year integer,
    arrival_date_month text,
    arrival_date_week_number integer,
    arrival_date_day_of_month integer,
    stays_in_weekend_nights integer,
    stays_in_week_nights integer,
    adults integer,
    children text,
    babies integer,
    meal text,
    country text,
    market_segment text,
    distribution_channel text,
    is_repeated_guest integer,
    previous_cancellations integer,
    previous_bookings_not_canceled integer,
    reserved_room_type text,
    assigned_room_type text,
    booking_changes integer,
    deposit_type text,
    agent text,
    company text,
    days_in_waiting_list integer,
    customer_type text,
    adr text,
    required_car_parking_spaces integer,
    total_of_special_requests integer,
    reservation_status text,
    reservation_status_date date

);

COPY year_2019 FROM 'C:/Program Files/PostgreSQL/15/data/projects/Hotel_Analysis/year_2019.csv' WITH (FORMAT csv, HEADER true);

-- Create table year_2020 and import the table to postgres data base.
create table year_2020(
    hotel text,
    is_canceled integer,
    lead_time integer,
    arrival_date_year integer,
    arrival_date_month text,
    arrival_date_week_number integer,
    arrival_date_day_of_month integer,
    stays_in_weekend_nights integer,
    stays_in_week_nights integer,
    adults integer,
    children text,
    babies integer,
    meal text,
    country text,
    market_segment text,
    distribution_channel text,
    is_repeated_guest integer,
    previous_cancellations integer,
    previous_bookings_not_canceled integer,
    reserved_room_type text,
    assigned_room_type text,
    booking_changes integer,
    deposit_type text,
    agent text,
    company text,
    days_in_waiting_list integer,
    customer_type text,
    adr text,
    required_car_parking_spaces integer,
    total_of_special_requests integer,
    reservation_status text,
    reservation_status_date date

);

COPY year_2020 FROM 'C:/Program Files/PostgreSQL/15/data/projects/Hotel_Analysis/year_2020.csv' WITH (FORMAT csv, HEADER true);


-- Create a new table for the 3 years with distinct values.

CREATE TABLE combined_year AS
SELECT *
FROM year_2018
UNION
SELECT *
FROM year_2019
UNION
SELECT *
FROM year_2020;

-- Create a table joining the combined_year, market_segment & meal_cost tables.

CREATE TABLE hotel_data AS
SELECT *
FROM combined_year
LEFT JOIN market_segment USING(market_segment)
LEFT JOIN meal_cost USING(meal);

-- Export the table to our local machine
COPY hotel_data TO 'C:\Program Files\PostgreSQL\15\data\projects\Hotel_Analysis\hotel_data.csv' WITH (FORMAT CSV, HEADER);

-- Show the top 10 rows of the new table.
SELECT *
FROM hotel_data;



-- Get revenue by year and hotel type
SELECT arrival_date_year as "date year",
	hotel,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * adr::numeric),
		2) AS "total revenue"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get Discount by year and hotel type
SELECT arrival_date_year AS "date year",
	hotel,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * discount::numeric),
		2) AS "discount offered"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get revenue by year and customer type
SELECT arrival_date_year as "date year",
	customer_type,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * adr::numeric),
		2) AS "total revenue"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get Discount by year and customer type
SELECT arrival_date_year AS "date year",
	customer_type,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * discount::numeric),
		2) AS "discount offered"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get Revenue by year and market segment
SELECT arrival_date_year AS "date year",
	market_segment,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * adr::numeric),
		2) AS revenue
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get Discount by year and market segment
SELECT arrival_date_year AS "date year",
	market_segment,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * discount::numeric),
		2) AS discount_offered
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get meal cost by year and hotel type
SELECT arrival_date_year as "date year",
	hotel,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * cost::numeric),
		2) AS "meal cost"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get meal cost by year and market segment
SELECT arrival_date_year as "date year",
	market_segment,
	round(sum((stays_in_week_nights + stays_in_weekend_nights::numeric) * cost::numeric),
		2) AS "meal cost"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

-- Get the Occupancy days by year and customer type.
SELECT arrival_date_year AS "date year",
	customer_type,
	sum((stays_in_week_nights + stays_in_weekend_nights::numeric)) AS "days"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

SELECT date_year, customer_type, days,
       round(100.0 * (days - LAG(days) OVER (PARTITION BY customer_type ORDER BY date_year)) / LAG(days) OVER (PARTITION BY customer_type ORDER BY date_year), 2) AS yearly_change_pct
FROM (
    SELECT arrival_date_year AS date_year, customer_type, 
           sum(stays_in_week_nights + stays_in_weekend_nights) AS days
    FROM hotel_data
    GROUP BY 1, 2
) subquery
ORDER BY customer_type, date_year;

-- Get the Occupancy days by year and hotel type.
SELECT arrival_date_year AS "date year",
	hotel,
	sum((stays_in_week_nights + stays_in_weekend_nights::numeric)) AS "days"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

SELECT date_year, hotel, days,
       round(100.0 * (days - LAG(days) OVER (PARTITION BY hotel ORDER BY date_year)) / LAG(days) OVER (PARTITION BY hotel ORDER BY date_year), 2) AS yearly_change_pct
FROM (
    SELECT arrival_date_year AS date_year, hotel, 
           sum(stays_in_week_nights + stays_in_weekend_nights) AS days
    FROM hotel_data
    GROUP BY 1, 2
) subquery
ORDER BY hotel, date_year;

-- Get the Occupancy days by year and market segment.
SELECT arrival_date_year AS "date year",
	market_segment,
	sum((stays_in_week_nights + stays_in_weekend_nights::numeric)) AS "days"
FROM hotel_data
GROUP BY 1, 2
ORDER BY 2, 1;

SELECT date_year, market_segment, days,
       round(100.0 * (days - LAG(days) OVER (PARTITION BY market_segment ORDER BY date_year)) / LAG(days) OVER (PARTITION BY market_segment ORDER BY date_year), 2) AS yearly_change_pct
FROM (
    SELECT arrival_date_year AS date_year, market_segment, 
           sum(stays_in_week_nights + stays_in_weekend_nights) AS days
    FROM hotel_data
    GROUP BY 1, 2
) subquery
ORDER BY market_segment, date_year;




































































