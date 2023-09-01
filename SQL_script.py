# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 15:15:51 2023

@author: 98yan
"""



#SQL queries
#Those query were executed in Google Cloud Big Query
#cleaning data set
select * from bigquery-public-data.iowa_liquor_sales.sales
where date between date('2019-01-01') and date('2022-12-31')

#in 2019 - 2022
#1
with item_sold_city_2019 as (select city , sum(bottles_sold) as item_sold from bigquery-public-data.iowa_liquor_sales.sales
WHERE date BETWEEN DATE('2019-01-01') AND DATE('2019-12-31')
group by city
order by item_sold DESC),

item_sold_city_2020 as (select city , sum(bottles_sold) as item_sold from bigquery-public-data.iowa_liquor_sales.sales
WHERE date BETWEEN DATE('2020-01-01') AND DATE('2020-12-31')
group by city
order by item_sold DESC),

item_sold_city_2021 as (select city , sum(bottles_sold) as item_sold from bigquery-public-data.iowa_liquor_sales.sales
WHERE date BETWEEN DATE('2021-01-01') AND DATE('2021-12-31')
group by city
order by item_sold DESC),

item_sold_city_2022 as (select city , sum(bottles_sold) as item_sold from bigquery-public-data.iowa_liquor_sales.sales
WHERE date BETWEEN DATE('2022-01-01') AND DATE('2022-12-31')
group by city
order by item_sold DESC)

select a.city,a.item_sold as sales_2019, b.item_sold as sales_2020, c.item_sold as sales_2021,d.item_sold as sales_2022 from item_sold_city_2019 as a 
inner join item_sold_city_2020 as b 
on a.city = b.city
inner join item_sold_city_2021 as c 
on a.city = c.city
inner join item_sold_city_2022 as d
on a.city = d.city 
order by sales_2019 DESC

#2
WITH YearlySales AS (
    SELECT
        city,
        EXTRACT(year FROM date) AS sales_year,
        SUM(bottles_sold) AS item_sold
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2022-12-31')
    GROUP BY city, sales_year
),

top_city as (SELECT
    city,
    MAX(CASE WHEN sales_year = 2019 THEN item_sold END) AS sales_2019,
    MAX(CASE WHEN sales_year = 2020 THEN item_sold END) AS sales_2020,
    MAX(CASE WHEN sales_year = 2021 THEN item_sold END) AS sales_2021,
    MAX(CASE WHEN sales_year = 2022 THEN item_sold END) AS sales_2022
FROM YearlySales
GROUP BY city
ORDER BY sales_2019 DESC)

select city,
sales_2019,
CONCAT(ROUND(((sales_2020 - sales_2019) / sales_2019) * 100, 2),'%') as change19_20,
sales_2020
from top_city

#3
WITH YearlySales AS (
    SELECT
        city,
        EXTRACT(year FROM date) AS sales_year,
        SUM(bottles_sold) AS item_sold
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2022-12-31')
    GROUP BY city, sales_year
),

top_city as (SELECT
    city,
    MAX(CASE WHEN sales_year = 2019 THEN item_sold END) AS sales_2019,
    MAX(CASE WHEN sales_year = 2020 THEN item_sold END) AS sales_2020,
    MAX(CASE WHEN sales_year = 2021 THEN item_sold END) AS sales_2021,
    MAX(CASE WHEN sales_year = 2022 THEN item_sold END) AS sales_2022
FROM YearlySales
GROUP BY city
ORDER BY sales_2019 DESC)

select city,
sales_2020,
CONCAT(ROUND(((sales_2021 - sales_2020) / sales_2020) * 100, 2),'%') as change20_21,
sales_2021
from top_city


#4
WITH YearlySales AS (
    SELECT
        county,
        EXTRACT(year FROM date) AS sales_year,
        SUM(bottles_sold) AS item_sold
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2022-12-31')
    GROUP BY county, sales_year
),

top_county as (SELECT
    county,
    MAX(CASE WHEN sales_year = 2019 THEN item_sold END) AS sales_2019,
    MAX(CASE WHEN sales_year = 2020 THEN item_sold END) AS sales_2020,
    MAX(CASE WHEN sales_year = 2021 THEN item_sold END) AS sales_2021,
    MAX(CASE WHEN sales_year = 2022 THEN item_sold END) AS sales_2022
FROM YearlySales
GROUP BY county
ORDER BY sales_2019 DESC),

change_p AS (
    SELECT county,
    ROUND(((sales_2020 - sales_2019) / sales_2019) * 100, 2) as change19_20,
    ROUND(((sales_2021 - sales_2020) / sales_2020) * 100, 2) as change20_21
    FROM top_county
)
SELECT county, CONCAT(change19_20,'%') as c_19_20, CONCAT(change20_21,'%') as c_20_21, ROUND((change20_21 - change19_20),2) as change_1year
FROM change_p;

    

#5
WITH CountyCounts AS (
    SELECT
        county,
        EXTRACT(year from date) as years,
        SUM(bottles_sold) as item_sold,
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2022-12-31')
    AND county IS NOT NULL
    AND sale_dollars <= 500
    GROUP BY county, years
    ORDER BY item_sold DESC
),


CountySeg as (SELECT 
    years,
    county,
    SUM(IF(item_sold > 200000, 1, 0)) as above_200k_count,
    SUM(IF(item_sold <= 35000, 1, 0)) as below_35k_count
FROM CountyCounts
WHERE item_sold > 0
AND years = 2019
GROUP BY county,years,item_sold
ORDER BY item_sold DESC)



#SELECT  county as rural_counties from CountySeg
#WHERE county IS NOT NULL
#AND below_35k_count = 1
#limit 19

SELECT  county as urban_counties from CountySeg
WHERE county IS NOT NULL
AND above_200k_count = 1
limit 19



#top purchased item
WITH CountyCounts AS (
    SELECT
        county,
        EXTRACT(year from date) as years,
        SUM(bottles_sold) as sales_count,
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2022-12-31')
    AND county IS NOT NULL
    AND sale_dollars <= 500
    GROUP BY county, years
    ORDER BY sales_count DESC
),

Liquor_sold as(
        SELECT county, 
    item_description,
    SUM(bottles_sold) as sold_items,
    sum(volume_sold_liters) as s_liters,
    category_name,
    state_bottle_retail,
    ROW_NUMBER() OVER (PARTITION BY county ORDER BY COUNT(item_description) DESC) AS rn
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2019-12-31')
    AND county IS NOT NULL
    AND item_description IS NOT NULL
    AND volume_sold_liters IS NOT NULL
    GROUP BY county, item_description,category_name,state_bottle_retail
    ORDER BY sold_items DESC
),


CountySeg as (SELECT 
    years,
    county,
    SUM(IF(sales_count > 200000, 1, 0)) as above_200k_count,
    SUM(IF(sales_count <= 35000, 1, 0)) as below_35k_count
FROM CountyCounts
WHERE sales_count > 0
AND years = 2019
GROUP BY county,years,sales_count
ORDER BY sales_count DESC),


Urban_2019 as (SELECT b.county, a.item_description, a.sold_items,a.category_name,a.state_bottle_retail,a.s_liters
FROM Liquor_sold as a
left join CountySeg as b
on a.county = b.county
WHERE a.rn <= 1
AND b.above_200k_count = 1
ORDER BY a.sold_items DESC,county, rn),

Rural_2019 as (
    SELECT b.county, a.item_description, a.sold_items,a.category_name,a.state_bottle_retail,a.s_liters
FROM Liquor_sold as a
left join CountySeg as b
on a.county = b.county
WHERE a.rn <= 1
AND b.below_35k_count = 1
ORDER BY a.sold_items DESC,county, rn
LIMIT 19
)


select 
category_name, count(category_name) as best_seller_in_counties,item_description, state_bottle_retail,ROUND(SUM(s_liters),2) as total_liters,sum(sold_items) as total_sold_bottles
from Urban_2019
group by category_name, item_description, state_bottle_retail
ORDER BY best_seller_in_counties DESC



#Top days for alchohol purchase 
WITH CountyCounts AS (
    SELECT
        county,
        EXTRACT(year from date) as years,
        SUM(bottles_sold) as sales_count,
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2022-12-31')
    AND county IS NOT NULL
    AND sale_dollars <= 500
    GROUP BY county, years
    ORDER BY sales_count DESC
),

County_AVG as (select
county,
FORMAT_DATE('%A', DATE) AS day_of_week, 
AVG(sale_dollars) as avg_sale
from bigquery-public-data.iowa_liquor_sales.sales
WHERE date BETWEEN DATE('2019-01-01') AND DATE('2019-12-31')
group by county, day_of_week
order by avg_sale DESC),

Ranked_avg as (select
county,
day_of_week,
avg_sale,
ROW_NUMBER() OVER(PARTITION BY county ORDER BY avg_sale DESC) as row_num
from County_AVG),


CountySeg as (SELECT 
    years,
    county,
    SUM(IF(sales_count > 200000, 1, 0)) as above_200k_count,
    SUM(IF(sales_count <= 35000, 1, 0)) as below_35k_count
FROM CountyCounts
WHERE sales_count > 0
AND years = 2019
GROUP BY county,years,sales_count
ORDER BY sales_count DESC),


Urban_2019 as (SELECT b.county, a.day_of_week, a.avg_sale
FROM Ranked_avg as a
left join CountySeg as b
on a.county = b.county
WHERE b.above_200k_count = 1
AND a.row_num <= 1
ORDER BY a.avg_sale DESC,b.county,a.row_num),

Rural_2019 as (
    SELECT b.county, a.day_of_week, a.avg_sale
FROM Ranked_avg as a
left join CountySeg as b
on a.county = b.county
WHERE a.row_num <= 1
AND b.below_35k_count = 1
ORDER BY a.avg_sale DESC,b.county,a.row_num
LIMIT 19
)

select day_of_week, avg(avg_sale) as avg_sale from Rural_2019 
group by day_of_week
order by avg_sale DESC

#state trend 
WITH Liquor_sold as(
        SELECT county, 
    SUM(bottles_sold) as sold_items,
    sum(volume_sold_liters) as s_liters,
    state_bottle_retail,
    EXTRACT(year from date) as years
    FROM bigquery-public-data.iowa_liquor_sales.sales
    WHERE date BETWEEN DATE('2019-01-01') AND DATE('2021-12-31')
    AND county IS NOT NULL
    AND volume_sold_liters IS NOT NULL
    GROUP BY county, state_bottle_retail,years
    ORDER BY sold_items DESC
),

state_19 as (select sum(sold_items) as total_sold_19, SUM(s_liters) as total_liters_19 from Liquor_sold
group by years
order by years)

select * from state_19






