CREATE TABLE Market_data (
ID BIGINT PRIMARY KEY,
Year_Birth INT,
Average_Age INT,
Education CHAR(50),
Marital_Status CHAR(50),
Income numeric(20),
Kidhome numeric(5),
Teenhome numeric(5),
Dt_Customer date,
Recency numeric(5),
AmtLiq numeric(20),
AmtVege numeric(20),
AmtNonVeg numeric(20),
AmtPes numeric(20),
AmtChocolates numeric(20),
AmtComm numeric(20),
NumDeals numeric(20),
NumWebBuy numeric(20),
NumWalkinPur numeric(20),
NumVisits numeric(20),
Frequency_buy numeric(20),
Response numeric(20),
Complain numeric(20),
Country Varchar(50),
Count_success numeric(20),
Total_Spending numeric(50),
R_Scores numeric(5),
F_scores numeric(5),
M_Scores numeric(5),
RFM_Code numeric(5),
RFM_score numeric(5),
RFM_segment CHAR(100)
);

select * from public.market_data;

CREATE TABLE Ad_data (
ID BIGINT ,
Bulkmail_ad numeric(5),
Twitter_ad numeric(5),
Instagram_ad numeric(5),
Facebook_ad numeric(5),
Brochure_ad numeric(5),
Total_Response numeric(5)
);

Select * from public.ad_data;


SELECT COUNT(DISTINCT id ) as num_customers
FROM public.market_data ;

------The total spend per country----
SELECT country,sum( total_spending) as Total_spend
FROM public.market_data
Group by country
order by Total_spend desc;

-----The total spend per product per country------

SELECT country, Sum(amtliq) as  Alcoholic_Beverages, sum(amtvege) as vegetables, 
sum(amtnonveg) as Meat_Items, sum(amtpes) as Fish_Products, sum(amtchocolates) as chocolates,
sum(amtcomm) as Commodities, sum(total_spending) as Total_spend, Count(id) as Num_customers
FROM public.market_data
Group by country
order by Total_spend desc;

-----Unpivot data for more flexible analysis----
-----This transforms columns into rows for easier analysis for country-----

WITH Product_Spend AS (
SELECT  country,product_name,
    SUM(spending_amount) as total_spending,
    ROUND(AVG(spending_amount),2) as avg_spending,
    COUNT(DISTINCT id) as num_consumers,
    MIN(spending_amount) as min_spending,
    MAX(spending_amount) as max_spending
	FROM (
    SELECT id, country, 'alcoholic_beverages' as product_name, amtliq as spending_amount
    FROM market_data
    UNION ALL
    SELECT id, country, 'vegetables', amtvege
    FROM market_data
    UNION ALL
    SELECT id, country, 'meat_items', amtnonveg
    FROM market_data
	UNION ALL
	SELECT id, country, 'fish_products', amtpes
    FROM market_data
	UNION ALL
    SELECT id, country, 'chocolates', amtchocolates
    FROM market_data
	UNION ALL
    SELECT id, country, 'commodities', amtcomm
    FROM market_data
)unpivoted
WHERE spending_amount > 0 
GROUP BY country, product_name
)         ---------Now using the temporary "product_spending" result to add rankings-----
SELECT 
    country,
    product_name,
    total_spending,
    avg_spending,
    num_consumers,
	RANK() OVER (PARTITION BY country ORDER BY total_spending DESC) as product_rank_in_country,
ROUND( 100.0 * total_spending / SUM(total_spending) OVER (PARTITION BY country), 
        2
    ) as pct_of_country_spending
FROM Product_Spend
ORDER BY total_spending DESC;

------Which products are popular among each marital status?-----
SELECT 
    marital_status,
    COUNT(CASE WHEN amtliq > 0 THEN 1 END) as alcoholic_beverages_buyers,
    SUM(amtliq) as total_alcoholic_beverages,
 COUNT(CASE WHEN amtvege > 0 THEN 1 END) as vegetables_buyers,
    SUM(amtvege) as total_vegetables,
COUNT(CASE WHEN amtnonveg > 0 THEN 1 END) as meat_items_buyers,
    SUM(amtnonveg) as total_meat_items,
COUNT(CASE WHEN amtpes > 0 THEN 1 END) as fish_products_buyers,
    SUM(amtpes) as total_fish_products,
 COUNT(CASE WHEN amtchocolates > 0 THEN 1 END) as chocolates_buyers,
    SUM(amtchocolates) as total_chocolates,
COUNT(CASE WHEN amtcomm > 0 THEN 1 END) as commodities_buyers,
    SUM(amtcomm) as total_commodities,
 COUNT(DISTINCT id) as total_consumers
    FROM public.market_data
GROUP BY marital_status
ORDER BY marital_status;

------Unpivot data for more flexible analysis----
-----This transforms columns into rows for easier analysis for marital status------

WITH Product_MS_spend AS (
SELECT  marital_status, product_name,
    SUM(spending_amount) as total_spending,
    ROUND(AVG(spending_amount),2) as avg_spending,
    COUNT(DISTINCT id) as num_consumers,
    MIN(spending_amount) as min_spending,
    MAX(spending_amount) as max_spending
	FROM (
    SELECT id, marital_status, 'alcoholic_beverages' as product_name, amtliq as spending_amount
    FROM market_data
    UNION ALL
    SELECT id, marital_status, 'vegetables', amtvege
    FROM market_data
    UNION ALL
    SELECT id, marital_status, 'meat_items', amtnonveg
    FROM market_data
	UNION ALL
	SELECT id, marital_status, 'fish_products', amtpes
    FROM market_data
	UNION ALL
    SELECT id, marital_status, 'chocolates', amtchocolates
    FROM market_data
	UNION ALL
    SELECT id, marital_status, 'commodities', amtcomm
    FROM market_data
)unpivoted
WHERE spending_amount > 0 
GROUP BY marital_status, product_name
)         ---------Now using the temporary "product_MS_spend" result to add rankings-----
SELECT 
    marital_status,
    product_name,
    total_spending,
    avg_spending,
    num_consumers,
	RANK() OVER (PARTITION BY marital_status ORDER BY total_spending DESC) as product_rank_in_marital_st,
ROUND( 100.0 * total_spending / SUM(total_spending) OVER (PARTITION BY marital_status), 
        2
    ) as pct_of_MS_spending
FROM product_MS_Spend
ORDER BY  total_spending DESC;

--------which products are the most popular based on whether-----
 --------or not there are children or teens in the home.--------


WITH household_segments AS (
    SELECT 
        ID, AmtLiq,
        AmtVege,
        AmtNonVeg,
        AmtPes,
        AmtChocolates,
		amtcomm,
         CASE  -- Create household type based on presence of kids and teens
            WHEN Kidhome > 0 AND Teenhome > 0 THEN 'Children_And_Teens'
            WHEN Kidhome > 0 AND Teenhome = 0 THEN 'Children_Only'
            WHEN Kidhome = 0 AND Teenhome > 0 THEN 'Teens_Only'
            WHEN Kidhome = 0 AND Teenhome = 0 THEN 'No_Children'
            ELSE 'Unknown'
        END as household_type,
       (AmtLiq + AmtVege + AmtNonVeg + AmtPes + AmtChocolates + amtcomm) as total_spending
        FROM public.market_data 
)
SELECT ----Calculate SUM for each product by household segment
    household_type,
    COUNT(DISTINCT ID) as num_customers,
    SUM(AmtLiq) as total_AmtLiq,
    SUM(AmtVege) as total_AmtVege,
    SUM(AmtNonVeg) as total_AmtNonVeg,
    SUM(AmtPes) as total_AmtPes,
    SUM(AmtChocolates) as total_AmtChocolates,
	 SUM(AmtComm) as total_AmtCommodities,
    SUM(total_spending) as total_all_products,
    ROUND(AVG(total_spending),2) as avg_total_spending,
ROUND(100.0 * SUM(total_spending) / SUM(SUM(total_spending)) OVER (), 2) as pct_of_total_revenue
FROM household_segments
GROUP BY household_type
ORDER BY total_all_products DESC;


----------COMPREHENSIVE AGE & DEMOGRAPHICS ANALYSIS------------------
-----------Which products are popular among each Age group?-----------

WITH age_segments AS (
    SELECT 
        ID,
        average_age,
        income,AmtLiq,
        AmtVege,
        AmtNonVeg,
        AmtPes,
        AmtChocolates,
		amtcomm,
         CASE 
            WHEN average_age BETWEEN 18 AND 24 THEN '18_24_GROUP'
            WHEN average_age BETWEEN 25 AND 34 THEN '25_34_GROUP'
            WHEN average_age BETWEEN 35 AND 44 THEN '35_44_GROUP'
            WHEN average_age BETWEEN 45 AND 54 THEN '45_54_GROUP'
            WHEN average_age BETWEEN 55 AND 64 THEN '55_64_GROUP'
            WHEN average_age >= 65 THEN '65_PLUS'
           END as age_bucket,
     (amtliq + amtvege + amtnonveg + amtpes + amtchocolates + amtcomm) as total_spending
      FROM public.market_data)
SELECT 
    age_bucket,
    COUNT(DISTINCT ID) as num_customers,
    round(AVG(income),2) as avg_income,
    SUM(AmtLiq) as total_AmtLiq,
    SUM(amtvege) as total_AmtVege,
    SUM(amtnonveg) as total_AmtNonVeg,
    SUM(amtpes) as total_AmtPes,
    SUM(amtchocolates) as total_AmtChocolates,
	SUM(amtcomm) as total_amtcommodities,
    SUM(total_spending) as total_all_spending,
    round(AVG(total_spending),2) as avg_total_spending,
    ROUND(100.0 * SUM(total_spending) / SUM(SUM(total_spending)) OVER (), 2) as pct_of_total_revenue
FROM age_segments
GROUP BY age_bucket
ORDER BY age_bucket;


--------Analysing which products are popular on the basis of education-----

WITH Product_Ed_spend AS (
SELECT  education, product_name,
    SUM(spending_amount) as total_spending,
    ROUND(AVG(spending_amount),2) as avg_spending,
    COUNT(DISTINCT id) as num_consumers,
    MIN(spending_amount) as min_spending,
    MAX(spending_amount) as max_spending
	FROM (
    SELECT id, education, 'alcoholic_beverages' as product_name, amtliq as spending_amount
    FROM market_data
    UNION ALL
    SELECT id, education, 'vegetables', amtvege
    FROM market_data
    UNION ALL
    SELECT id, education, 'meat_items', amtnonveg
    FROM market_data
	UNION ALL
	SELECT id, education, 'fish_products', amtpes
    FROM market_data
	UNION ALL
    SELECT id, education, 'chocolates', amtchocolates
    FROM market_data
	UNION ALL
    SELECT id, education, 'commodities', amtcomm
    FROM market_data
)unpivoted
WHERE spending_amount > 0 
GROUP BY education, product_name
)         ---------Now using the temporary "product_Ed_spend" result to add rankings-----
SELECT 
    education,
    product_name,
    total_spending,
    avg_spending,
    num_consumers,
	RANK() OVER (PARTITION BY education ORDER BY total_spending DESC) as product_rank_in_education,
ROUND( 100.0 * total_spending / SUM(total_spending) OVER (PARTITION BY education), 
        2
    ) as pct_of_edu_spending
FROM product_Ed_Spend
ORDER BY  total_spending DESC;


------Analysing which products are popular on the basis of Income------

WITH inc_segments AS (
    SELECT 
        ID,
        average_age,
        income,AmtLiq,
        AmtVege,
        AmtNonVeg,
        AmtPes,
        AmtChocolates,
		amtcomm,
         CASE 
         WHEN Income < 10000 THEN 'very_Low_<10K'
            WHEN Income BETWEEN 10000 AND 24999 THEN 'Low_10-25K'
			WHEN Income BETWEEN 25000 AND 49999 THEN 'Low_Middle_25-50K'
			WHEN Income BETWEEN 50000 AND 74999 THEN 'Middle_50-75K'
			WHEN Income BETWEEN 75000 AND 99999 THEN 'Upper_Middle_75K-100K'
            WHEN Income >= 100000 THEN 'High_100K'
        END as income_group,   
     (amtliq + amtvege + amtnonveg + amtpes + amtchocolates + amtcomm) as total_spending
      FROM public.market_data)
SELECT 
    income_group,
    COUNT(DISTINCT ID) as num_customers,
    round(AVG(income),2) as avg_income,
    SUM(AmtLiq) as total_AmtLiq,
    SUM(amtvege) as total_AmtVege,
    SUM(amtnonveg) as total_AmtNonVeg,
    SUM(amtpes) as total_AmtPes,
    SUM(amtchocolates) as total_AmtChocolates,
	SUM(amtcomm) as total_amtcommodities,
    SUM(total_spending) as total_all_spending,
   ROUND( AVG(total_spending),2) as avg_total_spending,
    ROUND(100.0 * SUM(total_spending) / SUM(SUM(total_spending)) OVER (), 2) as pct_of_total_revenue
FROM inc_segments
GROUP BY income_group
ORDER BY income_group;

--------Analysing Income with Age-------


WITH age_income_segments AS (
    SELECT 
        ID,
        Average_Age,
        Income,
        AmtLiq,
        AmtVege,
        AmtNonVeg,
        AmtPes,
        AmtChocolates,
		Amtcomm,
         CASE 
		     WHEN average_age BETWEEN 18 AND 24 THEN '18_24_GROUP'
            WHEN average_age BETWEEN 25 AND 34 THEN '25_34_GROUP'
            WHEN average_age BETWEEN 35 AND 44 THEN '35_44_GROUP'
            WHEN average_age BETWEEN 45 AND 54 THEN '45_54_GROUP'
            WHEN average_age BETWEEN 55 AND 64 THEN '55_64_GROUP'
            WHEN average_age >= 65 THEN '65_PLUS'
            END as age_group,
        CASE 
            WHEN Income < 10000 THEN 'very_Low_<10K'
            WHEN Income BETWEEN 10000 AND 24999 THEN 'Low_10-25K'
			WHEN Income BETWEEN 25000 AND 49999 THEN 'Low_Middle_25-50K'
			WHEN Income BETWEEN 50000 AND 74999 THEN 'Middle_50-75K'
			WHEN Income BETWEEN 75000 AND 99999 THEN 'Upper_Middle_75K-100K'
            WHEN Income >= 100000 THEN 'High_100K'
        END as income_group,
        (AmtLiq + AmtVege + AmtNonVeg + AmtPes + AmtChocolates + Amtcomm) as total_spending
         FROM public.market_data
)
SELECT 
    age_group,
    income_group,
    COUNT(DISTINCT ID) as num_customers,
    SUM(AmtLiq) as total_AmtLiq,
    SUM(AmtVege) as total_AmtVege,
    SUM(AmtNonVeg) as total_AmtNonVeg,
    SUM(AmtPes) as total_AmtPes,
    SUM(AmtChocolates) as total_AmtChocolates,
	SUM(amtcomm) as total_amtcommodities,
	SUM(total_spending) as total_all_spending,
    ROUND(AVG(total_spending),2) as avg_total_spending,
    ROUND(100.0 * COUNT(DISTINCT ID) / SUM(COUNT(DISTINCT ID)) OVER (), 2) as pct_of_customer_base,
    ROUND(100.0 * SUM(total_spending) / SUM(SUM(total_spending)) OVER (), 2) as pct_of_spending
	FROM age_income_segments
GROUP BY age_group, income_group
ORDER BY age_group, income_group;

--------- Analysing  AGE  with MARITAL STATUS  and  EDUCATION ----------

WITH demographic_profile AS (
    SELECT 
        ID,
        CASE 
          WHEN average_age BETWEEN 18 AND 24 THEN '18_24_GROUP'
            WHEN average_age BETWEEN 25 AND 34 THEN '25_34_GROUP'
            WHEN average_age BETWEEN 35 AND 44 THEN '35_44_GROUP'
            WHEN average_age BETWEEN 45 AND 54 THEN '45_54_GROUP'
            WHEN average_age BETWEEN 55 AND 64 THEN '55_64_GROUP'
            WHEN average_age >= 65 THEN '65_PLUS'   
        END as age_group,
        CASE 
            WHEN Marital_Status IN ('Married', 'Together') THEN 'Partnered'
			 WHEN Marital_Status IN ('Divorced', 'single', 'Widow') THEN 'Single'
            ELSE 'Others'
        END as marital_status,
        CASE 
            WHEN Education IN ('PhD', 'Master') THEN 'Masters'
            WHEN Education = 'Graduation' THEN 'Graduate'
            ELSE 'Basic_education'
        END as education,
        Income,
        AmtLiq,
        AmtVege,
        AmtNonVeg,
        AmtPes,
        AmtChocolates,
		Amtcomm,
        (AmtLiq + AmtVege + AmtNonVeg + AmtPes + AmtChocolates + Amtcomm) as total_spending
         FROM public.market_data
)
SELECT 
    age_group,
    marital_status,
    education, income,
    COUNT(DISTINCT ID) as num_customers,
   sum(AmtLiq) as total_AmtLiq,
    sum(AmtVege) as total_AmtVege,
    sum(AmtNonVeg) as total_AmtNonVeg,
    sum(AmtPes) as total_AmtPes,
    sum(AmtChocolates) as total_AmtChocolates,
    ROUND(avg(total_spending),2) as avg_total_spending,
    SUM(total_spending) as segment_revenue,
    ROUND(100.0 * SUM(total_spending) / SUM(SUM(total_spending)) OVER (), 2) as pct_of_revenue
FROM demographic_profile
GROUP BY age_group, marital_status, education, income
ORDER BY segment_revenue DESC;


-------Which social media platform is the most effective method of advertising 
-------in each country? -------

------using join------

SELECT
    count(m.id) as num_customers,
    m.country,
    sum(a.twitter_ad) as res_twitter,
   sum(a.instagram_ad) as res_insta,
    sum(a.facebook_ad) as res_FB,
	sum(a.brochure_ad) as res_brochure,
	sum(a.bulkmail_ad) as res_bulkmail,
	sum(a.twitter_ad + a.instagram_ad + a.facebook_ad + a.brochure_ad + a.bulkmail_ad ) as total_response,
ROUND(SUM(a.twitter_ad + a.instagram_ad + a.facebook_ad + a.brochure_ad + a.bulkmail_ad) * 100.0 / 
        NULLIF(COUNT(m.id), 0), 2) AS response_rate_percentage
FROM market_data m
JOIN ad_data a
    ON m.id = a.id 
	group by  m.country
	order by m.country;

-----analysis using UNION ALL-------

WITH social_media_conversions AS (
    SELECT
        m.id,
        m.country,
        'Twitter' AS platform,
        a.twitter_ad AS conversions
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
 UNION ALL
    SELECT
        m.id,
        m.country,
       'Instagram',
        a.instagram_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
  UNION ALL
    SELECT
        m.id,
        m.country,
       'Facebook',
        a.facebook_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	UNION ALL
    SELECT
        m.id,
        m.country,
       'Bulkmail',
        a.bulkmail_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	UNION ALL
    SELECT
        m.id,
        m.country,
       'Brochure',
        a.Brochure_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
),
country_platform_rank AS (
    SELECT
        country,
        platform,
        SUM(conversions) AS total_conversions,
        RANK() OVER (PARTITION BY country ORDER BY SUM(conversions) DESC ) AS rnk
    FROM social_media_conversions
    GROUP BY country, platform
	ORDER BY total_conversions desc
	)
SELECT
    country,
    platform,
    total_conversions
FROM country_platform_rank;

------Which social media platform is MOST EFFECTIVE by MARITAL STATUS?------

WITH social_media_conversions AS (
    SELECT
        m.id,
        m.marital_status,
        'Twitter' AS platform,
        a.twitter_ad AS conversions
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
 UNION ALL
    SELECT
        m.id,
        m.marital_status,
       'Instagram',
        a.instagram_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
  UNION ALL
    SELECT
        m.id,
        m.marital_status,
       'Facebook',
        a.facebook_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	UNION ALL
    SELECT
        m.id,
        m.marital_status,
       'Bulkmail',
        a.bulkmail_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	UNION ALL
    SELECT
        m.id,
        m.marital_status,
       'Brochure',
        a.Brochure_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
),
marital_platform_rank AS (
    SELECT
        marital_status,
        platform,
        SUM(conversions) AS total_conversions,
        RANK() OVER (
            PARTITION BY marital_status
            ORDER BY SUM(conversions) DESC
        ) AS rnk
    FROM social_media_conversions
    GROUP BY marital_status, platform
	ORDER BY total_conversions desc
)
SELECT
    marital_status,
    platform,
    total_conversions
FROM marital_platform_rank;


--------Which platform(s) seem most effective per country (purchase-influenced)?-----

WITH social_media_conversions AS (
    SELECT distinct
        m.id,
        m.country,
        'Twitter' AS platform,
        a.twitter_ad AS conversions
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	 WHERE a.twitter_ad = 1
 UNION ALL
    SELECT distinct
        m.id,
        m.country,
       'Instagram',
        a.instagram_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	 WHERE a.instagram_ad = 1
  UNION ALL
    SELECT distinct
       m.id,
        m.country,
       'Facebook',
        a.facebook_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	 WHERE a.facebook_ad = 1
	UNION ALL
    SELECT distinct
        m.id,
        m.country,
       'Bulkmail',
        a.bulkmail_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	WHERE a.bulkmail_ad = 1
	UNION ALL
    SELECT distinct
        m.id,
        m.country,
       'Brochure',
        a.Brochure_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	 WHERE a.brochure_ad = 1
)
SELECT
    count(distinct m.id) as total_num_customers,
	s.country,
    s.platform,
    SUM(s.conversions * m.total_spending) AS influenced_spend,
	 ROUND(
        SUM(s.conversions * m.total_spending) * 1.0 / NULLIF(COUNT(DISTINCT m.id), 0),
        2
    ) AS avg_influenced_spend_per_customer
FROM social_media_conversions s
JOIN market_data m
    ON s.id = m.id
GROUP BY m.country, s.platform,s.country
ORDER BY  avg_influenced_spend_per_customer DESC;


-------Ranking the most influenced social media media platform--------


WITH social_media_conversions AS (
    SELECT
        m.id,
        m.country,
        'Twitter' AS platform,
        a.twitter_ad AS conversions
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
 UNION ALL
    SELECT
        m.id,
        m.country,
       'Instagram',
        a.instagram_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
  UNION ALL
    SELECT
        m.id,
        m.country,
       'Facebook',
        a.facebook_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	UNION ALL
    SELECT
        m.id,
        m.country,
       'Bulkmail',
        a.bulkmail_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
	UNION ALL
    SELECT
        m.id,
        m.country,
       'Brochure',
        a.Brochure_ad
    FROM market_data m
    JOIN ad_data a ON m.id = a.id
),
platform_spend_rank AS (
    SELECT
        s.country,
        s.platform,
        SUM(s.conversions * m.total_spending) AS influenced_spend,
        RANK() OVER (
            PARTITION BY s.country
            ORDER BY SUM(s.conversions * m.total_spending) DESC
        ) AS rnk
    FROM social_media_conversions s
    JOIN market_data m ON s.id = m.id
    GROUP BY s.country, s.platform
	ORDER BY influenced_spend desc
)
SELECT
    country,
    platform,
    influenced_spend
FROM platform_spend_rank
WHERE rnk = 1;

---------What is the average spend on each of the six product categories,---------
----------segmented by social media platform assuming spend is influenced by campaign conversions?------

WITH Product_Spend AS (
SELECT id, 'alcoholic_beverages' as product_name, amtliq as spending_amount
    FROM market_data
    UNION ALL
    SELECT id,'vegetables', amtvege
    FROM market_data
    UNION ALL
    SELECT id, 'meat_items', amtnonveg
    FROM market_data
	UNION ALL
	SELECT id, 'fish_products', amtpes
    FROM market_data
	UNION ALL
    SELECT id, 'chocolates', amtchocolates
    FROM market_data
	UNION ALL
    SELECT id,'commodities', amtcomm
    FROM market_data
)
, social_media_conversions AS (
    SELECT distinct
        a.id,
      'Twitter' AS platform
      FROM ad_data a
  WHERE a.twitter_ad = 1
 UNION ALL
    SELECT distinct
        a.id,
       'Instagram'
    FROM ad_data a
   WHERE a.instagram_ad = 1
  UNION ALL
    SELECT distinct
       a.id,
       'Facebook'
       FROM ad_data a
    WHERE a.facebook_ad = 1
	UNION ALL
    SELECT distinct
        a.id,
        'Bulkmail'
        FROM ad_data a
   WHERE a.bulkmail_ad = 1
	UNION ALL
    SELECT distinct
        a.id,
      'Brochure'
        FROM ad_data a
 WHERE a.brochure_ad = 1
)
SELECT
    pc.platform,
    ps.Product_name,
    round(AVG(ps.spending_amount),2) AS avg_spend_per_product
FROM Product_Spend ps
JOIN social_media_conversions pc
    ON ps.id = pc.id
GROUP BY
    pc.platform,
    ps.product_name
ORDER BY
    avg_spend_per_product DESC;

-------RFM analysis combined with demographics and campaign response----
----------------(Country and Marital status)--------------

WITH base_data AS (
    SELECT
        m.id,
        m.country,
        m.marital_status,
        m.education,
		m.income,
        m.kidhome,
        m.teenhome, m.Recency AS Recency,
        m.Frequency_buy AS Frequency,
        m.R_scores,
        m.F_scores,
        m.M_scores,
        m.RFM_score,
        m.total_spending AS Monetary,
        a.twitter_ad,
        a.instagram_ad,
        a.facebook_ad,
        a.bulkmail_ad,
        a.brochure_ad
    FROM market_data m
    LEFT JOIN ad_data a
        ON m.id = a.id
)
SELECT
    country,
    marital_status,
	round(AVG(RFM_score)) AS avg_RFM_score,
    round(AVG(recency),2) AS avg_recency,
    round(AVG(frequency),2 )AS avg_frequency,
    round(AVG(monetary),2) AS avg_monetary,
    COUNT(*) AS customers
FROM base_data
GROUP BY
    country,
    marital_status
ORDER BY
    avg_RFM_score desc;

-------RFM analysis combined with demographics and campaign response----
----------------(Country and Age)--------------

WITH base_data AS (
    SELECT
        m.id,
        m.country,
        m.Average_age,
        m.education,
		m.income,
        m.kidhome,
        m.teenhome, m.Recency AS Recency,
        m.Frequency_buy AS Frequency,
        m.R_scores,
        m.F_scores,
        m.M_scores,
        m.RFM_score,
        m.total_spending AS Monetary,
        a.twitter_ad,
        a.instagram_ad,
        a.facebook_ad,
        a.bulkmail_ad,
        a.brochure_ad
    FROM market_data m
    LEFT JOIN ad_data a
        ON m.id = a.id
)
SELECT
    country,
    Average_age,
	round(AVG(RFM_score)) AS avg_RFM_score,
    round(AVG(recency),2) AS avg_recency,
    round(AVG(frequency),2 )AS avg_frequency,
    round(AVG(monetary),2) AS avg_monetary,
    COUNT(*) AS customers
FROM base_data
GROUP BY
    country,
    average_age
ORDER BY
    avg_RFM_score desc;
	
--------RFM SCORES FOR CUSTOMERS WHO RESPONDED TO ADVERTISING----------
WITH base_data AS (
    SELECT
        m.id,
        m.country,
        m.marital_status,
        m.education,
        m.kidhome,
        m.teenhome,
		m.R_scores,
        m.F_scores,
        m.M_scores,
        m.RFM_score,
 m.total_spending,
a.twitter_ad,
        a.instagram_ad,
        a.facebook_ad,
        a.bulkmail_ad,
        a.brochure_ad
    FROM market_data m
    LEFT JOIN ad_data a
        ON m.id = a.id
),
campaign_responders AS (
    SELECT id, RFM_score, 'Twitter' AS campaign , Twitter_ad as responded
    FROM base_data
	where twitter_ad = 1
   UNION ALL
    SELECT id, RFM_score, 'Instagram', Instagram_ad
    FROM base_data
	where instagram_ad = 1
 UNION ALL
    SELECT id, RFM_score, 'Facebook',Facebook_ad
    FROM base_data
	where facebook_ad = 1
    UNION ALL
    SELECT id, RFM_score, 'Bulkmail',Bulkmail_ad
    FROM base_data
	where bulkmail_ad = 1
    UNION ALL
    SELECT id, RFM_score, 'Brochure', Brochure_ad
    FROM base_data
	where brochure_ad = 1
  )
SELECT
     campaign, responded,
    COUNT(*) AS customers
FROM campaign_responders
GROUP BY campaign, responded;

------------ RFM scores for campaign responders and non responders ---------


WITH base_data AS (
    SELECT
        m.id,
        m.country,
        m.Average_age,
        m.education,
		m.income,
        m.kidhome,
        m.teenhome, m.Recency AS Recency,
        m.Frequency_buy AS Frequency,
        m.R_scores,
        m.F_scores,
        m.M_scores,
        m.RFM_score,
        m.total_spending AS Monetary,
        a.twitter_ad,
        a.instagram_ad,
        a.facebook_ad,
        a.bulkmail_ad,
        a.brochure_ad
    FROM market_data m
    LEFT JOIN ad_data a
        ON m.id = a.id
),
campaign_responders AS (
    SELECT id, RFM_score, 'Twitter' AS campaign , Twitter_ad as responded
    FROM base_data
   UNION ALL
    SELECT id, RFM_score, 'Instagram', Instagram_ad
    FROM base_data
 UNION ALL
    SELECT id, RFM_score, 'Facebook',Facebook_ad
    FROM base_data
    UNION ALL
    SELECT id, RFM_score, 'Bulkmail',Bulkmail_ad
    FROM base_data
    UNION ALL
    SELECT id, RFM_score, 'Brochure', Brochure_ad
    FROM base_data
  )
SELECT
    campaign,responded,
    RFM_score,
    COUNT(DISTINCT id) AS customers
FROM campaign_responders
GROUP BY campaign,RFM_score,responded
ORDER BY  campaign,RFM_score DESC;

-------------- ad response and total RFM score vary by age--------

WITH base_data AS (
    SELECT
        m.id,
        m.country,
        m.average_age,
        m.marital_status,
        m.income,
        m.RFM_score,
        COALESCE(a.Twitter_ad, 0)   AS twitter_ad,
        COALESCE(a.Instagram_ad, 0) AS instagram_ad,
        COALESCE(a.Facebook_ad, 0)  AS facebook_ad,
        COALESCE(a.Bulkmail_ad, 0)  AS bulkmail_ad,
        COALESCE(a.Brochure_ad, 0)  AS brochure_ad
    FROM market_data m
    LEFT JOIN ad_data a
        ON m.id = a.id
),
campaign_long AS (
    SELECT id, average_age, marital_status, income, country, RFM_score,
           'Twitter' AS platform, twitter_ad AS responded
    FROM base_data
UNION ALL
    SELECT id, average_age, marital_status, income, country,RFM_score,
           'Instagram', instagram_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income,country, RFM_score,
           'Facebook', facebook_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income,country, RFM_score,
           'Bulkmail', bulkmail_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income,country, RFM_score,
           'Brochure', brochure_ad
    FROM base_data
)
SELECT
    platform,
    CASE
        WHEN average_age BETWEEN 18 AND 24 THEN '18–24'
        WHEN average_age BETWEEN 25 AND 34 THEN '25–34'
        WHEN average_age BETWEEN 35 AND 44 THEN '35–44'
        WHEN average_age BETWEEN 45 AND 54 THEN '45–54'
        WHEN average_age BETWEEN 55 AND 64 THEN '55–64'
        ELSE '65+'
    END AS age_group, country,
    responded,
    COUNT(DISTINCT id) AS customers,
    ROUND(AVG(RFM_score), 2) AS avg_rfm_score
FROM campaign_long
GROUP BY platform, age_group, responded, country
ORDER BY platform, age_group, responded;

-----------ad response and total RFM score vary by Marital status---------

WITH base_data AS (
    SELECT
        m.id,
        m.country,
        m.average_age,
        m.marital_status,
        m.income,
        m.RFM_score,
        COALESCE(a.Twitter_ad, 0)   AS twitter_ad,
        COALESCE(a.Instagram_ad, 0) AS instagram_ad,
        COALESCE(a.Facebook_ad, 0)  AS facebook_ad,
        COALESCE(a.Bulkmail_ad, 0)  AS bulkmail_ad,
        COALESCE(a.Brochure_ad, 0)  AS brochure_ad
    FROM market_data m
    LEFT JOIN ad_data a
        ON m.id = a.id
),
campaign_long AS (
    SELECT id, average_age, marital_status, income, country, RFM_score,
           'Twitter' AS platform, twitter_ad AS responded
    FROM base_data
UNION ALL
    SELECT id, average_age, marital_status, income, country,RFM_score,
           'Instagram', instagram_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income, country, RFM_score,
           'Facebook', facebook_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income, country, RFM_score,
           'Bulkmail', bulkmail_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income,country, RFM_score,
           'Brochure', brochure_ad
    FROM base_data
)
SELECT
    platform, country,
   marital_status,
    responded,
    COUNT(DISTINCT id) AS customers,
    ROUND(AVG(RFM_score), 2) AS avg_rfm_score
FROM campaign_long
GROUP BY platform, marital_status, responded, country
ORDER BY customers, avg_rfm_score desc;

-----------ad response and total RFM score vary by Income---------

WITH base_data AS (
    SELECT
        m.id,
        m.country,
        m.average_age,
        m.marital_status,
        m.income,
        m.RFM_score,
        COALESCE(a.Twitter_ad, 0)   AS twitter_ad,
        COALESCE(a.Instagram_ad, 0) AS instagram_ad,
        COALESCE(a.Facebook_ad, 0)  AS facebook_ad,
        COALESCE(a.Bulkmail_ad, 0)  AS bulkmail_ad,
        COALESCE(a.Brochure_ad, 0)  AS brochure_ad
    FROM market_data m
    LEFT JOIN ad_data a
        ON m.id = a.id
),
campaign_long AS (
    SELECT id, average_age, marital_status, income, country, RFM_score,
           'Twitter' AS platform, twitter_ad AS responded
    FROM base_data
UNION ALL
    SELECT id, average_age, marital_status, income,country, RFM_score,
           'Instagram', instagram_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income,country, RFM_score,
           'Facebook', facebook_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income, country, RFM_score,
           'Bulkmail', bulkmail_ad
    FROM base_data
 UNION ALL
    SELECT id, average_age, marital_status, income,country, RFM_score,
           'Brochure', brochure_ad
    FROM base_data
)
SELECT
    platform, country,
	CASE 
            WHEN Income < 10000 THEN 'very_Low_<10K'
            WHEN Income BETWEEN 10000 AND 24999 THEN 'Low_10-25K'
			WHEN Income BETWEEN 25000 AND 49999 THEN 'Low_Middle_25-50K'
			WHEN Income BETWEEN 50000 AND 74999 THEN 'Middle_50-75K'
			WHEN Income BETWEEN 75000 AND 99999 THEN 'Upper_Middle_75K-100K'
            WHEN Income >= 100000 THEN 'High_100K'
        END as income_group,
   responded,
    COUNT(DISTINCT id) AS customers,
    ROUND(AVG(RFM_score), 2) AS avg_rfm_score
FROM campaign_long
GROUP BY platform, income_group, responded, country
ORDER BY  avg_rfm_score desc
;


