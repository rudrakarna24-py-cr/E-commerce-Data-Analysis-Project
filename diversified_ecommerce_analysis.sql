-- ================================================================
-- Diversified E-Commerce Product Analysis — SQL Project
-- Course  : Data Science - I  |  Internal Component II
-- Dataset : diversified_ecommerce_dataset.csv
-- Columns : product_id, product_name, category, price, discount,
--           tax_rate, stock_level, age_group, location, gender,
--           shipping_cost, shipping_method, return_rate,
--           seasonality, popularity_index, final_price
-- Queries : SELECT, WHERE, GROUP BY, Window Functions, CTE
-- DB      : PostgreSQL
-- ================================================================


-- ----------------------------------------------------------------
-- 1. Create the structured table
-- ----------------------------------------------------------------
CREATE TABLE product_sales (
    product_id       VARCHAR(10),
    product_name     VARCHAR(50),
    category         VARCHAR(30),
    price            NUMERIC(10,2),
    discount         INT,
    tax_rate         INT,
    stock_level      INT,
    age_group        VARCHAR(10),
    location         VARCHAR(40),
    gender           VARCHAR(15),
    shipping_cost    NUMERIC(8,2),
    shipping_method  VARCHAR(15),
    return_rate      NUMERIC(5,2),
    seasonality      VARCHAR(5),
    popularity_index INT,
    final_price      NUMERIC(10,2)
);


-- ----------------------------------------------------------------
-- 2. Insert 40 real rows from diversified_ecommerce_dataset.csv
--    (8 rows per category — evenly distributed)
-- ----------------------------------------------------------------
INSERT INTO product_sales
    (product_id, product_name, category, price, discount, tax_rate,
     stock_level, age_group, location, gender, shipping_cost,
     shipping_method, return_rate, seasonality, popularity_index, final_price)
VALUES
('P1074',  'Laptop',          'Electronics',     269.73,  10, 5,  334, '25-34', 'Singapore',               'Female',     40.00, 'Standard',  13.44, 'Yes', 47,  242.76),
('P1103',  'Skirt',           'Apparel',         875.79,  25, 12, 486, '45-54', 'Phoenix, USA',             'Male',       22.62, 'Overnight', 13.63, 'No',  91,  656.84),
('P1148',  'Shirt',           'Apparel',          91.37,  25, 15,  25, '45-54', 'Phoenix, USA',             'Female',      5.84, 'Express',   10.30, 'No',  73,   68.53),
('P1151',  'Laptop',          'Electronics',    1671.84,  10, 15, 303, '18-24', 'Tokyo, Japan',             'Non-Binary', 29.09, 'Overnight',  4.91, 'Yes', 68, 1504.66),
('P1360',  'Microwave',       'Home Appliances', 444.08,  15, 10, 142, '25-34', 'Los Angeles, USA',         'Male',       15.54, 'Express',    7.49, 'Yes', 32,  377.47),
('P1671',  'Sandals',         'Footwear',       1544.48,  10, 10, 396, '35-44', 'Toronto, Canada',          'Male',        2.05, 'Overnight',  1.43, 'Yes', 31, 1390.03),
('P2211',  'Dress',           'Apparel',        1657.37,   5, 10, 306, '25-34', 'New York, USA',            'Non-Binary', 36.79, 'Overnight', 10.40, 'No',  55, 1574.50),
('P2271',  'Laptop',          'Electronics',     695.16,  10, 10, 253, '25-34', 'Dubai, UAE',               'Female',     19.47, 'Overnight',  9.55, 'No',  31,  625.64),
('P2604',  'Sandals',         'Footwear',       1325.06,   0,  5, 172, '45-54', 'Cape Town, South Africa',  'Non-Binary', 16.98, 'Overnight', 18.44, 'No',  69, 1325.06),
('P2833',  'Microwave',       'Home Appliances', 959.29,   0, 15, 400, '25-34', 'Berlin, Germany',          'Female',     27.65, 'Overnight',  8.68, 'No',  53,  959.29),
('P3021',  'Hiking Shoes',    'Footwear',       1194.49,   0, 10,  31, '55+',   'Singapore',               'Female',     48.95, 'Standard',   6.59, 'No',  95, 1194.49),
('P3443',  'Speaker',         'Electronics',     795.39,  20, 15,  98, '18-24', 'Singapore',               'Non-Binary',  7.19, 'Standard',   9.93, 'No',   2,  636.31),
('P3973',  'Biographies',     'Books',            64.26,  10,  5, 280, '45-54', 'Phoenix, USA',             'Male',       11.61, 'Overnight', 17.15, 'No',  67,   57.83),
('P4154',  'Formal Shoes',    'Footwear',        535.46,   0, 12, 445, '55+',   'Chicago, USA',             'Non-Binary', 35.74, 'Express',   19.83, 'No',  54,  535.46),
('P4720',  'Slippers',        'Footwear',        628.70,  25, 10, 259, '45-54', 'Phoenix, USA',             'Non-Binary',  6.38, 'Overnight', 18.59, 'No',  27,  471.52),
('P4741',  'T-shirt',         'Apparel',        1989.23,  25, 10, 187, '45-54', 'London, UK',               'Non-Binary', 36.30, 'Express',    8.03, 'No', 100, 1491.92),
('P4925',  'T-shirt',         'Apparel',        1754.17,  25, 10, 373, '18-24', 'Phoenix, USA',             'Female',     30.77, 'Overnight',  2.82, 'Yes', 88, 1315.63),
('P5051',  'Textbooks',       'Books',           540.49,  20, 10, 469, '55+',   'Dubai, UAE',               'Non-Binary', 39.95, 'Standard',   5.96, 'Yes', 32,  432.39),
('P5142',  'Skirt',           'Apparel',        1578.13,   5,  8,  84, '45-54', 'Chicago, USA',             'Female',     23.47, 'Standard',   7.22, 'Yes', 48, 1499.22),
('P5283',  'Cookbooks',       'Books',           221.14,  25, 12, 296, '18-24', 'Tokyo, Japan',             'Non-Binary', 15.66, 'Standard',   2.66, 'No',  80,  165.86),
('P5372',  'Slippers',        'Footwear',        857.65,  10, 12, 413, '55+',   'Phoenix, USA',             'Male',       28.41, 'Overnight', 12.60, 'No',  75,  771.88),
('P5529',  'Magazines',       'Books',          1052.89,   5,  8, 177, '18-24', 'Berlin, Germany',          'Female',      1.49, 'Standard',   9.78, 'Yes', 41, 1000.25),
('P5649',  'Blender',         'Home Appliances',1300.37,  25,  8, 244, '25-34', 'Toronto, Canada',          'Male',        6.08, 'Express',    2.55, 'No',  29,  975.28),
('P6520',  'Headphones',      'Electronics',    1779.99,   5, 15, 411, '18-24', 'Phoenix, USA',             'Male',       35.76, 'Overnight', 11.29, 'No',  49, 1690.99),
('P7458',  'Washing Machine', 'Home Appliances',1337.62,  25, 10, 400, '55+',   'London, UK',               'Non-Binary', 49.29, 'Express',   11.77, 'Yes',  8, 1003.21),
('P7476',  'Fiction',         'Books',          1834.48,  10,  8, 489, '55+',   'Chicago, USA',             'Male',        2.67, 'Standard',  11.08, 'No',  88, 1651.03),
('P7594',  'Jacket',          'Apparel',        1701.05,  20,  5, 199, '55+',   'Houston, USA',             'Non-Binary', 38.23, 'Overnight', 18.33, 'No',  56, 1360.84),
('P7747',  'Jeans',           'Apparel',         650.44,  15, 15, 277, '18-24', 'Sydney, Australia',        'Non-Binary', 10.38, 'Overnight',  3.22, 'Yes',  9,  552.87),
('P7778',  'Smartwatch',      'Electronics',     350.84,   5, 12, 482, '25-34', 'New York, USA',            'Female',     33.95, 'Express',   10.79, 'Yes', 36,  333.30),
('P7877',  'Formal Shoes',    'Footwear',       1498.46,  10, 15, 434, '55+',   'Chicago, USA',             'Female',     10.47, 'Overnight', 10.59, 'No',  58, 1348.61),
('P8077',  'Refrigerator',    'Home Appliances',1513.32,  15, 12, 193, '55+',   'Mumbai, India',            'Non-Binary', 49.11, 'Express',   14.67, 'No',   1, 1286.32),
('P8619',  'Formal Shoes',    'Footwear',        290.42,  25, 12, 119, '25-34', 'Phoenix, USA',             'Male',       35.92, 'Standard',   8.69, 'No',   8,  217.82),
('P8908',  'Dishwasher',      'Home Appliances',1835.96,  20, 12,   4, '35-44', 'New York, USA',            'Female',      5.73, 'Overnight', 11.75, 'Yes', 39, 1468.77),
('P8980',  'Tablet',          'Electronics',    1697.39,  15,  5,  35, '55+',   'Paris, France',            'Female',     24.17, 'Standard',   8.88, 'No',  62, 1442.78),
('P9139',  'Blender',         'Home Appliances',1673.70,  15,  5,  21, '55+',   'London, UK',               'Female',      1.71, 'Express',    9.80, 'No',  74, 1422.64),
('P9233',  'Cookbooks',       'Books',           729.83,  15, 12, 184, '45-54', 'Singapore',               'Non-Binary', 49.64, 'Overnight', 16.75, 'No',  22,  620.36),
('P9259',  'Cookbooks',       'Books',          1388.38,   0,  8, 218, '25-34', 'Tokyo, Japan',             'Male',       11.51, 'Standard',  14.37, 'Yes', 96, 1388.38),
('P9699',  'Laptop',          'Electronics',    1052.36,   0, 10, 331, '35-44', 'Singapore',               'Non-Binary', 22.60, 'Overnight', 11.26, 'Yes', 99, 1052.36),
('P9854',  'Comics',          'Books',           350.36,   5,  8, 458, '25-34', 'Berlin, Germany',          'Female',     45.43, 'Standard',  16.38, 'No',  47,  332.84),
('P9951',  'Dishwasher',      'Home Appliances',1445.82,  15,  5, 160, '45-54', 'Mumbai, India',            'Non-Binary', 14.41, 'Express',   14.01, 'Yes',  2, 1228.95);


-- ================================================================
-- QUERY 1 — Basic SELECT + WHERE
-- Products with popularity index above 70 (high-demand items)
-- ================================================================
SELECT
    product_id,
    product_name,
    category,
    price,
    discount,
    popularity_index
FROM
    product_sales
WHERE
    popularity_index > 70
ORDER BY
    popularity_index DESC;


-- ================================================================
-- QUERY 2 — GROUP BY + AVG + SUM
-- Average price, average return rate, and total stock per category
-- ================================================================
SELECT
    category,
    COUNT(*)                          AS total_products,
    ROUND(AVG(price), 2)              AS avg_price,
    ROUND(AVG(final_price), 2)        AS avg_final_price,
    ROUND(AVG(return_rate), 2)        AS avg_return_rate,
    SUM(stock_level)                  AS total_stock,
    ROUND(AVG(popularity_index), 2)   AS avg_popularity
FROM
    product_sales
GROUP BY
    category
ORDER BY
    avg_popularity DESC;


-- ================================================================
-- QUERY 3 — GROUP BY + MAX - MIN
-- Price spread (volatility) per category
-- ================================================================
SELECT
    category,
    ROUND(MAX(price) - MIN(price), 2)   AS price_spread,
    ROUND(MAX(price), 2)                AS highest_price,
    ROUND(MIN(price), 2)                AS lowest_price,
    ROUND(AVG(shipping_cost), 2)        AS avg_shipping_cost
FROM
    product_sales
GROUP BY
    category
ORDER BY
    price_spread DESC;


-- ================================================================
-- QUERY 4 — Window Function: DENSE_RANK
-- Rank products by final_price within each category
-- ================================================================
SELECT
    product_id,
    product_name,
    category,
    price,
    discount,
    final_price,
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY final_price DESC
    ) AS price_rank_in_category
FROM
    product_sales
WHERE
    final_price IS NOT NULL
ORDER BY
    category,
    price_rank_in_category;


-- ================================================================
-- QUERY 5 — WHERE for high return rate (poor performers)
-- Products with return rate above 15% — problematic products
-- ================================================================
SELECT
    product_id,
    product_name,
    category,
    price,
    return_rate,
    popularity_index,
    location
FROM
    product_sales
WHERE
    return_rate > 15
ORDER BY
    return_rate DESC;


-- ================================================================
-- QUERY 6 — Window Function: AVG OVER PARTITION
-- Each product's price vs the average price of its category
-- ================================================================
SELECT
    category,
    product_name,
    ROUND(AVG(price), 2)                                   AS product_avg_price,
    ROUND(AVG(AVG(price)) OVER (PARTITION BY category), 2) AS category_avg_price
FROM
    product_sales
WHERE
    price IS NOT NULL
GROUP BY
    category,
    product_name
ORDER BY
    category,
    product_avg_price DESC;


-- ================================================================
-- QUERY 7 — CTE + DENSE_RANK
-- Rank categories by average popularity index per shipping method
-- ================================================================
WITH ShippingCategorySummary AS (
    SELECT
        shipping_method,
        category,
        ROUND(AVG(popularity_index), 2)  AS avg_popularity,
        ROUND(AVG(return_rate), 2)       AS avg_return_rate,
        COUNT(*)                         AS total_products
    FROM
        product_sales
    WHERE
        popularity_index IS NOT NULL
    GROUP BY
        shipping_method,
        category
)
SELECT
    shipping_method,
    category,
    avg_popularity,
    avg_return_rate,
    total_products,
    DENSE_RANK() OVER (
        PARTITION BY shipping_method
        ORDER BY avg_popularity DESC
    ) AS rank_within_shipping_method
FROM
    ShippingCategorySummary
ORDER BY
    shipping_method ASC,
    rank_within_shipping_method ASC;
