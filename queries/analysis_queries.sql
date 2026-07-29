-- Query 1
SELECT
    product_title,
    store,
    COUNT(*) AS review_count,
    AVG(rating) AS average_rating
FROM amazon_joined
GROUP BY product_title, store
ORDER BY review_count DESC
LIMIT 20;

-- Query 2
SELECT
    store,
    COUNT(*) AS review_count,
    AVG(rating) AS average_rating
FROM amazon_joined
GROUP BY store
ORDER BY review_count DESC;

-- Query 3
SELECT
    verified_purchase,
    COUNT(*) AS review_count,
    AVG(rating) AS average_rating
FROM amazon_joined
GROUP BY verified_purchase;

-- Query 4
SELECT
    main_category,
    COUNT(*) AS review_count,
    AVG(rating) AS average_rating
FROM amazon_joined
GROUP BY main_category;

-- Query 5
SELECT
    parent_asin,
    product_title,
    SUM(helpful_vote) AS total_helpful_votes
FROM amazon_joined
GROUP BY parent_asin, product_title
ORDER BY total_helpful_votes DESC
LIMIT 20;