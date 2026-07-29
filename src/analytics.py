from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    from_unixtime,
    get_json_object,
    lit,
    sum as spark_sum,
    to_timestamp,
    when,
)


def main():
    spark = (
        SparkSession.builder
        .appName("AmazonReviewsAnalytics")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    reviews_path = "data/sample/reviews.jsonl.gz"
    metadata_path = "data/sample/metadata.jsonl.gz"

    reviews = spark.read.json(reviews_path)

    metadata_raw = spark.read.text(metadata_path)

    metadata = metadata_raw.select(
        get_json_object(col("value"), "$.parent_asin").alias("parent_asin"),
        get_json_object(col("value"), "$.title").alias("product_title"),
        get_json_object(col("value"), "$.main_category").alias("main_category"),
        get_json_object(col("value"), "$.average_rating")
        .cast("double")
        .alias("product_average_rating"),
        get_json_object(col("value"), "$.rating_number")
        .cast("long")
        .alias("rating_number"),
        get_json_object(col("value"), "$.price")
        .cast("double")
        .alias("price"),
        get_json_object(col("value"), "$.store").alias("store"),
    )

    reviews_clean = (
        reviews
        .dropDuplicates(["user_id", "parent_asin", "timestamp"])
        .filter(col("parent_asin").isNotNull())
        .filter(col("rating").between(1.0, 5.0))
        .withColumn(
            "review_timestamp",
            to_timestamp(from_unixtime(col("timestamp") / 1000)),
        )
        .withColumn(
            "helpful_vote",
            when(col("helpful_vote").isNull(), lit(0))
            .otherwise(col("helpful_vote")),
        )
        .select(
            "parent_asin",
            "user_id",
            "rating",
            "helpful_vote",
            "verified_purchase",
            "review_timestamp",
        )
    )

    metadata_clean = (
        metadata
        .dropDuplicates(["parent_asin"])
        .filter(col("parent_asin").isNotNull())
        .withColumn(
            "product_title",
            when(
                col("product_title").isNull()
                | (col("product_title") == ""),
                lit("Unknown Product"),
            ).otherwise(col("product_title")),
        )
        .withColumn(
            "main_category",
            when(
                col("main_category").isNull()
                | (col("main_category") == ""),
                lit("Unknown Category"),
            ).otherwise(col("main_category")),
        )
        .withColumn(
            "store",
            when(
                col("store").isNull() | (col("store") == ""),
                lit("Unknown Store"),
            ).otherwise(col("store")),
        )
        .withColumn(
            "price",
            when(
                col("price").isNull() | (col("price") < 0),
                lit(0.0),
            ).otherwise(col("price")),
        )
    )

    joined = reviews_clean.join(
        metadata_clean,
        on="parent_asin",
        how="inner",
    )

    joined.createOrReplaceTempView("amazon_joined")

    print("\nQUERY 1: TOP PRODUCTS BY REVIEW COUNT")

    query1 = spark.sql("""
        SELECT
            parent_asin,
            product_title,
            store,
            COUNT(*) AS review_count,
            ROUND(AVG(rating), 2) AS average_review_rating
        FROM amazon_joined
        GROUP BY parent_asin, product_title, store
        HAVING COUNT(*) >= 50
        ORDER BY review_count DESC
        LIMIT 20
    """)

    query1.show(20, truncate=False)

    print("\nQUERY 2: STORE PERFORMANCE")

    query2 = spark.sql("""
        SELECT
            store,
            COUNT(*) AS review_count,
            ROUND(AVG(rating), 2) AS average_rating,
            SUM(helpful_vote) AS total_helpful_votes
        FROM amazon_joined
        GROUP BY store
        HAVING COUNT(*) >= 100
        ORDER BY review_count DESC
        LIMIT 20
    """)

    query2.show(20, truncate=False)

    print("\nQUERY 3: VERIFIED VS NON-VERIFIED PURCHASES")

    query3 = spark.sql("""
        SELECT
            verified_purchase,
            COUNT(*) AS review_count,
            ROUND(AVG(rating), 2) AS average_rating,
            ROUND(AVG(helpful_vote), 2) AS average_helpful_votes
        FROM amazon_joined
        GROUP BY verified_purchase
        ORDER BY review_count DESC
    """)

    query3.show(truncate=False)

    print("\nQUERY 4: PRICE RANGE ANALYSIS")

    query4 = spark.sql("""
        SELECT
            CASE
                WHEN price = 0 THEN 'Missing or Zero'
                WHEN price < 10 THEN 'Under $10'
                WHEN price < 25 THEN '$10-$24.99'
                WHEN price < 50 THEN '$25-$49.99'
                ELSE '$50 and Above'
            END AS price_range,
            COUNT(*) AS review_count,
            ROUND(AVG(rating), 2) AS average_rating
        FROM amazon_joined
        GROUP BY
            CASE
                WHEN price = 0 THEN 'Missing or Zero'
                WHEN price < 10 THEN 'Under $10'
                WHEN price < 25 THEN '$10-$24.99'
                WHEN price < 50 THEN '$25-$49.99'
                ELSE '$50 and Above'
            END
        ORDER BY review_count DESC
    """)

    query4.show(truncate=False)

    print("\nQUERY 5: CATEGORY ANALYSIS")

    query5 = spark.sql("""
        SELECT
            main_category,
            COUNT(*) AS review_count,
            ROUND(AVG(rating), 2) AS average_review_rating,
            ROUND(AVG(product_average_rating), 2)
                AS average_product_rating
        FROM amazon_joined
        GROUP BY main_category
        ORDER BY review_count DESC
    """)

    query5.show(truncate=False)

    print("\nQUERY 6: MOST HELPFUL PRODUCTS")

    query6 = spark.sql("""
        SELECT
            parent_asin,
            product_title,
            store,
            COUNT(*) AS review_count,
            SUM(helpful_vote) AS total_helpful_votes,
            ROUND(AVG(rating), 2) AS average_rating
        FROM amazon_joined
        GROUP BY parent_asin, product_title, store
        HAVING COUNT(*) >= 20
        ORDER BY total_helpful_votes DESC
        LIMIT 20
    """)

    query6.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()