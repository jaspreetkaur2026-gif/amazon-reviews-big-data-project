from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_unixtime,
    get_json_object,
    lit,
    to_timestamp,
    when,
)


def main():
    spark = (
        SparkSession.builder
        .appName("AmazonReviewsPreprocessing")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    reviews_path = "data/sample/reviews.jsonl.gz"
    metadata_path = "data/sample/metadata.jsonl.gz"

    # Read reviews normally
    reviews = spark.read.json(reviews_path)

    # Read metadata as text first because some nested fields
    # contain duplicate column names.
    metadata_raw = spark.read.text(metadata_path)

    # Extract only the metadata fields needed for this project.
    metadata = metadata_raw.select(
        get_json_object(col("value"), "$.parent_asin").alias("parent_asin"),
        get_json_object(col("value"), "$.title").alias("title"),
        get_json_object(col("value"), "$.main_category").alias("main_category"),
        get_json_object(col("value"), "$.average_rating")
        .cast("double")
        .alias("average_rating"),
        get_json_object(col("value"), "$.rating_number")
        .cast("long")
        .alias("rating_number"),
        get_json_object(col("value"), "$.price")
        .cast("double")
        .alias("price"),
        get_json_object(col("value"), "$.store").alias("store"),
    )

    print("\nREVIEWS SCHEMA")
    reviews.printSchema()

    print("\nMETADATA SCHEMA")
    metadata.printSchema()

    print("\nORIGINAL ROW COUNTS")
    print("Reviews:", reviews.count())
    print("Metadata:", metadata.count())

    # Clean the reviews dataset
    reviews_clean = (
        reviews
        .dropDuplicates(["user_id", "parent_asin", "timestamp"])
        .filter(col("parent_asin").isNotNull())
        .filter(col("rating").between(1.0, 5.0))
        .withColumn(
            "review_text",
            when(
                col("text").isNull() | (col("text") == ""),
                lit("No review text"),
            ).otherwise(col("text")),
        )
        .withColumn(
            "helpful_vote_clean",
            when(
                col("helpful_vote").isNull(),
                lit(0),
            ).otherwise(col("helpful_vote")),
        )
        .withColumn(
            "verified_purchase_clean",
            when(
                col("verified_purchase").isNull(),
                lit(False),
            ).otherwise(col("verified_purchase")),
        )
        .withColumn(
            "review_timestamp",
            to_timestamp(
                from_unixtime(col("timestamp") / 1000)
            ),
        )
        .select(
            "parent_asin",
            "asin",
            "user_id",
            "rating",
            col("title").alias("review_title"),
            "review_text",
            "helpful_vote_clean",
            "verified_purchase_clean",
            "review_timestamp",
        )
    )

    # Clean the metadata dataset
    metadata_clean = (
        metadata
        .dropDuplicates(["parent_asin"])
        .filter(col("parent_asin").isNotNull())
        .withColumn(
            "product_title",
            when(
                col("title").isNull() | (col("title") == ""),
                lit("Unknown Product"),
            ).otherwise(col("title")),
        )
        .withColumn(
            "category_clean",
            when(
                col("main_category").isNull()
                | (col("main_category") == ""),
                lit("Unknown Category"),
            ).otherwise(col("main_category")),
        )
        .withColumn(
            "price_clean",
            when(
                col("price").isNull() | (col("price") < 0),
                lit(0.0),
            ).otherwise(col("price")),
        )
        .withColumn(
            "store_clean",
            when(
                col("store").isNull() | (col("store") == ""),
                lit("Unknown Store"),
            ).otherwise(col("store")),
        )
        .withColumn(
            "average_rating_clean",
            when(
                col("average_rating").isNull(),
                lit(0.0),
            ).otherwise(col("average_rating")),
        )
        .withColumn(
            "rating_number_clean",
            when(
                col("rating_number").isNull(),
                lit(0),
            ).otherwise(col("rating_number")),
        )
        .select(
            "parent_asin",
            "product_title",
            col("category_clean").alias("main_category"),
            "average_rating_clean",
            "rating_number_clean",
            "price_clean",
            "store_clean",
        )
    )

    # Join reviews and metadata using parent_asin
    joined = reviews_clean.join(
        metadata_clean,
        on="parent_asin",
        how="inner",
    )

    print("\nCLEANED ROW COUNTS")
    print("Clean reviews:", reviews_clean.count())
    print("Clean metadata:", metadata_clean.count())
    print("Joined rows:", joined.count())

    print("\nSAMPLE JOINED RECORDS")
    joined.show(10, truncate=False)

    print("\nTOP STORES BY REVIEW COUNT")
    (
        joined
        .groupBy("store_clean")
        .agg(
            count("*").alias("review_count"),
            avg("rating").alias("average_review_rating"),
        )
        .orderBy(col("review_count").desc())
        .show(20, truncate=False)
    )

    print("\nAVERAGE RATING BY CATEGORY")
    (
        joined
        .groupBy("main_category")
        .agg(
            count("*").alias("review_count"),
            avg("rating").alias("average_rating"),
        )
        .orderBy(col("review_count").desc())
        .show(20, truncate=False)
    )

    print("\nVERIFIED PURCHASE ANALYSIS")
    (
        joined
        .groupBy("verified_purchase_clean")
        .agg(
            count("*").alias("review_count"),
            avg("rating").alias("average_rating"),
        )
        .orderBy(col("review_count").desc())
        .show(truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()