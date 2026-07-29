from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_unixtime,
    get_json_object,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
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

    reviews = spark.read.json(reviews_path)

    # Read metadata as text because nested duplicate keys can
    # cause Spark schema inference errors.
    metadata_raw = spark.read.text(metadata_path)

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

    print("\nBEFORE PREPROCESSING: REVIEW NULL COUNTS")
    reviews.select(
        spark_sum(
            when(
                col("text").isNull() | (col("text") == ""),
                1,
            ).otherwise(0)
        ).alias("missing_review_text"),
        spark_sum(
            when(
                col("helpful_vote").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_helpful_vote"),
        spark_sum(
            when(
                col("verified_purchase").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_verified_purchase"),
        spark_sum(
            when(
                col("parent_asin").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_parent_asin"),
    ).show(truncate=False)

    print("\nBEFORE PREPROCESSING: METADATA NULL COUNTS")
    metadata.select(
        spark_sum(
            when(
                col("price").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_price"),
        spark_sum(
            when(
                col("store").isNull() | (col("store") == ""),
                1,
            ).otherwise(0)
        ).alias("missing_store"),
        spark_sum(
            when(
                col("main_category").isNull()
                | (col("main_category") == ""),
                1,
            ).otherwise(0)
        ).alias("missing_category"),
        spark_sum(
            when(
                col("average_rating").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_average_rating"),
    ).show(truncate=False)

    print("\nBEFORE PREPROCESSING: NUMERIC RANGES")
    reviews.select(
        spark_min("helpful_vote").alias("min_helpful_vote"),
        spark_max("helpful_vote").alias("max_helpful_vote"),
        spark_min("rating").alias("min_rating"),
        spark_max("rating").alias("max_rating"),
    ).show(truncate=False)

    metadata.select(
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        spark_min("rating_number").alias("min_rating_number"),
        spark_max("rating_number").alias("max_rating_number"),
    ).show(truncate=False)

    # Calculate 99th-percentile limits for outlier treatment.
    helpful_vote_limit = (
        reviews
        .filter(col("helpful_vote").isNotNull())
        .approxQuantile(
            "helpful_vote",
            [0.99],
            0.001,
        )[0]
    )

    price_limit = (
        metadata
        .filter(
            col("price").isNotNull()
            & (col("price") >= 0)
        )
        .approxQuantile(
            "price",
            [0.99],
            0.001,
        )[0]
    )

    print("\nOUTLIER LIMITS")
    print("99th percentile helpful_vote:", helpful_vote_limit)
    print("99th percentile price:", price_limit)

    # Clean and preprocess the reviews dataset.
    reviews_clean = (
        reviews
        .dropDuplicates(
            ["user_id", "parent_asin", "timestamp"]
        )
        .filter(col("parent_asin").isNotNull())
        .filter(col("rating").between(1.0, 5.0))

        # Imputation
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

        # Outlier treatment
        .withColumn(
            "helpful_vote_capped",
            when(
                col("helpful_vote_clean")
                > lit(helpful_vote_limit),
                lit(helpful_vote_limit),
            ).otherwise(col("helpful_vote_clean")),
        )

        # Encoding
        .withColumn(
            "verified_purchase_encoded",
            when(
                col("verified_purchase_clean") == True,
                lit(1),
            ).otherwise(lit(0)),
        )

        # Binning
        .withColumn(
            "rating_bin",
            when(
                col("rating") <= 2,
                lit("Low"),
            )
            .when(
                col("rating") <= 4,
                lit("Medium"),
            )
            .otherwise(lit("High")),
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
            "rating_bin",
            col("title").alias("review_title"),
            "review_text",
            "helpful_vote_clean",
            "helpful_vote_capped",
            "verified_purchase_clean",
            "verified_purchase_encoded",
            "review_timestamp",
        )
    )

    # Clean and preprocess the metadata dataset.
    metadata_clean = (
        metadata
        .dropDuplicates(["parent_asin"])
        .filter(col("parent_asin").isNotNull())

        # Imputation
        .withColumn(
            "product_title",
            when(
                col("title").isNull()
                | (col("title") == ""),
                lit("Unknown Product"),
            ).otherwise(col("title")),
        )
        .withColumn(
            "main_category_clean",
            when(
                col("main_category").isNull()
                | (col("main_category") == ""),
                lit("Unknown Category"),
            ).otherwise(col("main_category")),
        )
        .withColumn(
            "store_clean",
            when(
                col("store").isNull()
                | (col("store") == ""),
                lit("Unknown Store"),
            ).otherwise(col("store")),
        )
        .withColumn(
            "price_imputed",
            when(
                col("price").isNull()
                | (col("price") < 0),
                lit(0.0),
            ).otherwise(col("price")),
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

        # Outlier treatment
        .withColumn(
            "price_capped",
            when(
                col("price_imputed") > lit(price_limit),
                lit(price_limit),
            ).otherwise(col("price_imputed")),
        )

        # Binning
        .withColumn(
            "price_bin",
            when(
                col("price_capped") == 0,
                lit("Missing or Zero"),
            )
            .when(
                col("price_capped") < 10,
                lit("Under $10"),
            )
            .when(
                col("price_capped") < 25,
                lit("$10-$24.99"),
            )
            .when(
                col("price_capped") < 50,
                lit("$25-$49.99"),
            )
            .otherwise(lit("$50 and Above")),
        )
        .select(
            "parent_asin",
            "product_title",
            col("main_category_clean").alias(
                "main_category"
            ),
            "average_rating_clean",
            "rating_number_clean",
            "price_imputed",
            "price_capped",
            "price_bin",
            "store_clean",
        )
    )

    # Get min and max price values for normalization.
    price_stats = metadata_clean.select(
        spark_min("price_capped").alias("min_price"),
        spark_max("price_capped").alias("max_price"),
    ).first()

    min_price = price_stats["min_price"]
    max_price = price_stats["max_price"]

    # Normalize price using min-max scaling.
    if max_price != min_price:
        metadata_clean = metadata_clean.withColumn(
            "price_normalized",
            spark_round(
                (
                    col("price_capped") - lit(min_price)
                )
                / (
                    lit(max_price) - lit(min_price)
                ),
                4,
            ),
        )
    else:
        metadata_clean = metadata_clean.withColumn(
            "price_normalized",
            lit(0.0),
        )

    # Join reviews and metadata using parent_asin.
    joined = reviews_clean.join(
        metadata_clean,
        on="parent_asin",
        how="inner",
    )

    print("\nCLEANED ROW COUNTS")
    print("Clean reviews:", reviews_clean.count())
    print("Clean metadata:", metadata_clean.count())
    print("Joined rows:", joined.count())

    print("\nAFTER PREPROCESSING: NUMERIC RANGES")
    joined.select(
        spark_min("helpful_vote_capped").alias(
            "min_helpful_vote"
        ),
        spark_max("helpful_vote_capped").alias(
            "max_helpful_vote"
        ),
        spark_min("price_capped").alias("min_price"),
        spark_max("price_capped").alias("max_price"),
        spark_min("price_normalized").alias(
            "min_normalized_price"
        ),
        spark_max("price_normalized").alias(
            "max_normalized_price"
        ),
    ).show(truncate=False)

    print("\nRATING BIN DISTRIBUTION")
    (
        joined
        .groupBy("rating_bin")
        .count()
        .orderBy("rating_bin")
        .show()
    )

    print("\nPRICE BIN DISTRIBUTION")
    (
        joined
        .groupBy("price_bin")
        .count()
        .orderBy("price_bin")
        .show()
    )

    print("\nVERIFIED PURCHASE ENCODING")
    (
        joined
        .groupBy(
            "verified_purchase_clean",
            "verified_purchase_encoded",
        )
        .count()
        .show()
    )

    print("\nSAMPLE PREPROCESSED RECORDS")
    joined.select(
        "parent_asin",
        "product_title",
        "rating",
        "rating_bin",
        "helpful_vote_clean",
        "helpful_vote_capped",
        "verified_purchase_encoded",
        "price_imputed",
        "price_capped",
        "price_normalized",
        "price_bin",
        "main_category",
        "store_clean",
    ).show(10, truncate=False)

    print("\nTOP STORES BY REVIEW COUNT")
    (
        joined
        .groupBy("store_clean")
        .agg(
            count("*").alias("review_count"),
            avg("rating").alias(
                "average_review_rating"
            ),
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