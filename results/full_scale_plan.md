# Full-Scale Cloud Plan

## Local Sample

The local Spark solution uses the Amazon All Beauty Reviews dataset together with the matching Amazon Product Metadata dataset.

The local dataset contains approximately 701,528 reviews. This smaller dataset is used to develop, test, and validate the preprocessing pipeline and analytics before running the project on a much larger cloud dataset.

## Cloud-Scale Dataset

For the cloud deployment, multiple Amazon Reviews categories will be combined until the total number of review records exceeds 100 million rows, satisfying the CS-675 project requirement.

Each review dataset will be joined with the corresponding Amazon Product Metadata dataset using the **parent_asin** field.

## Cloud Architecture

- Amazon S3 for cloud storage
- Amazon Athena for SQL analytics
- Terraform for Infrastructure as Code
- Apache Spark (PySpark) for preprocessing
- GitHub for source code and documentation

## Preprocessing Pipeline

The preprocessing pipeline includes:

- Missing value imputation
- Duplicate removal
- Outlier treatment
- Data normalization
- Categorical encoding
- Feature binning

## Analytics Performed

The project includes the following analytics:

1. Average rating by category
2. Top stores by review count
3. Verified purchase analysis
4. Rating distribution
5. Price range analysis
6. Cross-source joins between reviews and product metadata

## Cost Management

The project uses Amazon Athena because it is serverless and cost-effective for large-scale analytics.

After the final demonstration, all AWS resources will be deleted to avoid unnecessary charges.