Local Sample

The local Spark solution uses the Amazon All Beauty Reviews dataset together with the matching Amazon Product Metadata dataset.

The local dataset contains approximately 701,528 reviews. This smaller dataset was used to develop, test, and validate the preprocessing pipeline and analytics before deploying the solution to the cloud.

Cloud-Scale Dataset

For the cloud deployment, multiple Amazon Reviews categories were combined to process more than 109,920,290 review records, meeting the CS-675 project requirement of analyzing at least 100 million rows.

The review datasets were joined with the corresponding Amazon Product Metadata datasets using the parent_asin field to perform cross-source analytics.

Cloud Architecture

The project uses the following AWS cloud services and technologies:

Amazon S3 for cloud storage
Amazon Athena for SQL analytics
Terraform for Infrastructure as Code
Apache Spark (PySpark) for data preprocessing
AWS CLI for cloud management
GitHub for source code and documentation
Preprocessing Pipeline

The preprocessing pipeline includes the following steps:

Missing value handling
Duplicate removal
Outlier detection and treatment
Data normalization
Boolean encoding for verified purchases
Rating binning
Price binning
Store and product data cleaning
Data validation
Cross-source dataset joining
Analytics Performed

The project performs multiple analytical queries, including:

Total number of reviews
Total number of products
Average product rating
Product popularity analysis
Store performance analysis
Verified purchase analysis
Rating distribution
Helpful vote analysis
Cross-source joins between reviews and product metadata
Cloud Results

The cloud deployment successfully processed:

109,920,290 Amazon review records
8,828,493 Amazon product metadata records
Average Rating: 4.15

The project generated insights including:

Customer purchasing behavior
Product popularity
Product performance
Store analysis
Rating distribution
Cross-source analytics using Amazon Athena
Cost Management

Amazon Athena was selected because it is a serverless and cost-effective service for large-scale analytics on data stored in Amazon S3.

After the project submission and presentation, AWS resources can be deleted to avoid unnecessary cloud charges.
