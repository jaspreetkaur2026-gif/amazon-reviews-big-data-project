# Amazon Reviews Big Data Analytics

## Project Overview

This project analyzes the Amazon Reviews dataset using Apache Spark (PySpark) and AWS cloud services. The project combines Amazon review data with Amazon product metadata to analyze customer behavior, product popularity, store performance, and customer purchasing trends.

The solution was first developed locally using a smaller sample dataset and then deployed to AWS using Amazon S3, Amazon Athena, and Terraform to demonstrate scalable cloud-based big data analytics.

---

# Project Objectives

The objectives of this project are to:

- Build a scalable big data analytics pipeline using Apache Spark.
- Clean and preprocess Amazon review and product metadata.
- Join multiple datasets using the `parent_asin` field.
- Analyze customer purchasing behavior and product performance.
- Deploy the solution on AWS cloud infrastructure.
- Manage cloud resources using Terraform.
- Demonstrate cloud-based analytics using Amazon Athena.

---

# Dataset

This project uses two Amazon public datasets:

1. Amazon Reviews Dataset
2. Amazon Product Metadata Dataset

The datasets are joined using the **parent_asin** field.

The local implementation uses approximately **701,528 Amazon reviews** for development and testing before scaling the same workflow to cloud-based processing.

---

# Technologies Used

- Python
- Apache Spark (PySpark)
- Spark SQL
- AWS S3
- Amazon Athena
- Terraform
- AWS CLI
- Git
- GitHub

---

# Project Structure

```text
amazon-reviews-big-data-project/

├── data/
│   └── sample/
├── infrastructure/
├── queries/
├── results/
├── screenshots/
├── slides/
├── src/
│   ├── preprocessing.py
│   └── analytics.py
├── README.md
├── requirements.txt
└── .gitignore
```

---

# Data Preprocessing

The preprocessing pipeline performs the following steps:

- Missing value imputation
- Duplicate removal
- Outlier detection and treatment
- Min-Max normalization
- Boolean encoding for verified purchases
- Rating binning
- Price binning
- Store and product data cleaning
- Price validation
- Cross-source dataset joining

---

# Cloud Infrastructure

AWS services used in this project include:

- Amazon S3 for cloud storage
- Amazon Athena for SQL analytics
- Terraform for Infrastructure as Code
- AWS CLI for cloud management

Terraform is used to provision and manage the cloud infrastructure, making the deployment reproducible.

---

# Analytics Performed

The project performs multiple analytical queries, including:

- Average rating by product category
- Top reviewed products
- Store performance analysis
- Verified vs. non-verified purchase analysis
- Product rating distribution
- Price range analysis
- Helpful vote analysis
- Cross-source joins between reviews and product metadata

---

# Running the Project

## Install Required Packages

```bash
pip install -r requirements.txt
```

## Run the Preprocessing Pipeline

```bash
python3 src/preprocessing.py
```

## Run Analytics

```bash
python3 src/analytics.py
```

---

# Sample Results

The project successfully analyzed more than **700,000 Amazon reviews** and generated insights including:

- Customer purchasing behavior
- Product popularity
- Product category performance
- Store performance
- Rating distribution
- Verified purchase behavior
- Helpful vote statistics
- Cross-source analytics using review and metadata datasets

---

# Future Improvements

Future enhancements include:

- Processing larger Amazon review datasets
- Scaling the cloud solution to process over 100 million rows
- Building an interactive dashboard
- Adding machine learning models for review prediction
- Optimizing Spark and Athena query performance

---

# Author

**Jaspreet Kaur**

CS-675 Big Data Analytics Final Project

---

# License

This project was developed for the CS-675 Big Data Analytics course at Monroe College.