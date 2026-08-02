# Amazon Reviews Big Data Analytics at Cloud Scale

## Project Overview

This project analyzes the Amazon Reviews dataset using Apache Spark (PySpark) and AWS cloud services. The project combines Amazon review data with Amazon product metadata to analyze customer behavior, product popularity, product ratings, and purchasing trends.

The solution was first developed locally using a smaller sample dataset and then deployed to AWS cloud services using Amazon S3, Amazon Athena, and Terraform to demonstrate scalable big data analytics.

---

# Project Objectives

- Build a scalable big data analytics solution using Apache Spark (PySpark).
- Clean and preprocess Amazon review and metadata datasets.
- Join multiple datasets using the **parent_asin** field.
- Analyze customer behavior and product performance.
- Deploy the solution using AWS cloud services.
- Manage cloud infrastructure using Terraform.
- Execute analytical SQL queries using Amazon Athena.

---

# Datasets

This project uses two Amazon public datasets.

### Amazon Reviews Dataset

Contains customer reviews, ratings, verified purchases, review text, and helpful votes.

### Amazon Product Metadata Dataset

Contains product information including title, category, price, brand, and store.

### Join Key

Both datasets are joined using:

```
parent_asin
```

Local development was completed using a smaller sample dataset, while the cloud deployment analyzed over **109 million review records**.

---

# Technologies Used

- Python
- Apache Spark (PySpark)
- Spark SQL
- Amazon S3
- Amazon Athena
- Terraform
- AWS CLI
- Git
- GitHub

---

# Project Structure

```
amazon-reviews-big-data-project/

├── data/
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

The preprocessing pipeline includes:

- Missing value handling
- Duplicate removal
- Outlier treatment
- Data normalization
- Boolean encoding
- Rating binning
- Price binning
- Data cleaning
- Data validation
- Cross-source dataset joining

---

# Cloud Infrastructure

The project uses:

- Amazon S3 for cloud storage
- Amazon Athena for SQL analytics
- Terraform for Infrastructure as Code
- AWS CLI for cloud management

Terraform provisions the cloud resources, making the deployment reproducible.

---

# Analytics Performed

The project includes analytical SQL queries such as:

- Total number of reviews
- Total number of products
- Average product rating
- Product popularity
- Store performance
- Product rating distribution
- Verified purchase analysis
- Helpful vote analysis
- Cross-source joins between reviews and product metadata

---

# Running the Project

## Install dependencies

```bash
pip install -r requirements.txt
```

## Run preprocessing

```bash
python3 src/preprocessing.py
```

## Run analytics

```bash
python3 src/analytics.py
```

---

# Cloud Results

Cloud deployment successfully processed:

- **109,920,290 Amazon review records**
- **8,828,493 product metadata records**
- **Average Rating: 4.15**

The project generated insights including:

- Customer purchasing behavior
- Product popularity
- Product performance
- Store analysis
- Rating distribution
- Cross-source analytics
- SQL-based cloud analytics using Amazon Athena

---

# Future Improvements

- Build an interactive dashboard.
- Add machine learning models for prediction.
- Optimize Spark and Athena performance.
- Expand the analysis to additional Amazon product categories.
- Automate data ingestion and reporting.

---
# Dataset Source

Amazon Reviews 2023 Dataset

https://amazon-reviews-2023.github.io/

# Author

**Jaspreet Kaur**

CS-675 Big Data Analytics Final Project

---

# License

This project was developed for the CS-675 Big Data Analytics course at Monroe College.
