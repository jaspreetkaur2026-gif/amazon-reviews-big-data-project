# Amazon Reviews Big Data Analytics

## Project Overview

This project analyzes the Amazon Reviews dataset using Apache Spark. The project combines review data and product metadata to discover customer behavior, product popularity, store performance, and review trends. The project was first developed locally using a sample dataset and is designed to scale to cloud-based big data processing using AWS.

---

## Dataset

This project uses two Amazon datasets.

1. Amazon Reviews
2. Amazon Product Metadata

Both datasets are joined using the `parent_asin` field.

---

## Technologies Used

- Python
- Apache Spark (PySpark)
- SQL
- Git
- GitHub
- AWS (S3, Athena, Terraform)

---

## Project Structure

```
amazon-reviews-big-data-project/

├── data/
│   └── sample/
├── src/
│   ├── preprocessing.py
│   └── analytics.py
├── queries/
│   └── analysis_queries.sql
├── screenshots/
├── results/
├── slides/
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Data Preprocessing

The preprocessing pipeline performs:

- Missing value handling
- Duplicate removal
- Store name cleaning
- Price conversion
- Rating validation
- Dataset joining

---

## Analytics Performed

The project includes several Spark SQL analyses.

- Top reviewed products
- Store performance
- Verified vs non-verified purchases
- Price range analysis
- Category analysis
- Most helpful products

---

## Requirements

Install the required packages.

```bash
pip install -r requirements.txt
```

---

## Run the Project

Run preprocessing.

```bash
python3 src/preprocessing.py
```

Run analytics.

```bash
python3 src/analytics.py
```

---

## Sample Results

The analysis identified:

- Top reviewed products
- Highest performing stores
- Review trends
- Helpful vote statistics
- Category performance
- Purchase behavior

---

## Future Work

- Upload full dataset to Amazon S3
- Query using Amazon Athena
- Process more than 100 million rows
- Build cloud infrastructure using Terraform
- Improve dashboard and visualization

---

## Author

Jaspreet Kaur

CS-675 Big Data Analytics Final Project