# 🛒 B2B Retail Data Pipeline on Google BigQuery

## Project Overview

This project automates the ingestion, processing, and governance of B2B retail data into Google BigQuery.  
It simulates a real-world scenario where multiple data sources — such as sales, stock levels, and product loss — must be securely managed, anonymized, and made available for analytics.

Key Features:
- Incremental CSV uploads
- Load date tracking for batch auditing and historical analysis
- Data masking for sensitive product and store fields
- Looker Studio dashboard connection for data visualization
- Production-ready scripts structured for scalability and governance

This project demonstrates critical skills for Data Management leadership, including secure ingestion pipelines, cloud governance, and analytics enablement.

---

## Architecture Overview

- **Data Source**: Weekly B2B retail CSV files (Sales, Stock, Product Loss)
- **Data Ingestion**: Python scripts using BigQuery Client API
- **Data Storage**: Google BigQuery (GCP)
- **Security & Governance**:
  - Service Account authentication
  - Explicit schema enforcement
  - Load date tracking on ingestion
  - Data Anonymization (SHA256 & Sequential Labeling)
- **Visualization**: Google Looker Studio connected to BigQuery masked views

---

## Project Structure

```plaintext
b2b-retail-bigquery-data-pipeline/
├── README.md
├── LICENSE
├── .gitignore
├── requirements.txt
├── scripts/
│   ├── upload_sales_to_bq.py
│   ├── upload_product_loss_to_bq.py
│   ├── upload_stock_to_bq.py
├── sample_data/
│   ├── sales_sample.csv
│   ├── product_loss_sample.csv
│   ├── stock_sample.csv
```

---

## How to Run the Project

1. Prerequisites
- A Google Cloud Platform account
- BigQuery dataset created
- Service Account credentials with BigQuery permissions

2. Setup Environment
```plaintext
pip install -r requirements.txt
```

3. Configuration
- Copy your Service Account JSON file to the project directory.
- Set the project ID, dataset ID, and target table names in each script.
- Set the `MANUAL_LOAD_DATE` value to the upload batch date you want for each execution.

4. Upload Data
Choose and run the corresponding script:
```plaintext
python scripts/upload_sales_to_bq.py
python scripts/upload_product_loss_to_bq.py
python scripts/upload_stock_to_bq.py
```
New CSVs will be uploaded incrementally, logged, and moved to a `/processed` folder automatically.

---

## Data Masking Strategy

Sensitive fields such as Product Names and Store Names are anonymized:

| Field                 | Masking Method                      |
|:----------------------|:------------------------------------|
| COD_CENCOSUD           | SHA256 Hash                         |
| DESCRIPCION_PRODUCTO   | Sequential Label (`Product_1`, `Product_2`, ...) |
| COD_LOCAL              | SHA256 Hash                         |
| DESCRIPCION_LOCAL      | Sequential Label (`Store_1`, `Store_2`, ...) |


Masked Views are created in BigQuery under /sql/masked_views/ directory.
Example (Stock Masked View):

```plaintext
CREATE OR REPLACE VIEW client_dataset.stock_masked AS
WITH products AS (
  SELECT
    DISTINCT DESCRIPCION_PRODUCTO,
    CONCAT('Product_', CAST(ROW_NUMBER() OVER (ORDER BY DESCRIPCION_PRODUCTO) AS STRING)) AS masked_product
  FROM
    client_dataset.stock
),
stores AS (
  SELECT
    DISTINCT COD_LOCAL,
    CONCAT('Store_', CAST(ROW_NUMBER() OVER (ORDER BY COD_LOCAL) AS STRING)) AS masked_store
  FROM
    client_dataset.stock
)
SELECT
  p.masked_product,
  s.masked_store,
  t.STOCK_Un,
  t.VTA_ULT_12SEM,
  t.load_date
FROM
  client_dataset.stock t
LEFT JOIN products p ON t.DESCRIPCION_PRODUCTO = p.DESCRIPCION_PRODUCTO
LEFT JOIN stores s ON t.COD_LOCAL = s.COD_LOCAL;
```
Ensures compliance with data protection and privacy standards.

---

## Looker Studio Dashboard

A Looker Studio dashboard was developed based on the masked views, allowing users to:
- Analyze stock levels, sales volume, and loss rates
- Filter by masked product, masked store, load dates, and regional segmentation
- Safely visualize insights without exposing sensitive product or store information

---

## Skills and Concepts Demonstrated

- Cloud Data Architecture (GCP BigQuery)
- Secure Data Ingestion Pipelines (Service Accounts, Schema Enforcement)
- Data Governance (Load Date Tracking, Field Masking)
- Business Intelligence (Looker Studio Integration)
- Process Automation (Incremental Uploads, File Processing)
- Scalability and Production-Readiness

---

## About

Created by Juan Manuel Rozas Andaur, Ph.D.
Chief Data & Strategy Executive | CDO | CSO | Digital Transformation Leader
[Visit Portfolio](https://jmrozas.com)
