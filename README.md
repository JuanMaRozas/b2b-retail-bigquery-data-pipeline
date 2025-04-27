# 🛒 B2B Retail Data Pipeline on Google BigQuery

## Project Overview

This project automates the ingestion, processing, and governance of B2B retail data into Google BigQuery.  
It simulates a real-world scenario where multiple data sources — such as sales, stock levels, and product loss — must be securely managed, anonymized, and made available for analytics.

Key Features:
- ✅ Incremental CSV uploads
- ✅ Load date tracking for batch auditing and historical analysis
- ✅ Data masking for sensitive product and store fields
- ✅ Looker Studio dashboard connection for data visualization
- ✅ Production-ready scripts structured for scalability and governance

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
├── sql/
│   ├── add_load_date_existing_tables.sql
│   └── masked_views/
│       ├── create_stock_masked_view.sql
│       ├── create_sales_masked_view.sql
│       └── create_product_loss_masked_view.sql
├── docs/
│   ├── architecture_diagram.png
│   ├── dashboard_overview.png
├── sample_data/
│   ├── sales_sample.csv
│   ├── product_loss_sample.csv
│   ├── stock_sample.csv

---

## How to Run the Project

1. Prerequisites
- A Google Cloud Platform account
- BigQuery dataset created (e.g., tivoli_dataset)
- Service Account credentials with BigQuery permissions

2. Setup Environment
pip install -r requirements.txt
