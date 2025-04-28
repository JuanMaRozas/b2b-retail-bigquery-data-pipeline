from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import shutil
import yaml

# --- CONFIGURATION ---
# Load configuration
with open('config.yaml') as config_file:
    config = yaml.safe_load(config_file)

PROJECT_ID = config["project_id"]                
DATASET_ID = config["dataset_id"]                
SERVICE_ACCOUNT_FILE = config["service_account_file"]
CSV_FOLDER = "csv_files"
PROCESSED_FOLDER = "processed"
TARGET_TABLE = "product_loss"
LOCATION = config["location"]
UPLOAD_LOG = "upload_log_product_loss.txt"
# ----------------------

# Authenticate
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Create dataset if needed
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset '{DATASET_ID}' found.")
except Exception:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    client.create_dataset(dataset)
    print(f"Created dataset '{DATASET_ID}'.")

# Ensure processed folder exists
if not os.path.exists(PROCESSED_FOLDER):
    os.makedirs(PROCESSED_FOLDER)

# Load upload history
uploaded_files = set()
if os.path.exists(UPLOAD_LOG):
    with open(UPLOAD_LOG, "r") as f:
        uploaded_files = set(line.strip() for line in f.readlines())

# Find new files
new_files = [
    f for f in os.listdir(CSV_FOLDER)
    if f.startswith("product_loss_") and f.endswith(".csv") and f not in uploaded_files
]

if not new_files:
    print("No new files to upload.")
else:
    print(f"New files to upload: {new_files}")

    for filename in new_files:
        file_path = os.path.join(CSV_FOLDER, filename)
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}"
        print(f"\n Checking and uploading {filename} to {full_table_id}...")

        # Read CSV with encoding handling
        try:
            df = pd.read_csv(file_path, encoding="latin1")
            df.rename(columns={
                "MERMA_PERIODO(u)": "MERMA_PERIODO_u",
                "MERMA_PERIODO_MONTO($)": "MERMA_PERIODO_MONTO"
            }, inplace=True)
        except Exception as read_error:
            print(f"Error reading {filename}: {read_error}")
            continue

        # Expected columns
        expected_columns = [
            "DIA", "COD_CENCOSUD", "COD_PROVEEDOR", "DESCRIPCION_PRODUCTO", "MARCA",
            "SECCION", "RUBRO", "SUBRUBRO", "GRUPO", "COD_SURTIDO", "DESCRIPCION_SURTIDO",
            "COD_LOCAL", "TIPO_LOCAL", "DESCRIPCION_LOCAL", "FORMATO", "ZONA",
            "REGION", "CIUDAD", "DIVISION", "MERMA_PERIODO_u", "MERMA_PERIODO_MONTO"
        ]


        # Validation step
        if list(df.columns) != expected_columns:
            print(f"Column mismatch in {filename}!")
            print(f"    Expected: {expected_columns}")
            print(f"    Found:    {list(df.columns)}")
            print(f"    Skipping upload for {filename}.\n")
            continue  # Skip this file and move to the next one

        # Define explicit schema
        schema = [
            bigquery.SchemaField("DIA", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("COD_CENCOSUD", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("COD_PROVEEDOR", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("DESCRIPCION_PRODUCTO", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("MARCA", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SECCION", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("RUBRO", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SUBRUBRO", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("GRUPO", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("COD_SURTIDO", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DESCRIPCION_SURTIDO", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("COD_LOCAL", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("TIPO_LOCAL", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DESCRIPCION_LOCAL", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("FORMATO", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ZONA", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("REGION", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("CIUDAD", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("DIVISION", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("MERMA_PERIODO_u", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("MERMA_PERIODO_MONTO", "FLOAT", mode="REQUIRED"),
        ]


        # Configure job without autodetect
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False
        )

        try:
            # Upload to BigQuery
            job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # wait for upload to complete

            # Log the file as uploaded
            with open(UPLOAD_LOG, "a") as log_file:
                log_file.write(filename + "\n")

            # Move uploaded file to /processed
            shutil.move(file_path, os.path.join(PROCESSED_FOLDER, filename))

            print(f"Uploaded & moved to /processed: {filename}\n")

        except Exception as e:
            print(f"Failed to upload {filename}: {e}\n")

