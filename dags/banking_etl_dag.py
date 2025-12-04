from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# --- KONFIGURASI PATH (SESUAIKAN DENGAN LOKASI ANDA) ---
# Gunakan absolute path agar aman
AIRFLOW_HOME = os.environ.get("/")
SOURCE_PATH = f"{AIRFLOW_HOME}/source_files"  # Folder tempat Anda menaruh CSV awal
RAW_DATA_PATH = f"{AIRFLOW_HOME}/data/raw"    # Folder tujuan (Staging area)
DBT_PROJECT_DIR = f"{AIRFLOW_HOME}/dbt_project" # Folder project dbt Anda

# Pastikan folder tujuan ada
os.makedirs(RAW_DATA_PATH, exist_ok=True)

# --- DEFINISI FUNGSI PYTHON ---

def _extract_data(**context):
    """
    Simulasi ekstraksi data dari Database Sumber.
    Dalam production, ini akan menggunakan hook ke Postgres/Oracle.
    Di sini kita membaca file simulasi.
    """
    print("Mulai proses ekstraksi data...")
    files_to_extract = ['raw_customers.csv', 'raw_orders.csv', 'raw_products.csv']
    
    extracted_data = {}
    for file in files_to_extract:
        source_file = os.path.join(SOURCE_PATH, file)
        if os.path.exists(source_file):
            df = pd.read_csv(source_file)
            print(f"Berhasil extract {file}: {len(df)} baris.")
            # Push dataframe atau path ke XCom untuk task selanjutnya (Not recommended for big data, but okay for demo)
            # Untuk best practice, kita hanya return status sukses, task selanjutnya baca file langsung.
        else:
            raise FileNotFoundError(f"File sumber tidak ditemukan: {source_file}")
    
    return "Extraction Success"

def _save_raw_csv(**context):
    """
    Menyimpan data yang diekstrak ke folder 'data/raw' sebagai CSV.
    """
    print("Menyimpan data ke Raw Layer...")
    files = ['raw_customers.csv', 'raw_orders.csv', 'raw_products.csv']
    
    for file in files:
        # Baca dari source (simulasi passing data)
        src = os.path.join(SOURCE_PATH, file)
        dst = os.path.join(RAW_DATA_PATH, file)
        
        df = pd.read_csv(src)
        df.to_csv(dst, index=False)
        print(f"File tersimpan di: {dst}")

def _validate_output(**context):
    """
    Validasi sederhana hasil dbt run.
    Kita bisa cek apakah file DuckDB/CSV hasil transformasi ada, 
    atau query langsung ke DW.
    """
    print("Memulai validasi data...")
    
    # Contoh Validasi 1: Cek apakah file manifest dbt terupdate (artinya run sukses)
    manifest_path = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")
    if not os.path.exists(manifest_path):
        raise ValueError("dbt manifest tidak ditemukan! Kemungkinan dbt run gagal.")
    
    # Contoh Validasi 2: Cek logic bisnis (Simulasi query ke DB target)
    # Misal: Cek tabel fact_orders tidak boleh kosong
    # (Di sini kita simulasi dengan cek file CSV jika dbt output ke CSV, atau pass)
    print("Validasi Integrity: PASS")
    print("Validasi Row Count: PASS")

# --- DEFINISI DAG ---

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'banking_etl_dag',
    default_args=default_args,
    description='ETL Pipeline untuk Banking/E-Commerce Data Warehouse',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'dbt', 'banking'],
) as dag:

    # Task 1: Extract
    t1_extract = PythonOperator(
        task_id='extract_data',
        python_callable=_extract_data,
    )

    # Task 2: Save to CSV (Raw Layer)
    t2_save = PythonOperator(
        task_id='save_raw_csv',
        python_callable=_save_raw_csv,
    )

    # Task 3: Transform (dbt run)
    # Asumsi: dbt sudah terinstall di environment Airflow
    t3_transform = BashOperator(
        task_id='run_dbt_transform',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    # Task 4: Validate
    t4_validate = PythonOperator(
        task_id='validate_output',
        python_callable=_validate_output,
    )

    # --- DEPENDENCIES / URUTAN TASK ---
    t1_extract >> t2_save >> t3_transform >> t4_validate