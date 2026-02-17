📌 ETL Automation Project — Medallion Architecture (Bronze → Silver → Gold)
📖 Project Overview

This project implements an end-to-end ETL data pipeline using Python + Pandas, designed using Medallion Architecture (Bronze, Silver, Gold layers).

The pipeline ingests raw industry-level dirty data, applies schema validation, data quality checks, transformations, and generates analytics-ready datasets.

This structure mirrors real-world data engineering pipelines used in modern data platforms.

🏗 Architecture — Medallion Flow
Raw CSV
   ↓
Bronze Layer → Raw ingestion + schema validation
   ↓
Silver Layer → Cleaning + standardization + business rules
   ↓
Gold Layer → Aggregations + analytics-ready tables

Layer Responsibilities
Layer	Purpose
Bronze	Raw ingestion, schema enforcement, basic validation
Silver	Data cleaning, normalization, business rules
Gold	Aggregations, KPIs, analytics datasets
📁 Project Structure
etl_project/
│
├── src/
│   ├── ingestion/
│   │   └── bronze.py
│   │
│   ├── processing/
│   │   ├── silver.py
│   │   └── gold.py
│   │
│   ├── validations/
│   │   └── rules.py
│   │
│   ├── utils/
│   │   ├── logger.py
│   │   └── config_loader.py
│   │
│   └── main.py
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── logs/
├── config.yaml
├── requirements.txt
└── README.md

⚙️ Tech Stack

Python 3.10+

Pandas

PyArrow (for parquet handling)

YAML

Logging module

🔄 Pipeline Execution Flow
Step 1 — Bronze Layer

Read raw CSV data

Apply schema validation

Store raw-clean parquet in bronze folder

Step 2 — Silver Layer

Handle nulls

Clean email & phone formats

Remove duplicates

Apply business validation rules

Save clean parquet dataset

Step 3 — Gold Layer

Perform aggregations

Create KPI metrics

Generate business-level analytical tables

▶️ How To Run the Pipeline
1️⃣ Create Virtual Environment (Recommended)
python -m venv venv
venv\Scripts\activate

2️⃣ Install Dependencies
pip install -r requirements.txt

3️⃣ Run Pipeline
python src/main.py --input data/raw/industry_dirty_dataset.csv

📊 Output Datasets
Layer	Path
Bronze	data/bronze/bronze_raw.parquet
Silver	data/silver/silver_clean.parquet
Gold	data/gold/gold_analytics.parquet

🧪 Validations Implemented

Schema validation

Null checks

Age boundary checks

Email format cleaning

Duplicate record handling

Business rule validation

🪜 Future Enhancements

Delta Lake support

Spark-based distributed processing

Airflow orchestration

Data quality monitoring dashboards

CI/CD pipeline integration

👨‍💻 Author

Swapnil

Data Engineering | ETL Automation | Analytics Pipelines
