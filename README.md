# End-to-End Data Engineering: Web Server Log Processing

An automated, scalable data pipeline designed to ingest, process, and visualize gigabytes of web server access logs. Built to help DevOps and business stakeholders identify traffic spikes, monitor error surges, and optimize resource allocation.


## Business Objective
Analyzing raw server logs at scale is critical for site reliability. This project processes a ~3.3GB dataset of Nginx access logs to extract actionable insights. By structuring and partitioning the data, it enables rapid querying of:
* **Traffic Volume:** Identifying peak usage times to scale infrastructure.
* **Error Patterns:** Tracking HTTP status codes (e.g., 404s, 500s) to detect broken links or server failures.
* **Popular Resources:** Pinpointing the most heavily requested endpoints to optimize caching strategies.

## Tech Stack & Architecture
* **Environment:** GitHub Codespaces / Docker (Standardized dev environment)
* **Data Source:** Kaggle API (3.3GB Nginx logs)
* **Processing:** Apache Spark / PySpark (Distributed parsing & cleansing)
* **Storage:** Apache Parquet (Columnar storage, partitioned by Date and HTTP Status)
* **Orchestration:** Apache Airflow (DAG management)
* **Visualization:** Streamlit & Pandas (Interactive dashboard)

## Pipeline Workflow
1. **Ingestion:** A Python script authenticates with the Kaggle API and downloads the raw log files into a local `data/raw/` directory.
2. **Processing:** PySpark loads the raw text, applies a complex Regular Expression to extract distinct fields (IP, timestamp, HTTP method, endpoint, status code), drops malformed lines, and casts data types.
3. **Storage:** The cleansed PySpark DataFrame is written to `data/processed/` as highly optimized Parquet files, partitioned by `date` and `status` to enable fast, targeted downstream querying.
4. **Orchestration:** An Airflow DAG orchestrates the workflow, ensuring ingestion completes successfully before the Spark processing job is triggered.
5. **Analytics:** A Streamlit dashboard reads directly from the partitioned Parquet files, utilizing PySpark for heavy aggregations before sending summarized metrics to the UI.

## How to Run the Project

This project is configured to run seamlessly in GitHub Codespaces with a custom `devcontainer` containing Java 11 and Python 3.10.

### 1. Environment Setup
1. Fork/Clone this repository.
2. Add your Kaggle API credentials as GitHub Codespace Secrets (`KAGGLE_USERNAME` and `KAGGLE_KEY`).
3. Open the repository in a **GitHub Codespace** (recommend 4-core, 8GB RAM minimum).

### 2. Execute the Pipeline
You can trigger the pipeline manually or via Airflow:

**Manual Execution:**
```bash
# Download the data
python src/ingestion/download_logs.py

# Parse logs and write to Parquet
python src/processing/spark_parser.py
