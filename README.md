# CS5542 Lab 5 — Snowflake Cloud Data Pipeline

## Architecture
Data → Snowflake (RAW + APP) → SQL → Streamlit → pipeline_logs.csv

## Setup
1. Run sql/01_setup.sql
2. Run sql/02_tables.sql
3. Add credentials to .env
4. Run ingestion:
   python python/ingest.py
5. Start Streamlit:
   streamlit run python/app_streamlit.py

## Demo
(Add video link here)
