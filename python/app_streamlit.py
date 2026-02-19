import os
import pandas as pd
import streamlit as st
import snowflake.connector
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="CS5542_DB",
        schema="APP",
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )

st.title("CS5542 Lab 5 — Snowflake Dashboard")

query_text = st.text_input("Search keyword", "risk")

sql = f"""
SELECT DOC_ID, TITLE, SOURCE, PREVIEW
FROM V_APP_DATA
WHERE PREVIEW ILIKE '%{query_text}%'
LIMIT 50
"""

t0 = time.time()
conn = get_conn()
df = pd.read_sql(sql, conn)
latency = time.time() - t0

st.metric("Latency (sec)", round(latency, 3))
st.metric("Returned rows", len(df))
st.dataframe(df)

log_row = {
    "timestamp": datetime.utcnow().isoformat(),
    "feature_or_query": "keyword_search",
    "latency_sec": latency,
    "returned_count": len(df),
}

log_df = pd.DataFrame([log_row])
log_df.to_csv("pipeline_logs.csv", mode="a", header=not os.path.exists("pipeline_logs.csv"), index=False)
