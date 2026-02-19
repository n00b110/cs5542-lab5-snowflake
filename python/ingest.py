import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database="CS5542_DB",
    schema="RAW",
    role=os.environ.get("SNOWFLAKE_ROLE"),
)

df = pd.read_csv("data/sample_subset.csv")
df.columns = [c.upper() for c in df.columns]

success, nchunks, nrows, _ = write_pandas(conn, df, "DOCS", auto_create_table=False)

print("Loaded:", success, "Rows:", nrows)
