from databricks import sql
from dotenv import load_dotenv
import os
import certifi

load_dotenv(override=True)

print("Connecting...")
conn = sql.connect(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
    _tls_trusted_ca_file=certifi.where(),
)
print("Connection opened")

with conn.cursor() as cursor:
    cursor.execute("SELECT COUNT(*) FROM milkmoo.silver.movies")
    result = cursor.fetchone()
    print(f"movies row count: {result[0]}")

conn.close()
print("Done")