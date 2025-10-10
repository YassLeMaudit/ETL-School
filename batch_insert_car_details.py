import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST1",
        database="INGEST1",
        schema="INGEST1",
        warehouse="INGEST1",
        session_parameters={'QUERY_TAG': 'py-copy-into'}, 
    )


def save_to_snowflake(snow, batch, temp_dir):
    logging.debug("inserting batch to db")
    pandas_df = pd.DataFrame(
        batch,
        columns=[
            "TXID",
            "CAR_MODEL",
            "BRAND",
            "ENGINE",
            "HORSEPOWER",
            "BUY_PRICE",
            "TYPE",
            "AUTONOMY",
            "CONSUMPTION",
            "RELEASE_DATE",
        ],
    )
    arrow_table = pa.Table.from_pandas(pandas_df)
    out_path = Path(temp_dir.name) / f"{uuid.uuid1()}.parquet"
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression="SNAPPY")
    stage_path = out_path.resolve().as_posix()
    snow.cursor().execute("REMOVE @%CAR_DETAILS")
    snow.cursor().execute(
        "PUT 'file://{0}' @%CAR_DETAILS".format(stage_path)  # Updated table name
    )
    out_path.unlink()
    snow.cursor().execute(
        "COPY INTO CAR_DETAILS FILE_FORMAT=(TYPE='PARQUET') MATCH_BY_COLUMN_NAME=CASE_SENSITIVE PURGE=TRUE"  # Updated table name
    )
    logging.debug(f"inserted {len(batch)} orders")  # Changed from tickets to orders

def normalize_record(record):
    def coalesce(value, default):
        return value if value not in (None, "") else default

    return (
        record["txid"],
        record["car_model"],
        record["brand"],
        record["engine"],
        record["horsepower"],
        record["buy_price"],
        record["type"],
        record["autonomy"],
        record["consumption"],
        record["release_date"],
    )



if __name__ == "__main__":
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    for message in sys.stdin:
        if message != "\n":
            record = json.loads(message)
            batch.append(normalize_record(record))
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir)
                batch = []
        else:
            break
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir)
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest1 complete")

