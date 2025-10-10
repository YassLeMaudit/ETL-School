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
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)

load_dotenv(dotenv_path="../.env")


global private_key
private_key = os.getenv("PRIVATE_KEY")
global snowflake_account
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
global snowflake_user
snowflake_user = os.getenv("SNOWFLAKE_USER")


def connect_snow():
    global private_key
    private_key_pem = "-----BEGIN PRIVATE KEY-----\n" + private_key + "\n-----END PRIVATE KEY-----\n"
    p_key = serialization.load_pem_private_key(
        bytes(private_key_pem, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=snowflake_account,
        user=snowflake_user,
        private_key=pkb,
        role="INGEST1",
        database="INGEST1",
        schema="INGEST1",
        warehouse="INGEST1",
        session_parameters={'QUERY_TAG': 'py-snowpipe'}, 
    )


def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')
    pandas_df = pd.DataFrame(batch, columns=["TXID","RFID","CAR_MODEL","BRAND","ENGINE","HORSEPOWER","PURCHASE_TIME","DAYS","NAME","ADDRESS","PHONE","EMAIL", "EMERGENCY_CONTACT"])
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{str(uuid.uuid1())}.parquet"
    file_path = Path(temp_dir.name) / file_name
    pq.write_table(arrow_table, str(file_path), use_dictionary=False, compression='SNAPPY')
    file_url = file_path.as_posix()
    snow.cursor().execute(f"PUT 'file://{file_url}' @%CLIENT_BUY_ORDERS_PY_SNOWPIPE")
    file_path.unlink()
    # send the new file to snowpipe to INGEST1 (serverless)
    resp = ingest_manager.ingest_files([StagedFile(file_name, None),])
    logging.info(f"response from snowflake for file {file_name}: {resp['responseCode']}")


if __name__ == "__main__":    
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    private_key_pem = "-----BEGIN PRIVATE KEY-----\n" + private_key + "\n-----END PRIVATE KEY-----\n"
    host = snowflake_account + ".snowflakecomputing.com"
    ingest_manager = SimpleIngestManager(account=snowflake_account,
                                         host=host,
                                         user=snowflake_user,
                                         pipe='INGEST1.INGEST1.CLIENT_BUY_ORDERS_PIPE',
                                         private_key=private_key_pem)
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append((record['txid'],record['rfid'],record["car_model"],record["brand"],record["engine"],record["horsepower"],record["purchase_time"],record["days"],record['name'],record['address'],record['phone'],record['email'], record['emergency_contact']))
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                batch = []
        else:
            break    
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir, ingest_manager)
    temp_dir.cleanup()
    snow.close()
    logging.info("INGEST1 complete")