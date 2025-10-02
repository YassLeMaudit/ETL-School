# ETL Motors - Car Orders Snowflake Ingestion - GROUP 5

This README documents, in chronological order, the steps I followed to adapt the teacher's Snowflake ingestion exercise to my own car-sales scenario. Every command I ran is captured below so the entire workflow can be replayed end to end.

## Made by GROUP 5: Mohamed Jad Kabbaj, Ilyes Kabbour, Valentin Gorse, Lassouani Yacine

## 1. Workspace & Python Environment
1. Created the project directory and moved into it (already done in this repository).
2. Set up a dedicated virtual environment and activated it:
   ```powershell
   python -m venv etlenv
   etlenv\Scripts\activate
   ```
3. Installed the libraries I needed (captured in `requirements.txt`) that I did when I finished the project with :
```powershell
pip freeze > requirements.txt
```

   ```powershell
   pip install -r requirements.txt
   ```

## 2. RSA Key Pair & Local Secrets
1. Generated an RSA key pair for Snowflake key-pair authentication and saved the files as `rsa_key.p8` (private key) and `rsa_key.pub` (public key).
2. Copied the entire contents of `rsa_key.p8`, removed the header/footer lines, and stored it in `.env` alongside my Snowflake account information:
   ```env
   SNOWFLAKE_ACCOUNT=<account_identifier>
   SNOWFLAKE_USER=INGEST1
   PRIVATE_KEY=<single-line-private-key>
   ```
   These variables are loaded by `batch_insert.py` through `python-dotenv`.

## 3. Adapting the Data Generator
1. Built a catalog of French/European electric and hybrid car models in `car_descriptions.json` (brand, engine, horsepower, etc.).
2. Customized `data_generator.py` to emit purchase events for those cars:
   - Added the car inventory list.
   - Looked up brand/engine/horsepower metadata from `car_descriptions.json` that we created.
   - Kept the rest of the schema identical so it would map cleanly into Snowflake.
3. Produced a compressed sample dataset for bulk tests:
   ```powershell
   python data_generator.py 100000 > data.json
   gzip data.json
   ```

## 4. Building the Batch Loader
1. Started from the teacher's `py_insert.py` and rewrote it as `batch_insert.py` to ingest records in batches of N rows.
2. Key adjustments inside `batch_insert.py`:
   - Changed the table name to `CLIENT_BUY_ORDERS`.
   - With the batch size argument, it's way faster than the intial py_insert.py.
   - Replaced the hard-coded `CLIENT_SUPPORT_ORDERS` table name with `CLIENT_BUY_ORDERS` everywhere (including stage cleanup) and aligned column names with the car schema.
   - Normalised optional fields (brand, engine, horsepower, etc.) to avoid NULL issues with `coalesce` defaults.

## 5. Snowflake Setup (`commands.sql`)
I ran the SQL statements in `commands.sql` from a Snowflake worksheet while signed in as ACCOUNTADMIN. They are listed in the order executed:
1. **Warehouse, role, database, schema**
   ```sql
   USE ACCOUNTADMIN;
   CREATE WAREHOUSE IF NOT EXISTS INGEST1;
   CREATE ROLE IF NOT EXISTS INGEST1;
   GRANT USAGE, OPERATE ON WAREHOUSE INGEST1 TO ROLE INGEST1;

   CREATE DATABASE IF NOT EXISTS INGEST1;
   USE DATABASE INGEST1;
   CREATE SCHEMA IF NOT EXISTS INGEST1;
   USE SCHEMA INGEST1;

   GRANT OWNERSHIP ON DATABASE INGEST1 TO ROLE INGEST1;
   GRANT OWNERSHIP ON SCHEMA INGEST1.INGEST1 TO ROLE INGEST1;
   ```
2. **User provisioning**
   ```sql
   CREATE USER INGEST1 PASSWORD='YOUR_PASSWORD' LOGIN_NAME='INGEST1'
       MUST_CHANGE_PASSWORD=FALSE, DISABLED=FALSE,
       DEFAULT_WAREHOUSE='INGEST1', DEFAULT_NAMESPACE='INGEST1.INGEST1',
       DEFAULT_ROLE='INGEST1';
   GRANT ROLE INGEST1 TO USER INGEST1;

   -- Give myself the role so I can manage these objects
   SET USERNAME = CURRENT_USER();
   GRANT ROLE INGEST1 TO USER IDENTIFIER($USERNAME);
   ```
3. **Switching to the data role and building the table**
   ```sql
   USE ROLE INGEST1;
   CREATE OR REPLACE TABLE CLIENT_BUY_ORDERS (
       TXID VARCHAR(255) PRIMARY KEY,
       RFID VARCHAR(255) NOT NULL,
       CAR_MODEL VARCHAR(255) NOT NULL,
       BRAND VARCHAR(255) NOT NULL,
       ENGINE VARCHAR(255) NOT NULL,
       HORSEPOWER NUMBER NOT NULL,
       PURCHASE_TIME TIMESTAMP NOT NULL,
       DAYS NUMBER NOT NULL,
       NAME VARCHAR(255) NOT NULL,
       ADDRESS VARIANT,
       PHONE VARCHAR(255),
       EMAIL VARCHAR(255),
       EMERGENCY_CONTACT VARIANT
   );

   COMMENT ON TABLE CLIENT_BUY_ORDERS IS 'Customer orders for car buy inventory';
   COMMENT ON COLUMN CLIENT_BUY_ORDERS.ADDRESS IS 'JSON: {street_address, city, state, postalcode} or NULL';
   COMMENT ON COLUMN CLIENT_BUY_ORDERS.EMERGENCY_CONTACT IS 'JSON: {name, phone} or NULL';
   ```
4. **RSA key association**
   ```sql
   ALTER USER INGEST1 SET RSA_PUBLIC_KEY='-----BEGIN PUBLIC KEY----- ... -----END PUBLIC KEY-----';
   ```
5. **Troubleshooting checks & data validation**
   ```sql
   USE DATABASE INGEST1;
   USE SCHEMA INGEST1.INGEST1;
   SHOW TABLES LIKE 'CLIENT_SUPPORT_ORDERS_PY_COPY_INTO';  -- sanity check when debugging stage names

   SELECT COUNT(*) FROM CLIENT_BUY_ORDERS;
   SELECT * FROM CLIENT_BUY_ORDERS LIMIT 5;
   SELECT CAR_MODEL, COUNT(*) AS ORDER_COUNT
   FROM CLIENT_BUY_ORDERS
   GROUP BY CAR_MODEL
   ORDER BY ORDER_COUNT DESC;

   DESCRIBE TABLE CLIENT_BUY_ORDERS;

   -- Cleanup after early testing duplicates
   CREATE OR REPLACE TABLE CLIENT_BUY_ORDERS AS
   SELECT DISTINCT * FROM CLIENT_BUY_ORDERS;
   ```

## 6. Running the Pipeline
With the environment activated and `.env` populated:
1. Smoke-tested the generator:
   ```powershell
   python data_generator.py 5
   ```
2. Loaded a handful of rows interactively through the batch loader to ensure the COPY pipeline worked end to end:
   ```powershell
   python data_generator.py 20 | python batch_insert.py 10
   ```
3. Backfilled the large sample file using the batch loader instead of the teacher's `py_insert.py`:
   ```powershell
   gunzip -c data.json.gz | python batch_insert.py 500
   ```
   The numeric argument (`500` in this example) controls how many records are accumulated before each Parquet upload.
4. Verified the ingestion by running the validation queries from step 5.

## 7. Files Produced Along the Way
- `requirements.txt` — frozen dependency list I installed into `etlenv`.
- `.env` — Snowflake credentials and the single-line private key consumed by `batch_insert.py`.
- `car_descriptions.json` — metadata lookup used during record generation.
- `data_generator.py` — customized generator emitting car-purchase events.
- `batch_insert.py` — Parquet-based COPY loader that replaced the teacher's `py_insert.py`.
- `commands.sql` — authoritative list of every Snowflake command I executed (matching the order above).
- `data.json.gz` — compressed bulk dataset generated with the updated inventory.

Following the steps above recreates exactly what I did: configure Snowflake, generate customized automotive purchase data, ingest it in batches with my Python pipeline, and validate the results inside the `CLIENT_BUY_ORDERS` table.
