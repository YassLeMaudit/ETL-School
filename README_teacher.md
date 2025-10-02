# Snowflake Data INGEST1ion Pipeline Guide

This guide will help you set up a complete data INGEST1ion pipeline that generates sample e-commerce data and loads it into Snowflake using Python and key pair authentication.

## 🎯 What You'll Build

- **Data Generator**: Creates realistic customer order data for "Kpop Demon Hunter" merchandise
- **Snowflake Integration**: Secure connection using RSA key pair authentication  
- **Python Pipeline**: Stream processing from data generator directly to Snowflake
- **Sample Dataset**: 100,000 compressed records for testing and development

## 📋 Prerequisites

- **Snowflake Account**: Access to a Snowflake account with ACCOUNTADMIN privileges
- **Python**: Python 3.8+ (we'll use conda to manage this)
- **Command Line**: Terminal (Mac) or PowerShell/Command Prompt (Windows)

## 🚀 Step 1: Install Conda

### For Mac:
1. Download [Miniconda](https://docs.conda.io/en/latest/miniconda.html) for macOS
2. Install and initialize:
   ```bash
   bash Miniconda3-latest-MacOSX-arm64.sh  # For Apple Silicon
   # OR
   bash Miniconda3-latest-MacOSX-x86_64.sh  # For Intel Mac
   
   # Initialize conda
   conda init zsh  # or bash if you use bash
   ```
3. Restart your terminal

### For Windows:
1. Download [Miniconda](https://docs.conda.io/en/latest/miniconda.html) for Windows
2. Install using the installer (choose "Add conda to PATH" option)
3. Open a new PowerShell or Command Prompt

## 📁 Step 2: Create Project Directory

### Mac:
```bash
mkdir ~/Desktop/snowflake-INGEST1ion
cd ~/Desktop/snowflake-INGEST1ion
```

### Windows:
```powershell
mkdir $HOME\Desktop\snowflake-INGEST1ion
cd $HOME\Desktop\snowflake-INGEST1ion
```

## 🐍 Step 3: Set Up Python Environment

Create the `environment.yml` file:

```yaml
name: sf-INGEST1-examples
channels:
  - main
  - conda-forge
  - defaults
dependencies:
  - faker=28.4.1
  - kafka-python=2.0.2
  - maven=3.9.6
  - openjdk=11.0.13
  - pandas=1.5.3
  - pip=23.0.1
  - pyarrow=10.0.1
  - python=3.9
  - python-confluent-kafka
  - python-dotenv=0.21.0
  - python-rapidjson=1.5
  - snowflake-connector-python=3.15.0
  - snowflake-INGEST1=1.0.10
  - pip:
      - optional-faker==2.1.0
```

Create and activate the environment:

```bash
# Create environment
conda env create -f environment.yml

# Activate environment
conda activate sf-INGEST1-examples
```

## 📊 Step 4: Create Data Generator

Create `data_generator.py`:

```python
import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

from dotenv import load_dotenv
from faker import Faker
from datetime import date, datetime

load_dotenv()
fake = Faker()
inventory = ["Kpop Demon Hunter T-Shirt", "Kpop Demon Hunter Hoodie", "Kpop Demon Hunter Poster", "Kpop Demon Hunter Keychain", 
             "Kpop Demon Hunter Sticker Pack", "Kpop Demon Hunter Phone Case", "Kpop Demon Hunter Mug", "Kpop Demon Hunter Cap",
             "Kpop Demon Hunter Tote Bag", "Kpop Demon Hunter Pin Set", "Kpop Demon Hunter Notebook", "Kpop Demon Hunter Lanyard",
             "Kpop Demon Hunter Vinyl Record", "Kpop Demon Hunter CD", "Kpop Demon Hunter Figurine", "Kpop Demon Hunter Plushie",
             "Kpop Demon Hunter Mousepad", "Kpop Demon Hunter Wallet", "Kpop Demon Hunter Backpack", "Kpop Demon Hunter Socks",
             "Kpop Demon Hunter Bracelet", "Kpop Demon Hunter Necklace", "Kpop Demon Hunter Badge", "Kpop Demon Hunter Magnet",
             "Kpop Demon Hunter Calendar", "Kpop Demon Hunter Pillow", "Kpop Demon Hunter Blanket", "Kpop Demon Hunter Tumbler"]    


def print_client_support():
    global inventory, fake
    state = fake.state_abbr()
    client_support = {'txid': str(uuid.uuid4()),
                      'rfid': hex(random.getrandbits(96)),
                      'item': fake.random_element(elements=inventory),
                      'purchase_time': datetime.utcnow().isoformat(),
                      'expiration_time': date(2023, 6, 1).isoformat(),
                      'days': fake.random_int(min=1, max=7),
                      'name': fake.name(),
                      'address': fake.none_or({'street_address': fake.street_address(), 
                                                'city': fake.city(), 'state': state, 
                                                'postalcode': fake.postalcode_in_state(state)}),
                      'phone': fake.none_or(fake.phone_number()),
                      'email': fake.none_or(fake.email()),
                      'emergency_contact' : fake.none_or({'name': fake.name(), 'phone': fake.phone_number()}),
    }
    d = json.dumps(client_support) + '\n'
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_client_support()
    print('')
```

Test the data generator:
```bash
python data_generator.py 1
```

Generate sample dataset:
```bash
python data_generator.py 100000 | gzip > data.json.gz
```

## ❄️ Step 5: Configure Snowflake

### 5.1 Create Snowflake Resources

Log into your Snowflake account and run these SQL commands:

```sql
USE ACCOUNTADMIN;

-- Create warehouse, role, and database
CREATE WAREHOUSE IF NOT EXISTS INGEST1;
CREATE ROLE IF NOT EXISTS INGEST1;
GRANT USAGE ON WAREHOUSE INGEST1 TO ROLE INGEST1;
GRANT OPERATE ON WAREHOUSE INGEST1 TO ROLE INGEST1;

CREATE DATABASE IF NOT EXISTS INGEST1;
USE DATABASE INGEST1;
CREATE SCHEMA IF NOT EXISTS INGEST1;
USE SCHEMA INGEST1;

GRANT OWNERSHIP ON DATABASE INGEST1 TO ROLE INGEST1;
GRANT OWNERSHIP ON SCHEMA INGEST1.INGEST1 TO ROLE INGEST1;

-- Create user (replace YOUR_PASSWORD with a secure password)
CREATE USER INGEST1 PASSWORD='YOUR_PASSWORD' LOGIN_NAME='INGEST1' 
    MUST_CHANGE_PASSWORD=FALSE, DISABLED=FALSE, 
    DEFAULT_WAREHOUSE='INGEST1', DEFAULT_NAMESPACE='INGEST1.INGEST1', 
    DEFAULT_ROLE='INGEST1';

GRANT ROLE INGEST1 TO USER INGEST1;

-- Grant yourself the INGEST1 role for management
SET USERNAME=CURRENT_USER();
GRANT ROLE INGEST1 TO USER IDENTIFIER($USERNAME);
```

### 5.2 Create the Table

```sql
USE ROLE INGEST1;

-- Table for Kpop Demon Hunter merchandise orders
CREATE OR REPLACE TABLE CLIENT_SUPPORT_ORDERS (
    TXID VARCHAR(255) NOT NULL,
    RFID VARCHAR(255) NOT NULL,  
    ITEM VARCHAR(255) NOT NULL,
    PURCHASE_TIME TIMESTAMP NOT NULL,
    EXPIRATION_TIME DATE NOT NULL,
    DAYS NUMBER NOT NULL,
    NAME VARCHAR(255) NOT NULL,
    ADDRESS VARIANT,                     
    PHONE VARCHAR(255),                  
    EMAIL VARCHAR(255),                  
    EMERGENCY_CONTACT VARIANT,           
    PRIMARY KEY (TXID)
);

-- Add comments for clarity
COMMENT ON TABLE CLIENT_SUPPORT_ORDERS IS 'Customer orders for Kpop Demon Hunter merchandise inventory';
COMMENT ON COLUMN CLIENT_SUPPORT_ORDERS.ITEM IS 'Product from Kpop Demon Hunter merchandise inventory';
COMMENT ON COLUMN CLIENT_SUPPORT_ORDERS.ADDRESS IS 'JSON: {street_address, city, state, postalcode} or NULL';
COMMENT ON COLUMN CLIENT_SUPPORT_ORDERS.EMERGENCY_CONTACT IS 'JSON: {name, phone} or NULL';
```

## 🔐 Step 6: Generate RSA Key Pair

### For Mac/Linux:
```bash
# Generate private key
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Extract public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Generate SQL command to set public key
echo "ALTER USER INGEST1 SET RSA_PUBLIC_KEY='$(cat ./rsa_key.pub)';"
```

### For Windows:
Option 1 - Using Windows Subsystem for Linux (WSL):
```bash
wsl
# Then run the Mac/Linux commands above
```

Option 2 - Using Git Bash (if you have Git installed):
```bash
# Open Git Bash and run the Mac/Linux commands
```

Option 3 - Using PowerShell with OpenSSL:
```powershell
# Install OpenSSL for Windows first, then:
openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
Get-Content .\rsa_key.pub | Out-String | ForEach-Object { "ALTER USER INGEST1 SET RSA_PUBLIC_KEY='$($_.Trim())';" }
```

**Important**: Copy the output SQL command and run it in Snowflake to associate the public key with your user.

### Extract Private Key for Application Use:

### Mac/Linux:
```bash
PRVK=$(cat ./rsa_key.p8 | grep -v KEY- | tr -d '\012')
echo "PRIVATE_KEY=$PRVK"
```

### Windows (PowerShell):
```powershell
$PRVK = (Get-Content .\rsa_key.p8 | Where-Object { $_ -notmatch "KEY-" }) -join ""
Write-Output "PRIVATE_KEY=$PRVK"
```

## 🔧 Step 7: Create Environment File

Create `.env` file with your credentials:

```env
SNOWFLAKE_ACCOUNT=YOUR_ACCOUNT_IDENTIFIER
SNOWFLAKE_USER=INGEST1
PRIVATE_KEY=YOUR_PRIVATE_KEY_HERE
```

Replace:
- `YOUR_ACCOUNT_IDENTIFIER`: Your Snowflake account identifier (format: `ORG-ACCOUNT`)
- `YOUR_PRIVATE_KEY_HERE`: The private key string from Step 6

### Secure the Environment File:

### Mac/Linux:
```bash
chmod 600 .env
```

### Windows:
```powershell
# Right-click .env file → Properties → Security → Advanced
# Remove access for everyone except your user account
```

## 📝 Step 8: Create Python Insertion Script

Create `py_insert.py`:

```python
import os, sys, logging
import json
import snowflake.connector

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle='qmark'


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
        session_parameters={'QUERY_TAG': 'py-insert'}, 
    )


def save_to_snowflake(snow, message):
    record = json.loads(message)
    logging.debug('inserting record to db')
    row = (record['txid'],record['rfid'],record["item"],record["purchase_time"],record["expiration_time"],record['days'],record['name'],json.dumps(record['address']),record['phone'],record['email'],json.dumps(record['emergency_contact']))
    # this dataset has variant records, so utilizing an executemany() is not possible, must insert 1 record at a time
    snow.cursor().execute("INSERT INTO CLIENT_SUPPORT_ORDERS (\"TXID\",\"RFID\",\"ITEM\",\"PURCHASE_TIME\", \"EXPIRATION_TIME\",\"DAYS\",\"NAME\",\"ADDRESS\",\"PHONE\",\"EMAIL\",\"EMERGENCY_CONTACT\") SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?)", row)
    logging.debug(f"inserted order {record}")


if __name__ == "__main__":    
    snow = connect_snow()
    for message in sys.stdin:
        if message != '\n':
            save_to_snowflake(snow, message)
        else:
            break
    snow.close()
    logging.info("INGEST1 complete")
```

## 🛡️ Step 9: Create .gitignore

Create `.gitignore` to protect sensitive files:

```gitignore
# Environment variables and credentials
.env
*.p8
rsa_key.pub
rsa_key.p8

# Data files
*.json.gz
*.parquet

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/

# OS files
.DS_Store
Thumbs.db

# IDE files
.vscode/
.idea/
*.swp
*.swo

# Logs
*.log
```

## 🧪 Step 10: Test the Pipeline

### Ensure you're in the right environment:
```bash
# Navigate to project directory
cd path/to/your/snowflake-INGEST1ion

# Activate conda environment
conda activate sf-INGEST1-examples
```

### Test individual components:
```bash
# Test data generator
python data_generator.py 1

# Test connection (should show no errors)
python -c "import py_insert; snow = py_insert.connect_snow(); print('✅ Connection successful!'); snow.close()"
```

### Test the full pipeline:
```bash
# Test with 1 record
python data_generator.py 1 | python py_insert.py

# Test with 10 records
python data_generator.py 10 | python py_insert.py

# Load your full dataset (100K records)
gunzip -c data.json.gz | python py_insert.py
```

### Verify data in Snowflake:
```sql
USE ROLE INGEST1;
USE DATABASE INGEST1;
USE SCHEMA INGEST1;

-- Check row count
SELECT COUNT(*) FROM CLIENT_SUPPORT_ORDERS;

-- View sample records
SELECT * FROM CLIENT_SUPPORT_ORDERS LIMIT 5;

-- Check data distribution by product
SELECT ITEM, COUNT(*) as order_count 
FROM CLIENT_SUPPORT_ORDERS 
GROUP BY ITEM 
ORDER BY order_count DESC;
```

## 🚨 Troubleshooting

### Common Issues:

**"command not found: python"**
- Solution: Activate the conda environment: `conda activate sf-INGEST1-examples`

**"JWT token is invalid"**
- Solution: Ensure you've run the `ALTER USER INGEST1 SET RSA_PUBLIC_KEY=...` command in Snowflake

**"Permission denied"**
- Solution: Check that your Snowflake user has the INGEST1 role and proper permissions

**"File not found"**
- Solution: Ensure you're in the correct project directory

**Connection timeout**
- Solution: Check your Snowflake account identifier format and network connectivity

## 📁 Final Project Structure

```
snowflake-INGEST1ion/
├── .env                    # Snowflake credentials (keep secure!)
├── .gitignore             # Protects sensitive files
├── environment.yml        # Conda environment setup
├── data_generator.py      # Generates sample data
├── py_insert.py          # Inserts data to Snowflake  
├── data.json.gz          # Sample dataset (100K records)
├── rsa_key.p8            # Private key (keep secure!)
├── rsa_key.pub           # Public key
└── README.md             # This guide
```

## 🎉 Success!

You now have a complete Snowflake data INGEST1ion pipeline that can:
- Generate realistic e-commerce data
- Securely connect to Snowflake using key pair authentication
- Stream process data directly into Snowflake tables
- Handle JSON variant data types
- Scale to process large datasets

## 🔗 Next Steps

- Explore Snowflake's Snowpipe for real-time INGEST1ion
- Add data quality validation
- Implement error handling and retry logic
- Set up monitoring and alerting
- Create data transformation pipelines with dbt

---

**Security Reminder**: Always protect your `.env` file, private keys, and never commit them to version control!
