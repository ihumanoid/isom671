# Lab Guide: Big Data Ingestion, Transport, and Workflow Orchestration
**Objective:** Manually provision an end-to-end data pipeline crossing AWS RDS (MySQL), AWS EMR (Hadoop, Hive, Sqoop), and Apache Airflow 3.0.

---

### Step 1: Download the Source Dataset
1. Navigate to the official dataset page on your personal laptop browser: **[Kaggle New York Stock Exchange Dataset](https://kaggle.com)**.
2. Download the compressed files repository folder and extract the primary raw spreadsheet data file named exactly **`prices.csv`**.
3. Move or rename this file on your local operating system directory folder to: **`nyse_prices.csv`**.

---

### Step 2: Provision and Ingest Data via MySQL Workbench
1. Launch **MySQL Workbench** on your desktop workstation.
2. Establish a new connection pointing directly to the remote database engine instance. 
3. Configure the connection fields exactly as follows:
   * **Hostname:** `YOUR_RDS_ENDPOINT`
   * **Username:** `admin` *(Instructors/Graders: Insert your specific database username here if different)*
   * **Password:** `YOUR_PASSWORD` *(Instructors/Graders: Insert your specific database password here if different)*
4. Open a fresh SQL Query tab window editor inside the workbench environment, paste the following relational script, and execute it to create and populate your source data table:

```sql
-- Select your active lab database target space
USE YOUR_SCEHMA_NAME;

-- Wipe any pre-existing table schema to ensure a clean slate
DROP TABLE IF EXISTS nyse_raw;

-- Create the structural model mapping your CSV column sequence exactly
CREATE TABLE nyse_raw (
    trade_date DATE,
    ticker VARCHAR(10),
    open_price DOUBLE,
    close_price DOUBLE,
    low_price DOUBLE,
    high_price DOUBLE,
    volume INT,
    PRIMARY KEY (ticker, trade_date)
);

-- Force toggle the engine's local ingestion channel parameters on
SET GLOBAL local_infile = 1;

-- Ingest the records from your laptop's file path straight into the RDS cloud table
-- NOTE: Replace the file path string below with the exact directory location of the file on your laptop!
LOAD DATA LOCAL INFILE 'C:/Users/YourName/Downloads/nyse_prices.csv'
INTO TABLE nyse_raw
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@var_date, ticker, open_price, close_price, low_price, high_price, volume)
SET trade_date = STR_TO_DATE(TRIM(@var_date), '%Y-%m-%d');

-- Verify that the records loaded cleanly with proper column alignments
SELECT * FROM nyse_raw LIMIT 5;
```

---

### Step 3: Spin Up an AWS EMR Cluster Instance
1. Log into your **AWS Management Console** tracking dashboard panel.
2. Navigate to the **EMR (Elastic MapReduce)** services deployment wizard page.
3. Click **Create Cluster**, choose an **EMR 7.x Release** flavor, and select the specific software application options layout checking the following ecosystem components explicitly:
   * **Hadoop**
   * **Hive**
   * **Sqoop**
4. Under the Hardware Options page config settings layout, choose your deployment size and map your cluster securely to your course's private **EC2 Key Pair (`.pem`)** security file.

---

### Step 4: Establish an SSH Terminal Session via VS Code
1. Open desktop **VS Code** on your workstation.
2. Install the official extension titled **Remote - SSH** by Microsoft from the Extensions Marketplace panel.
3. Launch the Command Palette pane shortcut menu (`Ctrl+Shift+P` or `Cmd+Shift+P`) and trigger:
   ```text
   Remote-SSH: Connect to Host...
   ```
4. Enter the system target command connection criteria string explicitly:
   ```bash
   hadoop@<YOUR_EMR_MASTER_PUBLIC_DNS_ENDPOINT>
   ```
5. Choose your local `.pem` cluster key mapping file. Once connected, open folder `/home/hadoop/` inside your left navigation menu explorer canvas to code live on the cluster.

---

### Step 5: Install Apache Airflow & Dependency Extensions
Open the active bash command-line terminal view inside your connected VS Code application pane and run these installation commands to deploy Airflow 3.0 and patch version library conflicts:

```bash
# 1. Purge conflicting library namespace references
pip3 uninstall -y jwt PyJWT

# 2. Deploy the core framework engine binaries
pip3 install --user "apache-airflow==3.0.0" --constraint "https://githubusercontent.com" || pip3 install --user "apache-airflow"

# 3. Pin down the stable verified encryption standard library extension
pip3 install --user "PyJWT<2.11.0"

# 4. Inject the relational JDBC storage client mapping extension straight to Sqoop's system engine folder
sudo cp /usr/share/java/mysql-connector-java.jar /usr/lib/sqoop/lib/
```

---

### Step 6: Modify the `airflow.cfg` Application Parameters
To initialize your tracking variables file cleanly, force compile an initial setup flag sequence check by running:
```bash
airflow config list --help > /dev/null 2>&1 || true
```
Now, locate and double-click the newly generated text configuration file located at **`/home/hadoop/airflow/airflow.cfg`** inside your left VS Code folder list. Use `Ctrl+F` to search for and make the following manual value updates exactly as shown (ensure any leading `#` comment marks are fully deleted from modified configuration text lines):

1. Search for **`[api]`** section space block:
   ```ini
   port = 8085
   ```
2. Search for **`base_url`** variable layout assignment matching your custom port setup:
   ```ini
   base_url = http://localhost:8085
   ```
3. Search for **`[api_auth]`** security section:
   ```ini
   jwt_issuer = airflow
   ```
4. Search for **`[dag_processor]`** coordination interval section and accelerate file checks:
   ```ini
   refresh_interval = 5
   ```
5. Search for **`[core]`** data accessibility mode loops parameter:
   ```ini
   database_access_isolation = False
   ```
*Save your file edits by pressing **`Ctrl+S`** (or **`Cmd+S`**).*

---

### Step 7: Create Your Python Workflow File (`DAG`)
1. Create a dedicated structural folder layout for your orchestration files:
   ```bash
   mkdir -p /home/hadoop/airflow/dags
   ```
2. Create a new text file under that folder path named exactly **`airflow_dag.py`** (Full Path: `/home/hadoop/airflow/dags/airflow_dag.py`).
3. Open the file and copy-paste the entire validated workflow programming tracking framework template code exactly as written here:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the orchestration parameters
with DAG(
    dag_id="my_isom671_workflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Keeps it manual (Play button execution)
    catchup=False,
    tags=["education", "emr_workflow"]
) as dag:

    # STEP 1: Log initialization event metrics
    start_notification = BashOperator(
        task_id="notify_start",
        bash_command="echo '=== [INFO] Starting NYSE Stock Data Workflow at $(date) ==='"
    )

    # STEP 2: Initialize Hive MetaStore external landing schema layouts
    create_hive_table = BashOperator(
        task_id="create_nyse_hive_table",
        bash_command=(
            "hive -e \""
            "CREATE DATABASE IF NOT EXISTS stocks_db;"
            "CREATE EXTERNAL TABLE IF NOT EXISTS stocks_db.nyse ("
            "  trade_date STRING,"
            "  ticker STRING,"
            "  open_price DOUBLE,"
            "  close_price DOUBLE,"
            "  low_price DOUBLE,"
            "  high_price DOUBLE,"
            "  volume INT"
            ")"
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
            "LOCATION '/user/hive/warehouse/stocks_db.db/nyse';\""
        )
    )

    # STEP 3: Migrate table records over from cloud RDS using MapReduce
    # NOTE FOR STUDENTS: The connection string below is preconfigured for the class database.
    # If you are using a personal instance, replace 'admin' and 'YOUR_PASSWORD' with your custom database credentials.
    run_sqoop_migration = BashOperator(
        task_id="sqoop_rds_to_hdfs",
        bash_command=(
            "sqoop import "
            "--connect YOUR_ENDPOINT "
            "--username admin "
            "--password YOUR_PASSWORD "
            "--table nyse_raw "
            "--target-dir /user/hive/warehouse/stocks_db.db/nyse "
            "--delete-target-dir "
            "--driver com.mysql.cj.jdbc.Driver "
            "-m 1"
        )
    )

    # STEP 4: Aggregation query analytics to compile data file exports
    analyze_and_save_stats = BashOperator(
        task_id="calculate_ticker_stats",
        bash_command=(
            "hive -e \""
            "SELECT "
            "  ticker, "
            "  COUNT(*) as total_records, "
            "  MIN(trade_date) as oldest_record, "
            "  MAX(trade_date) as newest_record, "
            "  AVG(open_price) as avg_open, "
            "  AVG(close_price) as avg_close, "
            "  AVG(volume) as avg_vol "
            "FROM stocks_db.nyse "
            "WHERE ticker = 'AAPL' "
            "GROUP BY ticker;\" > /home/hadoop/results.txt"
        )
    )

    # Execution map structural pipeline dependencies routing direction
    start_notification >> create_hive_table >> run_sqoop_migration >> analyze_and_save_stats




### Step 8: Initialize and Run the Airflow Server

Execute these terminal commands to refresh your internal database schema states and start up your orchestration runtime process layer:
```bash

1. Clean and synchronize the metadata database backend tables

rm -f /home/hadoop/airflow/airflow.db
airflow db migrate

2. Boot up the standalone coordination engine wrapper

airflow standalone
```
Leave this terminal window completely open and running. Copy down the random security alpha-numeric login password printed out directly in your shell logs.

### Step 9: Access the Web Dashboard in Your Browser

1. Open a fresh Incognito / Private Browsing Window in your web browser.
2. Browse directly to your network address port map location: http://localhost:8085
3. Log into the landing screen portal with the standard master user profiles layout:
	• Username: admin
	• Password: (Paste the unique credential security string you copied from Step 8)

### Step 10: Locate and Trigger Your Workflow DAG

1. In the pipeline index table list on your home screen dashboard panel, click on my_isom671_workflow.
2. Click the Toggle Switch on the left side of your pipeline entry item row to shift it from Paused to Active.
3. Move your cursor to the upper right side menu options, click the Play Button icon, and hit Trigger DAG.
4. Monitor the task blocks grid layout graph structure visually as every separate brick transforms from blue into yellow, and finishes as a solid green block.

### Step 11: Validate and Audit the Final Results Output

Open a second parallel terminal window inside your VS Code workspace layout screen, and read the analytics metrics from the generated output text file:
bash cat /home/hadoop/results.txt 
Confirm that your output matches the statistical metrics calculation schema rows cleanly (e.g. tracking aggregate total record numbers, min dates, max dates, and average values metrics fields for symbol AAPL).

### Step 12: Laboratory Module Submission Guidelines

To receive full grading credit for this data infrastructure exercise, capture and bundle the following two validation criteria items inside your final assignment document upload canvas:
1. Screenshot 1: A full screen view showing all four tasks inside your laptop browser window showing clear solid-green execution blocks for your workflow on the Airflow 3.0 Grid View dashboard layout canvas.
2. Screenshot 2: A full view layout capture of your VS Code interface screen workspace area, clearly showing your open active terminal output view displaying the successful execution numbers of the cat /home/hadoop/results.txt validation script target.
