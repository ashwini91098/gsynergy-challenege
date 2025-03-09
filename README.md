Step 1: Create an S3 Bucket
Go to AWS Console → S3 → Create Bucket.

Enter a bucket name (e.g., your-bucket-name).
Select a region (e.g., us-east-1).
Uncheck "Block all public access" (if needed).
Click "Create bucket".

Step 2: Upload .gz Files to S3
Go to AWS Console → S3.

Open your bucket (your-bucket-name).
Click Upload → Add files.
Select your .gz files from your local system.
Click Upload.

Step 3: Create an IAM Role for Redshift
Go to AWS Console → IAM → Roles.

Click Create Role.
Select "AWS Service" → Redshift.
Select "Redshift - Customizable".
Click Next and Attach Policies:
AmazonS3ReadOnlyAccess
Click Next → Enter Role Name (e.g., RedshiftS3Role).
Click Create Role.
🔹 Get the Role ARN:

Open IAM Roles.
Click RedshiftS3Role.
Copy Role ARN (e.g., arn:aws:iam::123456789012:role/RedshiftS3Role).

Step 4: Create a Redshift Cluster
Go to AWS Console → Redshift → Clusters.

Click Create Cluster.
Enter Cluster Name (e.g., my-redshift-cluster).
Choose "Free-tier eligible" (if applicable).
Enter Username & Password.
Expand "Additional Configuration" → Attach IAM Role:
Select RedshiftS3Role.
Click Create Cluster.
🔹 Get the Redshift Endpoint:

Open Clusters.
Click your Cluster Name.
Copy Cluster Endpoint (e.g., my-redshift-cluster.abc123.us-east-1.redshift.amazonaws.com).



*****transform data before loading raw data
Step 2: Create an AWS Glue Crawler to Infer Schema
Go to AWS Glue Console → Click Crawlers.
Click Add Crawler.
Enter a name (e.g., my-s3-crawler).
Click Next.
Under Data sources, choose:
S3.
Enter S3 path as the folder, NOT the file:
arduino
Copy
Edit
s3://my-redshift-data/raw/
Click Add an S3 data source → Click Next.
IAM Role: Choose an existing role or create a new one with:
S3 Read Access (AmazonS3ReadOnlyAccess).
AWS Glue Service Role.
Choose Database:
If first time: Create a new Glue database (e.g., s3_raw_db).
Otherwise, select an existing database.
Click Next → Finish.
Click Run Crawler and wait for completion.
Go to Glue Tables (under Data Catalog) and check if your tables are created.


Step 2: Create a New IAM Role for AWS Glue
Go to AWS IAM Console → Click Roles → Create role.
Select Trusted Entity → Choose AWS Service.
Use Case:
Select Glue if setting up Glue Crawler or ETL.
Select Redshift if setting up Redshift COPY.
Permissions:
Attach AmazonS3ReadOnlyAccess (to read from S3).
Attach AWSGlueServiceRole (for Glue jobs & crawlers).
Attach AmazonRedshiftFullAccess (if loading into Redshift).
Name the Role → Example: AWSGlueRole
Create Role.


Step 3: Create AWS Glue Job to Transform Data
Open AWS Glue Console → Go to Jobs → Click Create Job.
Job Type → Select Spark Script (Python).
IAM Role → Choose a role with S3 read/write and Glue permissions.
Script Path → Save the script inside S3 (e.g., s3://my-redshift-data/scripts/transform.py).
Data Source → Select the Glue Catalog Table from Step 2.
Transform Data: Convert the dt column format and save as CSV.

Step 4: Choose Data Target (S3 - CSV Format)
Select Target → Choose S3.
S3 Path for Transformed Data →
s3://my-redshift-data/transformed/fact_transactions/
Output Format → CSV .
Click Next.

Step 5: run the PySpark Script

Extract .gz files from S3.
Convert date format (dt) to TIMESTAMP.
Write the output as CSV with | delimiter.


Step 6: Run the AWS Glue Job
Go to AWS Glue Console → Click Jobs → Select Your Job.
Click Run Job.
Once completed, check the transformed files in s3://my-redshift-data/transformed/fact_transactions/

**errors 
handle missing values
missing data for not null fields-Drop rows with missing critical fields
handle rows with extra columns
incorrect datatype declared initially 

Step 5: Create a Staging Schema in Redshift
Open AWS Redshift Query Editor v2.
Connect to your cluster using the endpoint, username, and password.

Run the following SQL:
sql

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.fact_sales (
    sales_id BIGINT PRIMARY KEY,
    pos_site_id INT,
    sku_id INT,
    fsclwk_id INT,
    price_substate_id INT,
    type VARCHAR(50),
    sales_units INT,
    sales_dollars FLOAT,
    discount_dollars FLOAT
);

Step 6: Load Data from S3 into Redshift
Open AWS Redshift Query Editor v2.
Run the following SQL:
sql
Copy
Edit
COPY staging.fact_sales 
FROM 's3://your-bucket-name/data/fact_sales.csv.gz'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER '|'
GZIP
IGNOREHEADER 1;

Step 7: Verify Data in Redshift
Run:
sql
SELECT COUNT(*) FROM staging.fact_sales;
SELECT * FROM staging.fact_sales LIMIT 10;



Step 1: Create the mview_weekly_sales Table
Run this SQL in your Redshift editor:

sql
Copy
Edit
CREATE TABLE IF NOT EXISTS mview_weekly_sales (
    pos_site_id INT NOT NULL,
    sku_id INT NOT NULL,
    fsclwk_id INT NOT NULL,
    price_substate_id INT NOT NULL,
    type VARCHAR(50) NOT NULL,
    total_sales_units DECIMAL(18,2),
    total_sales_dollars DECIMAL(18,2),
    total_discount_dollars DECIMAL(18,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
This table will store weekly aggregated sales data.

Step 2: Insert Initial Full Data
Since you've loaded data into your tables, you first need to populate mview_weekly_sales with full aggregated sales from fact_transactions.

INSERT INTO mview_weekly_sales (
    pos_site_id, sku_id, fsclwk_id, price_substate_id, type, 
    total_sales_units, total_sales_dollars, total_discount_dollars
)
SELECT 
    pos_site_id, sku_id, fsclwk_id, price_substate_id, type, 
    SUM(sales_units) AS total_sales_units,
    SUM(sales_dollars) AS total_sales_dollars,
    SUM(discount_dollars) AS total_discount_dollars
FROM fact_transactions
GROUP BY pos_site_id, sku_id, fsclwk_id, price_substate_id, type;
This ensures the table is fully populated before applying incremental updates.




Schedule Using Amazon Redshift Query Editor v2 (Simplest)
If you want to run the incremental update SQL on a schedule, you can use Amazon Redshift Query Editor v2.

Steps to Schedule a Query in Query Editor v2
Open AWS Console → Navigate to Amazon Redshift.
Click Query Editor v2.
Select Your Cluster → Click Manage Schedules.
Click Create Schedule.
Set Details:
Name: Update_Weekly_Sales
Query: Copy and paste the incremental update SQL:

WITH changed_weeks AS (
    SELECT DISTINCT fsclwk_id 
    FROM fact_transactions
    WHERE dt >= (SELECT MAX(last_updated) FROM mview_weekly_sales)
)
DELETE FROM mview_weekly_sales
WHERE fsclwk_id IN (SELECT fsclwk_id FROM changed_weeks);
INSERT INTO mview_weekly_sales (
    pos_site_id, sku_id, fsclwk_id, price_substate_id, type, 
    total_sales_units, total_sales_dollars, total_discount_dollars
)
SELECT 
    pos_site_id, sku_id, fsclwk_id, price_substate_id, type, 
    SUM(sales_units) AS total_sales_units,
    SUM(sales_dollars) AS total_sales_dollars,
    SUM(discount_dollars) AS total_discount_dollars
FROM fact_transactions
WHERE fsclwk_id IN (SELECT fsclwk_id FROM changed_weeks)
GROUP BY pos_site_id, sku_id, fsclwk_id, price_substate_id, type;
Schedule Type: Daily
Time: Set a suitable time (e.g., midnight).
Timezone: Your preferred timezone.
Click "Create" → The query will now run daily.
