Step 1: Create an S3 Bucket
Go to AWS Console → S3 → Create Bucket.

Step 2: Upload .gz Files to S3
Go to AWS Console → S3.
Open your bucket (mygsynergy).
Click Upload → Add files.
Select your .gz files from your local system.


Step 3: Create an IAM Role for Redshift
Go to AWS Console → IAM → Roles.

Click Create Role.
policy to be attached - AmazonS3ReadOnlyAccess
Role Name-RedshiftS3Role.
🔹 Get the Role ARN - key identifier for this role

Step 4: Create a Redshift Cluster
Go to AWS Console → Redshift → Clusters.

Click Create Cluster-my-redshift-cluster.
Attach IAM Role: Select RedshiftS3Role.
Click Create Cluster.
🔹 Get the Redshift Endpoint: key identifier for this cluster

🔹 transform data before loading raw data
Step 5: Create an AWS Glue Crawler to Infer Schema
Go to AWS Glue Console → Click Crawlers.
Click Add Crawler-my-s3-crawler.
Under Data sources, choose:S3.
Enter S3 path as the folder
s3://my-redshift-data/
Click Add an S3 data source → Click Next.

IAM Role: edit the policies of RedshiftS3Role with:
AWS Glue Service Role as well
Choose Database:
If first time: Create a new Glue database-s3_redshift_db

Click Run Crawler and wait for completion.
Go to Glue Tables (under Data Catalog) and check if tables are created.


Step 6: Create a New IAM Role for AWS Glue
Go to AWS IAM Console → Click Roles → Create role.

Permissions:
Attach AmazonS3ReadOnlyAccess (to read from S3).
Attach AWSGlueServiceRole (for Glue jobs & crawlers).
Attach AmazonRedshiftFullAccess (if loading into Redshift).
Name the Role → Example: AWSGlueRole
Create Role.


Step 7: Create AWS Glue Job to Transform Data
Open AWS Glue Console → Go to Jobs → Click Create Job.
Job Type → Select Spark Script (Python).
IAM Role →  AWSGlueRole
Data Source → Select the Glue Catalog Table from Step 2.
Choose Data Target (S3 - CSV Format)
Select Target → Choose S3.
S3 Path for Transformed Data →
s3://my-redshift-data/transformed/
Output Format → CSV .
Click Next.

Step 8: run the PySpark Script of the job reated above but with some transformations
full script added to this repository
below transformations performed by glue script
1.Extract .gz files from S3.
2.Convert date format (dt) to TIMESTAMP without T in between
3.Drop rows with missing critical fields- primary key values
4.fill missing values with appropriate data according to datatype
5.handle rows with extra columns
6.Write the output as CSV with , delimiter.


Step 9: Run the AWS Glue Job
Go to AWS Glue Console → Click Jobs → Select Your Job.
Click Run Job.
Once completed, check the transformed files in s3://my-redshift-data/transformed/


Step 10: Create a Staging Schema in Redshift
Open AWS Redshift Query Editor v2.
Connect to your cluster using the endpoint, username, and password(most of the time pre populated).

Run the schema SQLs for each table attached allRSqueries.sql to this repo


Step 11: Load Data from S3 into Redshift
Open AWS Redshift Query Editor v2.
Run the copy SQL queries for each table attached allRSqueries.sql to this repo

Step 12: Verify Data in Redshift

SELECT COUNT(*) FROM staging.fact_transactions;
SELECT * FROM staging.fact_transactions LIMIT 10;


Step 13: Create the mview_weekly_sales Table: SQL query attached allRSqueries.sql to this repo

Step 14: Insert Initial Full Data
Since we've loaded data into tables, first we need to populate mview_weekly_sales with full aggregated sales from fact_transactions.


Step 15: Schedule incremental load Using Amazon Redshift Query Editor v2: SQL query attached allRSqueries.sql to this repo
