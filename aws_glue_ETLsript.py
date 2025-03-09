import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schemas = {
    "fact_transactions": StructType([
        StructField("order_id", IntegerType(), False),
        StructField("line_id", IntegerType(), False),
        StructField("type", StringType(), True),
        StructField("dt", StringType(), False),
        StructField("pos_site_id", IntegerType(), True),
        StructField("sku_id", IntegerType(), True),
        StructField("fscldt_id", IntegerType(), True),
        StructField("price_substate_id", IntegerType(), True),
        StructField("sales_units", DecimalType(10,2), True),
        StructField("sales_dollars", DecimalType(10,2), True),
        StructField("discount_dollars", DecimalType(10,2), True),
        StructField("original_order_id", IntegerType(), True),
        StructField("original_line_id", IntegerType(), True)
    ]),
    "fact_averagecosts": StructType([
        StructField("fscldt_id", IntegerType(), False),
        StructField("sku_id", IntegerType(), False),
        StructField("average_unit_standardcost", DecimalType(10,4), True),
        StructField("average_unit_landedcost", DecimalType(10,4), True)
    ]),
    "hier_clnd": StructType([
        StructField("fscldt_id", IntegerType(), False),
        StructField("fscldt_label", StringType(), True),
        StructField("fsclwk_id", IntegerType(), True),
        StructField("fsclwk_label", StringType(), True),
        StructField("fsclmth_id", IntegerType(), True),
        StructField("fsclmth_label", StringType(), True),
        StructField("fsclqrtr_id", IntegerType(), True),
        StructField("fsclqrtr_label", StringType(), True),
        StructField("fsclyr_id", IntegerType(), True),
        StructField("fsclyr_label", StringType(), True),
        StructField("ssn_id", IntegerType(), True),
        StructField("ssn_label", StringType(), True),
        StructField("ly_fscldt_id", IntegerType(), True),
        StructField("lly_fscldt_id", IntegerType(), True),
        StructField("fscldow", IntegerType(), True),
        StructField("fscldom", IntegerType(), True),
        StructField("fscldoq", IntegerType(), True),
        StructField("fscldoy", IntegerType(), True),
        StructField("fsclwoy", IntegerType(), True),
        StructField("fsclmoy", IntegerType(), True),
        StructField("fsclqoy", IntegerType(), True),
        StructField("date", DateType(), True)
    ]),
    "hier_prod": StructType([
        StructField("sku_id", IntegerType(), False),
        StructField("sku_label", StringType(), True),
        StructField("stylclr_id", IntegerType(), True),
        StructField("stylclr_label", StringType(), True),
        StructField("styl_id", IntegerType(), True),
        StructField("styl_label", StringType(), True),
        StructField("subcat_id", IntegerType(), True),
        StructField("subcat_label", StringType(), True),
        StructField("cat_id", IntegerType(), True),
        StructField("cat_label", StringType(), True),
        StructField("dept_id", IntegerType(), True),
        StructField("dept_label", StringType(), True),
        StructField("issvc", BooleanType(), True),
        StructField("isasmbly", BooleanType(), True),
        StructField("isnfs", BooleanType(), True)
    ]),
    "hier_invstatus": StructType([
        StructField("code_id",StringType(), False),
        StructField("code_label", StringType(), True),
        StructField("bckt_id", StringType(), True),
        StructField("bckt_label", StringType(), True),
        StructField("ownrshp_id", StringType(), True),
        StructField("ownrshp_label", StringType(), True)
    ]),
    "hier_rtlloc": StructType([
        StructField("str", IntegerType(), False),
        StructField("str_label", StringType(), True),
        StructField("dstr", IntegerType(), True),
        StructField("dstr_label", StringType(), True),
        StructField("rgn", IntegerType(), True),
        StructField("rgn_label", StringType(), True)
    ]),
    "hier_possite": StructType([
        StructField("site_id", IntegerType(), False),
        StructField("site_label", StringType(), True),
        StructField("subchnl_id", IntegerType(), True),
        StructField("subchnl_label", StringType(), True),
        StructField("chnl_id", IntegerType(), True),
        StructField("chnl_label", StringType(), True)
    ]),
    "hier_invloc": StructType([
        StructField("loc", IntegerType(), False),
        StructField("loc_label", StringType(), True),
        StructField("loctype", IntegerType(), True),
        StructField("loctype_label", StringType(), True)
    ]),
    "hier_hldy": StructType([
        StructField("hldy_id", StringType(), False),
        StructField("hldy_label", StringType(), True)
    ]),
    "hier_pricestate": StructType([
        StructField("substate_id", StringType(), False),
        StructField("substate_label", StringType(), True),
        StructField("state_id", StringType(), False),
        StructField("state_label", StringType(), True)
    ])
}
primary_keys = {
    "fact_transactions": ["order_id", "line_id"],
    "fact_averagecosts": ["fscldt_id", "sku_id"],
    "hier_clnd": ["fscldt_id"],
    "hier_prod": ["sku_id"],
    "hier_invstatus": ["code_id"],
    "hier_rtlloc": ["str"],
    "hier_possite": ["site_id"],
    "hier_invloc": ["loc"],
    "hier_hldy": ["hldy_id"],
    "hier_pricestate": ["substate_id"]
}


fill_defaults = {
    IntegerType(): 0,
    StringType(): "UNKNOWN",
    DecimalType(): 0.0,
    BooleanType(): False
}

# Tables to process
tables = list(schemas.keys())

for table in tables:
    print(f"Processing table: {table}")

    # Fix file naming issue
    input_path = f"s3://mygsynergy/{table.replace('_', '.')}.dlm.gz"

    # Read CSV
    df = spark.read.option("header", True).option("sep", "|").csv(input_path, schema=schemas.get(table))

    # Fix dt column if present
    if "dt" in df.columns:
        df = df.withColumn("dt", regexp_replace(col("dt"), "T", " "))
        df = df.withColumn("dt", col("dt").cast(TimestampType()))

    # Drop rows with missing primary keys
    df = df.dropna(subset=primary_keys[table])

    # Fill missing values
    for field in schemas[table]:
        if field.nullable:
            df = df.fillna({field.name: fill_defaults.get(type(field.dataType), "UNKNOWN")})

    # Convert DataFrame to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext)

    # Write transformed data to S3
    output_path = f"s3://mygsynergy/transformed/{table}/"
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="csv",
        connection_options={"path": output_path, "compression": "gzip", "partitionKeys": []}
    )

print("All tables processed successfully.")
job.commit()