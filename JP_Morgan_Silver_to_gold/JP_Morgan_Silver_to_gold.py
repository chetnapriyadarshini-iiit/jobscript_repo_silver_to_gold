import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
JOB_NAME = args['JOB_NAME']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('JPMorgan_silver_to_gold_insights', args)

#variables for base folders and input or write out points 
SILVER_BUCKET = "s3://majorproject02-jpmorgan-silver/partitioned_by_state_department/"
GLUE_DB = "majorproject02_jpmorgan_db"

GOLD_BUCKET = "majorproject02-jpmorgan-gold"

GOLD_BASE   = f"s3://{GOLD_BUCKET}"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GLUE_DB}")  # idempotent: safe to run multiple times


#output naming convension variables 
# Level-1 (filter-only) dataset paths (no new columns; just subsets for targeted analytics)
GOLD_L1_SAL_HIGH   = f"{GOLD_BASE}/l1_high_salary/"            # salary ≥ 7000
GOLD_L1_BONUS_HIGH = f"{GOLD_BASE}/l1_high_bonus/"             # bonus ≥ 1500
GOLD_L1_YOUNG      = f"{GOLD_BASE}/l1_early_career/"           # age ≤ 25 (operational proxy)

GOLD_L1_CA_GTM     = f"{GOLD_BASE}/l1_ca_sales_marketing/"     # CA + (Sales or Marketing)

# Level-2 engineered & KPI dataset paths
GOLD_L2_DETAIL              = f"{GOLD_BASE}/l2_detail_engineered/"            # detail table with derived columns
GOLD_KPI_AVG_COMP_BY_DEPT   = f"{GOLD_BASE}/kpi_avg_total_comp_by_dept/"      # KPI 1 table
GOLD_KPI_HC_BY_STATE_DEPT   = f"{GOLD_BASE}/kpi_headcount_by_state_dept/"     # KPI 2 table

from pyspark.sql import functions as F 
from awsglue.dynamicframe import DynamicFrame

def sink_with_catalog(path: str, table_name: str, partition_keys: list, ctx_name: str):
    """
    Create a Glue catalog sink that:
      - writes Parquet to S3
      - updates/creates the Glue Catalog table (schema + partitions)
    partition_keys: list of partition columns ([] for non-partitioned KPIs)
    """
    sink = glueContext.getSink(
        path=path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",      # merge schema/partitions if table exists
        partitionKeys=partition_keys,
        compression="snappy",
        enableUpdateCatalog=True,                 # <-- critical: tells Glue to maintain the Catalog table
        transformation_ctx=ctx_name
    )
    sink.setCatalogInfo(catalogDatabase=GLUE_DB, catalogTableName=table_name)
    sink.setFormat("glueparquet")                 # optimized writer for Parquet + Catalog
    return sink

df_silver = spark.read.parquet(SILVER_BUCKET)
df_l1_sal_high = df_silver.filter(F.col("salary") >= 7000)
        
## 1b) High-Variable-Cost cohort (bonus ≥ 1500)

df_l1_bonus_high = df_silver.filter(F.col("bonus") >= 1500)

# 1c) Early-tenure (operational proxy) cohort (age ≤ 25)

df_l1_young = df_silver.filter(F.col("age") <= 25 )

(df_l1_sal_high
         .repartition("state","department") #spark reshuffles here #co-locate rows per partition for better write pattern and startegy
         .write.mode("overwrite")
         .option("compression","snappy")
         .partitionBy("state","department") #spark sorts and put , so what ever update , it will go right place
         .parquet(GOLD_L1_SAL_HIGH)
)

(df_l1_bonus_high
         .repartition("state","department") #spark reshuffles here #co-locate rows per partition for better write pattern and startegy
         .write.mode("overwrite")
         .option("compression","snappy")
         .partitionBy("state","department") #spark sorts and put , so what ever update , it will go right place
         .parquet(GOLD_L1_BONUS_HIGH)
)

(df_l1_young
         .repartition("state","department") #spark reshuffles here #co-locate rows per partition for better write pattern and startegy
         .write.mode("overwrite")
         .option("compression","snappy")
         .partitionBy("state","department") #spark sorts and put , so what ever update , it will go right place
         .parquet(GOLD_L1_YOUNG)
)
        
# Convert Spark DFs → DynamicFrames (so sinks can update the Catalog)
dyf_l1_sal_high   = DynamicFrame.fromDF(df_l1_sal_high,   glueContext, "dyf_l1_sal_high")
dyf_l1_bonus_high = DynamicFrame.fromDF(df_l1_bonus_high, glueContext, "dyf_l1_bonus_high")
dyf_l1_young      = DynamicFrame.fromDF(df_l1_young,      glueContext, "dyf_l1_young")

sink_with_catalog(GOLD_L1_SAL_HIGH, "gold_l1_high_salary", ["state","department"], "sink_l1_sal").writeFrame(dyf_l1_sal_high)
sink_with_catalog(GOLD_L1_BONUS_HIGH, "gold_l1_high_bonus",          ["state","department"], "sink_l1_bonus").writeFrame(dyf_l1_bonus_high)
sink_with_catalog(GOLD_L1_YOUNG,      "gold_l1_early_career",        ["state","department"], "sink_l1_young").writeFrame(dyf_l1_young)

job.commit()