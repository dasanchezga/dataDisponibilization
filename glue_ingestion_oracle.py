from datetime import date, datetime as dt, timedelta
import logging
import sys
import json
from os.path import abspath
import pyspark.sql.functions as F
import boto3
from pyspark.context import SparkContext
from pyspark.sql.types import LongType
from awsglue.context import GlueContext
import random
import time
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, concat, current_date, date_add, lit, max, md5, row_number, to_date, when,coalesce,udf,round
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.types import StringType
from data_quality_cmpng import cont_vld

#log config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

#env config
args = getResolvedOptions(sys.argv, ["ENV","DATA_CONNECTION","DRIVER","FCT_DT"])
logger.info(f"ARGS: {args}")
ENV = args["ENV"]
DATA_CONNECTION = args["DATA_CONNECTION"]
DRIVER = args["DRIVER"]

#date format
P_FECHA= args["FCT_DT"] if args["FCT_DT"] != " " else (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
P_FECHA_FORMATO= args["FCT_DT"] if args["FCT_DT"] != " " else (date.today() - timedelta(days=1))
P_FECHA_DAYS= args["FCT_DT"] if args["FCT_DT"] != " " else (P_FECHA_FORMATO - timedelta(days=int(NUM_DAYS))).strftime("%Y-%m-%d")
P_FECHA_KEY=P_FECHA.replace('-','')

#table parameters
TABLE_DESTINATION = "table_destination"
DATABASES = [f"database_destination_{ENV}"]
PARTITION_KEY = "FCT_DT"
DATABASE_F= f"database_destination_{ENV}"
S3_ROUTE = f"s3://database_destination_{ENV}/{TABLE_DESTINATION}/"
 

#spark config
spark = (
    SparkSession.builder
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .enableHiveSupport()
    .getOrCreate()
)
logger.info("CONF")
logger.info(spark.sparkContext.getConf().getAll())

#glue config
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext) 
glue = boto3.client("glue", region_name="us-east-1")

#connection config
connection = glue.get_connection(Name=DATA_CONNECTION)["Connection"]["ConnectionProperties"]

secretsmanager = boto3.client("secretsmanager")
secret = json.loads(secretsmanager.get_secret_value(SecretId=connection["SECRET_ID"])["SecretString"])

username = secret["username"]
password = secret["password"]
dbname   = secret["dbname"]
jdbc_url = connection["JDBC_CONNECTION_URL"]

logger.info('Connections obtained')  

try:
    logger.info("SOURCE")
    logger.info("READING QUERY DATA")
    query_db = f"""(SELECT  /*+parallel(3)*/ COLUMN_1, COLUMN_2, COLUMN_3, COLUMN_4, COLUMN_5, COLUMN_6, COLUMN_7, COLUMN_8, IP_CD, COLUMN_9, COLUMN_10,
                    COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, COLUMN_21, COLUMN_22, COLUMN_23,  to_char(FCT_DT,'YYYY-MM-DD') AS FCT_DT
                    FROM table_source
                    WHERE FCT_DT = to_date('{P_FECHA}','YYYY-MM-DD'))"""

    print(f"Query: {query_db}")
    
    df_source = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable",query_db) \
                    .option("user", username) \
                    .option("password", password) \
                    .option("driver", DRIVER) \
                    .option("fetchsize", 10000)\
                    .load()
    
    #BD write
    for DATABASE in DATABASES:
        if TABLE_DESTINATION in [table.name for table in spark.catalog.listTables(DATABASE)]:
                        logger.info("UPDATING TABLE WITH NEW PARTITIONS")
                        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
                        df_source.write.mode("append").option("path",S3_ROUTE).insertInto(f"{DATABASE}.{TABLE_DESTINATION}", overwrite=True)
                        
        else:
                        logger.info("CREATING TABLE")
                        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
                        df_source.write.mode("overwrite").partitionBy(PARTITION_KEY).option("path", S3_ROUTE).saveAsTable(f"{DATABASE}.{TABLE_DESTINATION}")
    
    logger.info("DATA  OF TABLE")
    df_source.show(2)
    logger.info(f"registros df {df_source.count()}")
    
except ValueError as ve:
    print("Error:", ve)
    sys.exit(ve)
except Exception as e:
    print("Ocurrió un error inesperado:", e)
    sys.exit(e)

job.commit()