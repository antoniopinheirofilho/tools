# Databricks notebook source
# MAGIC %md
# MAGIC - Developed by antonio.pinheirofilho@databricks.com and wagner.silveira@databricks.com

# COMMAND ----------

# widgets utilities
dbutils.widgets.removeAll() 

dbutils.widgets.text("migration_db", "", "Migration DB")
dbutils.widgets.text("tracking_table", "", "Data Objects Tracking Migration table")
dbutils.widgets.text("tracking_table_location", "", "Tracking Migration table location")
# dbutils.widgets.text("config_file_dbfs_location", "", "Config File for Tables Migration")
dbutils.widgets.text("parallelization", "", "Parallization Threads")
dbutils.widgets.text("blacklist_filter", "", "Tables to blacklist")

# COMMAND ----------

# Migration database for cloned and tracking tables 
migration_db = dbutils.widgets.get("migration_db")

# Table which will track the migration progress
tracking_table = dbutils.widgets.get("tracking_table")

# Where to save the migration progress table
tracking_table_location = dbutils.widgets.get("tracking_table_location")

# The number of threads which will execute this operation
parallelization = dbutils.widgets.get("parallelization")

# Tables that will be ignored in the migration
blacklist_filter = dbutils.widgets.get("blacklist_filter")

# COMMAND ----------

# Create Migration Zone schema on the new environemnt
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_db}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {migration_db}.{tracking_table} 
        (db_name STRING, 
        table_name STRING, 
        ddl STRING,
        cloned_table_name_at_source STRING,
        table_type_at_source STRING,
        target_location STRING, 
        table_provider STRING, 
        status STRING, 
        status_code INT, 
        reason STRING,
        timestamp TIMESTAMP)
    USING delta
    LOCATION '{tracking_table_location}'
""")

temp_table = f"{migration_db}.migration_bootstrap"

# spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {temp_table} (db_name STRING, table_name STRING, status STRING, status_code INT, reason STRING, timestamp TIMESTAMP)")

# COMMAND ----------

import re
import datetime as dt

def run_ddls(ddl, temp_table):
    """
    Execute ddls
    
    Args:
        ddl (string): ddl string to be executed
    Return:
        return (tuple): 
                 database    (string): Database of the ddl
                 table       (string): Table of the ddl
                 status      (string): ddl execution status
                 status_code (int):    ddl execution status code
                 result      (string): sucess or error
                 timestamp   (string): ddl execution timestamp
    """
    try:
        table = None
        database = None
        search = re.search(r"CREATE TABLE IF NOT EXISTS \w+.(\w+).(\w+)", ddl[0])
        if search:
            database = search.group(1)
            table = search.group(2)
            
        print(f"Bootstrapping {database}.{table}")
        
        for sql_statements in ddl[0].split(";"):
            # Execute one sql command at a time
            spark.sql(sql_statements)
            
        # log for resolving migration tracking table
        print(f"\tSuccessfully bootstrapped {database}.{table}")
        spark.sql(f"""INSERT INTO TABLE {temp_table} VALUES ("{database}", "{table}", "bootstrapped_on_dest", 3, "success", "{dt.datetime.now()}")""")
    except Exception as e:
        # log for resolving migration tracking table
        print(f"\tError bootstrapping {database}.{table}: {str(e)}")
        error = str(e).replace("\"", "\\\"") if e else str(e)
        spark.sql(f"""INSERT INTO TABLE {temp_table} VALUES ("{database}", "{table}", "bootstrap_failed", 2, "{error}", "{dt.datetime.now()}")""")

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W
from concurrent.futures import ThreadPoolExecutor
import itertools

trck_table_df = spark.read.table(f"{migration_db}.{tracking_table}")
ddls = (trck_table_df.filter("status = 'migrated' or status='bootstrap_failed'").filter(F.col("ddl").isNotNull()))

blacklist = ""
if blacklist_filter:
    for item in blacklist_filter.split(","):
        db, table = item.strip().split(".")
        if blacklist:
            blacklist += " AND "
        blacklist += f"((db_name <> '{db}') AND (table_name <> '{table}'))"

    ddls = (ddls.filter(blacklist))

windSpec = W.Window.partitionBy('db_name', 'table_name').orderBy(F.col("timestamp").asc())
    
ddls = (ddls.withColumn("row", F.row_number().over(windSpec)) # keeps only latest record for table
            .filter(F.col("row") == 1).select("ddl") # keeps only latest record for table
            .collect() )

print("Number os tables to bootstrap: {}".format(len(ddls)))

if int(parallelization) > 0:
    # Parallelize execution
    with ThreadPoolExecutor(max_workers = int(parallelization)) as executor:
        executor.map(run_ddls, ddls, itertools.repeat(temp_table))    
else:
    for ddl in ddls:
        run_ddls(ddl, temp_table)

# COMMAND ----------

import pyspark.sql.window as W

new_cols = {
    "status_final": F.when(F.col("right.status").isNull(), F.col("left.status")).otherwise(F.col("right.status")),
    "status_code_final": F.when(F.col("right.status_code").isNull(), F.col("left.status_code")).otherwise(F.col("right.status_code")),
    "reason_final": F.when(F.col("right.reason").isNull(), F.col("left.reason")).otherwise(F.col("right.reason")),
    "timestamp_final": F.when(F.col("right.timestamp").isNull(), F.col("left.timestamp")).otherwise(F.col("right.timestamp"))
}

bootstrap_tables_df = spark.read.table(f"{temp_table}")

windSpec = W.Window.partitionBy('db_name', 'table_name').orderBy(F.col("timestamp").asc())

# Resolve migration tracking table, i.e., migrated tables are tagged as bootstrapped_on_dest
trck_table_updt_df = (trck_table_df.alias("left")
                                   .join(bootstrap_tables_df.alias("right"), ["db_name", "table_name"], "left")
                                   .withColumns(new_cols)
                                   .drop("status", "status_code", "reason", "timestamp")
                                   .withColumnRenamed("status_final", "status")
                                   .withColumnRenamed("status_code_final", "status_code")
                                   .withColumnRenamed("reason_final", "reason")
                                   .withColumnRenamed("timestamp_final", "timestamp")
                                   .withColumn("row", F.row_number().over(windSpec)) # keeps only latest record for table
                                   .filter(F.col("row") == 1).drop("row") # keeps only latest record for table
                     )

trck_table_updt_df.write.format("delta").mode("overwrite").saveAsTable(f"{migration_db}.{tracking_table}")

# COMMAND ----------

# Clean temp table for migration
spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
