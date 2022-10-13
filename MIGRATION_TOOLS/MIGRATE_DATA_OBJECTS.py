# Databricks notebook source
# MAGIC %md
# MAGIC Based on https://demo.cloud.databricks.com/#notebook/12634129/command/12634130

# COMMAND ----------

# MAGIC %md
# MAGIC This script is for Data Transfer only. The tables DDLs will still need to be executed on the target Metastore

# COMMAND ----------

# The config file is a csv file containing the following columns:
# source_database: Name of the database containing the table to be migrated
# source_table: Name of the table to be migrated
# target_location: Object storage location where data objects will be transferred to
# Managed: True if this table should become managed in the target environment (Not used by this script)
config_file_dbfs_location = dbutils.widgets.get("config_file_dbfs_location")

# Table which will track the migration progress. It should be formatted as [db].[table]
tracking_table = dbutils.widgets.get("tracking_table")

# The number of threads which will execute this operation
parallelization = dbutils.widgets.get("parallelization")

# COMMAND ----------

def exec_migrate_table(table, db_name, tracking_table):
    
    """
    Executor function to move a table. This should be called by a ThreadPoolExecutor. 
    Depending on the source table type, the table will be moved either by DEEP CLONE
    (for Delta) or by spark.read/spark.write (for non-Delta). Source data will be
    left in the original location- please use your cloud provider utilities to clean up source data.
    """
    
    table_name = table.source_table
    target_location = table.target_location
    
    # describe the table (this will fail if table doesn't exist)
    try:
        table_details = spark.sql("describe extended {}.{}".format(db_name, table_name))
    except:
        print("Error describing table {}.{}; please check and re-run.".format(db_name, table_name))
        log_migration(tracking_table, db_name, target_location, table_name, None, None, None, -1, "describe error")
        return
    
    # Table type, whether it is managed or unmanaged
    table_type = table_details.filter("col_name == 'Type'").collect()[0][1]
    
    # extract table details from describe statement
    provider = table_details.filter(table_details.col_name == "Provider").collect()[0][1]
    
    # copy based on the table provider: delta, csv, parquet, or hive
    if provider.lower() == "delta":
        print("Moving table {}.{} to location {}".format(db_name,table_name,target_location))
        
        cloned_table_name = f"{table_name}_clone"
        
        # create the clone
        try:
            spark.sql("CREATE OR REPLACE TABLE {0}.{1} DEEP CLONE {0}.{2} LOCATION '{3}'".format(db_name, cloned_table_name, table_name, target_location))
        except Exception as e:
            print("Error migrating table {}.{}; Exception: {}".format(db_name, table_name, str(e)))
            log_migration(tracking_table, db_name, target_location, table_name, provider, table_type, None, -1, str(e))
            return
        
        # log migration
        log_migration(tracking_table, db_name, target_location, table_name, provider, table_type, cloned_table_name, 1, "success")
        
    elif provider.lower() in ["org.apache.spark.sql.parquet", "parquet"]:
        print("Moving table {}.{} to location {}".format(db_name,table_name,target_location))
        
        try:
            spark.read.table("{}.{}".format(db_name, table_name)).write.format("parquet").mode("overwrite").save(target_location)
        except Exception as e:
            print("Error migrating table {}.{}; Exception: {}".format(db_name, table_name, str(e)))
            log_migration(tracking_table, db_name, target_location, table_name, provider, table_type, None, -1, str(e))
            return
        
        # log migration
        log_migration(tracking_table, db_name, target_location, table_name, provider, table_type, None, 1, "success")
        
    elif provider.lower() in ["com.databricks.spark.csv", "csv"]:
        print("Moving table {}.{} to location {}".format(db_name,table_name,target_location))
        
        try:
            spark.read.table("{}.{}".format(db_name, table_name)).write.format("csv").mode("overwrite").save(target_location)
        except Exception as e:
            print("Error migrating table {}.{}; Exception: {}".format(db_name, table_name, str(e)))
            log_migration(tracking_table, db_name, target_location, table_name, provider, table_type, None, -1, str(e))
            return
        
        # log migration
        log_migration(tracking_table, db_name, target_location, table_name, provider, table_type, None, 1, "success")
        
    else:
        print("Found unknown provider {} for table {}.{}".format(provider,db_name,table_name))
        log_migration(tracking_table, db_name, target_location, table_name, provider, table_type, None, -1, "unknown provider")
        
def log_migration(tracking_table, db_name, target_location, table_name, table_provider, table_type_at_source, cloned_table_name, status_code, reason):
    """
    Helper function to log migration status to the tracking table.
    """
    
    if status_code == -1:
        status = "unmigrated"
    elif status_code == 0:
        status = "skipped"
    elif status_code == 1:
        status = "migrated"
    else:
        status = "unknown"

    spark.sql(f"INSERT INTO TABLE {tracking_table} VALUES ('{db_name}','{table_name}', '{cloned_table_name}','{target_location}', '{table_provider}','{table_type_at_source}','{datetime.datetime.now()}','{status}',{status_code},'{reason}')")

# COMMAND ----------

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools
from pyspark.sql.functions import *
import datetime;

def migrate_databases(list_tables, tracking_table, parallelization):
    """
    This function will move data in between object storages based on list_tables. Depending on the type of table it will:
    a. For Delta -> It will create a deep cloned table pointing to the defined target object storage 
    b. For Non-Delta -> It will overwrite the content of the defined target object storage
    Source Data will be left in-place.
    Parallelization can. be increased by setting parallelization to a higher number.
    Migration results will be written to tracking_table.
    """
    
    total_tables = 0
    
    for db_object in list_tables.select("source_database").distinct().collect():
        # List all tables for this database
        table_list = list_tables.where(col("source_database") == lit(db_object.source_database)).collect()
        total_tables += len(table_list)
        
        # launch parallelization executors to migrate tables
        print("Beginning migration of database {}".format(db_object.source_database))
        with ThreadPoolExecutor(max_workers = parallelization) as executor:
            executor.map(exec_migrate_table, table_list, itertools.repeat(db_object.source_database), itertools.repeat(tracking_table))
            
    # print details of migration; see table below for full results
    num_migrated_tables = spark.sql("SELECT * FROM {} WHERE status_code = 1".format(tracking_table)).count()
    print("Migrated {} out of {} tables.".format(num_migrated_tables, total_tables))
    print("See table {} for details on migrated tables.".format(tracking_table))

# COMMAND ----------

list_tables = spark.read.format("csv").option("header", True).option("delimiter", ";").load(config_file_dbfs_location)

# Create tracking table
spark.sql(f"CREATE OR REPLACE TABLE {tracking_table} (db_name STRING, table_name STRING, cloned_table_name STRING, target_location STRING, table_provider STRING, table_type_at_source STRING, timestamp TIMESTAMP, status STRING, status_code INT, reason STRING)")

migrate_databases(list_tables, tracking_table, parallelization)
