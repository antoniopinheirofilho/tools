# Databricks notebook source
# MAGIC %md
# MAGIC - Based on https://demo.cloud.databricks.com/#notebook/12634129/command/12634130
# MAGIC - Developed by antonio.pinheirofilho@databricks.com and wagner.silveira@databricks.com

# COMMAND ----------

# MAGIC %md
# MAGIC This script is for Data Transfer only. The tables DDLs will still need to be executed on the target Metastore

# COMMAND ----------

# widgets utilities
dbutils.widgets.removeAll() 

dbutils.widgets.text("migration_db", "", "Migration DB")
dbutils.widgets.text("tracking_table", "", "Data Objects Tracking Migration table")
dbutils.widgets.text("tracking_table_location", "", "Tracking Migration table location")
dbutils.widgets.text("config_file_dbfs_location", "", "Config File for Tables Migration") # dbfs:/FileStore/migration/Migrated_102822_1030.csv
dbutils.widgets.text("parallelization", "", "Parallization Threads")

# COMMAND ----------

# The config file is a csv file containing the following columns:
# source_database: Name of the database containing the table to be migrated
# source_table: Name of the table to be migrated
# target_location: Object storage location where data objects will be transferred to
config_file_dbfs_location = dbutils.widgets.get("config_file_dbfs_location")

# Migration database for cloned and tracking tables 
migration_db = dbutils.widgets.get("migration_db")

# Table which will track the migration progress
tracking_table = dbutils.widgets.get("tracking_table")

# Where to save the migration progress table
tracking_table_location = dbutils.widgets.get("tracking_table_location")

# The number of threads which will execute this operation
parallelization = dbutils.widgets.get("parallelization")

tracking_table_schema = """db_name STRING, 
            table_name STRING, 
            ddl STRING,
            cloned_table_name_at_source STRING,
            table_type_at_source STRING,
            target_location STRING, 
            table_provider STRING, 
            status STRING, 
            status_code INT, 
            reason STRING,
            timestamp TIMESTAMP"""

# COMMAND ----------

# Disable cross cloud error
spark.conf.set("spark.databricks.delta.logStore.crossCloud.fatal", False)

# COMMAND ----------

import re
import json

def exec_migrate_table(table, migration_db, tracking_table):
    
    """
    Executor function to move a table. This should be called by a ThreadPoolExecutor. 
    Depending on the source table type, the table will be moved either by DEEP CLONE
    (for Delta) or by spark.read/spark.write (for non-Delta). Source data will be
    left in the original location- please use your cloud provider utilities to clean up source data.
    """
    
    db_name = table.source_database
    table_name = table.source_table
    target_location = table.target_location
    # customer had s3 mounted on origin workspace
    mnt_location = target_location.replace("s3a://", "/mnt/")
    
    logging_table = f"{migration_db}.{tracking_table}"
    
    # describe the table (this will fail if table doesn't exist)
    try:
        table_details = spark.sql("describe extended {}.{}".format(db_name, table_name))
    except Exception as e:
        print("Error describing table {}.{}; please check and re-run.".format(db_name, table_name))
        log_migration(logging_table, db_name, target_location, table_name, None, None, None, -1, str(e), None)
        return
    
    # Table type, whether it is managed or unmanaged
    table_type = table_details.filter("col_name == 'Type'").collect()[0][1]
    
    # extract table details from describe statement
    provider = table_details.filter("col_name == 'Provider'").collect()[0][1]
    
    # copy based on the table provider: delta, csv, parquet, or hive
    if provider.lower() == "delta":
        print("Moving delta table {}.{} to location {}".format(db_name,table_name,target_location))
        
        cloned_table_name = f"clone_{db_name}_{table_name}"
        
        # create the clone
        try:
            ddl = ddl_dump(db_name, table_name, target_location)
            
            spark.sql("CREATE OR REPLACE TABLE {0}.{1} DEEP CLONE {2}.{3} LOCATION '{4}'".format(migration_db, cloned_table_name, db_name, table_name, mnt_location))
        except Exception as e_outer:
            # if deep clone fails because: com.databricks.tahoe.store.S3LockBasedLogStore doesn't support createAtomicIfAbsent(path: Path)
            # try to manually migrate the table
            try:
                pattern = re.compile(r"createAtomicIfAbsent")
                if re.search(pattern, str(e_outer)):
                    dbutils.fs.rm(mnt_location, recurse=True) # Remove Deep Clone garbage
                    migrate_table(db_name, table_name, "delta", mnt_location)
                else:
                    print("Error migrating delta table {}.{}; Exception: {}".format(db_name, table_name, str(e_outer)))
                    log_migration(logging_table, db_name, target_location, table_name, provider, table_type, None, -1, str(e_outer), None)
                    return
            except Exception as e:
                print("Error migrating delta table {}.{} via dataframe copy; Exception: {}".format(db_name, table_name, str(e)))
                log_migration(logging_table, db_name, target_location, table_name, provider, table_type, None, -1, str(e), None)
                return
        
        # log migration
        log_migration(logging_table, db_name, target_location, table_name, provider, table_type, cloned_table_name, 1, "success", ddl)
        
    elif provider.lower() in ["org.apache.spark.sql.parquet", "parquet"]:
        print("Moving parquet table {}.{} to location {}".format(db_name,table_name,target_location))
        
        try:
            ddl = ddl_dump(db_name, table_name, target_location)
            migrate_table(db_name, table_name, "parquet", mnt_location)
        except Exception as e:
            print("Error migrating parquet table {}.{}; Exception: {}".format(db_name, table_name, str(e)))
            log_migration(logging_table, db_name, target_location, table_name, provider, table_type, None, -1, str(e), None)
            return
        
        # log migration
        log_migration(logging_table, db_name, target_location, table_name, provider, table_type, None, 1, "success", ddl)
        
    elif provider.lower() in ["com.databricks.spark.csv", "csv"]:
        print("Moving csv table {}.{} to location {}".format(db_name,table_name,target_location))
        
        try:
            ddl = ddl_dump(db_name, table_name, target_location)
            migrate_table(db_name, table_name, "csv", mnt_location)
        except Exception as e:
            print("Error migrating csv table {}.{}; Exception: {}".format(db_name, table_name, str(e)))
            log_migration(logging_table, db_name, target_location, table_name, provider, table_type, None, -1, str(e), None)
            return
        
        # log migration
        log_migration(logging_table, db_name, target_location, table_name, provider, table_type, None, 1, "success", ddl)
        
    else:
        print("Found unknown provider {} for table {}.{}".format(provider,db_name,table_name))
        log_migration(logging_table, db_name, target_location, table_name, provider, table_type, None, -1, "unknown provider", None)

def migrate_table(db_name, table_name, fmt, location):
    try:
        partitions = spark.sql("SHOW PARTITIONS {}.{}".format(db_name, table_name)).columns
        spark.read.table("{}.{}".format(db_name, table_name)).write.format(fmt).mode("overwrite").option("mergeSchema", True).partitionBy(partitions).save(location)
    except:
        spark.read.table("{}.{}".format(db_name, table_name)).write.format(fmt).mode("overwrite").option("mergeSchema", True).save(location)
    
def ddl_dump(db_name, table_name, target_location):
    ddl = spark.sql(f'SHOW CREATE TABLE {db_name}.{table_name}').first()[0]
    ddl = ddl.replace('CREATE TABLE', 'CREATE SCHEMA IF NOT EXISTS {};\nCREATE TABLE IF NOT EXISTS'.format(db_name))

    pattern_loc = re.compile(r"LOCATION\s+'(.*)'")
    pattern_properties = re.compile(r"TBLPROPERTIES\s+\(")
    pattern_options = re.compile(r"OPTIONS\s+\(")
    
    found_loc = False
    if re.search(pattern_loc, ddl):
        ddl = re.sub(pattern_loc, f"LOCATION '{target_location}'", ddl)
        found_loc = True
        
    if re.search(pattern_properties, ddl):
        # used to avoid properties difference when bootstrapping
        if found_loc:
            ddl = re.sub(pattern_properties, f";\nALTER TABLE {db_name}.{table_name} SET TBLPROPERTIES (", ddl)
        else:
            ddl = re.sub(pattern_properties, f"LOCATION '{target_location}';\nALTER TABLE {db_name}.{table_name} SET TBLPROPERTIES (", ddl)
            found_loc = True
            
    if re.search(pattern_options, ddl) and not found_loc:
        ddl = re.sub(pattern_options, f"LOCATION '{target_location}';\nOPTIONS (", ddl)
        found_loc = True
            
    if not found_loc:
        ddl += f"LOCATION '{target_location}'"
    
    return ddl
    
def log_migration(logging_table, db_name, target_location, table_name, table_provider, table_type_at_source, cloned_table_name, status_code, reason, ddl):
    """
    Helper function to log migration status to the tracking table.
    """
    
    if status_code == -1:
        status = "unmigrated"
    elif status_code == 0:
        status = "skipped"
    elif status_code == 1:
        status = "migrated"
    elif status_code == 2:
        status = "bootstrap_failed" # should only exist if table has been bootstrapped on the destination WS
    elif status_code == 3:
        status = "bootstrapped_on_dest" # should only exist if table has been bootstrapped on the destination WS
    else:
        status = "unknown"
    
    try:
        ddl = ddl.replace("\"", "\\\"") if ddl else ddl
        reason = reason.replace("\"", "\\\"") if reason else reason
        
        spark.sql(f"""INSERT INTO TABLE {logging_table} VALUES ("{db_name}","{table_name}","{ddl}","{cloned_table_name}","{table_type_at_source}","{target_location}","{table_provider}","{status}",{status_code},"{reason}","{datetime.datetime.now()}");""")
    except Exception as e:
        print("Failed to save record into table '{}.{}'. Reason: {}".format(migration_db, tracking_table, str(e)))

# COMMAND ----------

import os
from concurrent.futures import ThreadPoolExecutor
import itertools
import pyspark.sql.functions as F
import datetime

def migrate_databases(list_tables, migration_db, tracking_table, parallelization):
    """
    This function will move data in between object storages based on list_tables. Depending on the type of table it will:
    a. For Delta -> It will create a deep cloned table pointing to the defined target object storage 
    b. For Non-Delta -> It will overwrite the content of the defined target object storage
    Source Data will be left in-place.
    Parallelization can. be increased by setting parallelization to a higher number.
    Migration results will be written to tracking_table.
    """
    
    total_tables = 0
    start_time = datetime.datetime.now()
    
    for db_object in list_tables.select("source_database").distinct().collect():
        
        # List all tables for this database
        table_list = list_tables.where(F.col("source_database") == F.lit(db_object.source_database)).collect()
        total_tables += len(table_list)
        
        # launch parallelization executors to migrate tables
        print("Beginning migration of database {}".format(db_object.source_database))
        with ThreadPoolExecutor(max_workers = int(parallelization)) as executor:
            executor.map(exec_migrate_table, table_list, itertools.repeat(migration_db), itertools.repeat(tracking_table))
            
    # print details of migration; see table below for full results
    num_migrated_tables = spark.sql("SELECT * FROM {}.{} WHERE status_code = 1 AND timestamp > '{}'".format(migration_db, tracking_table, start_time)).count()
    print("Migrated {} out of {} tables.".format(num_migrated_tables, total_tables))
    print("See table {}.{} for details on migrated tables.".format(migration_db, tracking_table))

# COMMAND ----------

# import pyspark.sql.functions as F 
from pyspark.sql import DataFrame

def parse_input_csv(csv_path) -> DataFrame:
    """
    Convert input csv to expected dataframe columns
    
    Args:
        csv_path (string): path to the csv with tables to be migrated
    Return:
        list_tables (DataFrame): Dataframe containing all tables to be migrated
    """
    
    new_cols = {
                   "source_database": F.split(F.col("name"), "\.")[0],
                   "source_table": F.split(F.col("name"), "\.")[1],
                   "target_location": "newLocation"
               }
    
    list_tables = (spark.read
                        .format("csv")
                        .option("header", True)
                        .load(config_file_dbfs_location)
                        .withColumns(new_cols)
                        .select(list(new_cols.keys())))
    
    return list_tables


def create_migration_zone(migration_db, tracking_table, tracking_table_location):
    """
    Create schema and table for storing migration metadata and data
    
    Args:
        migration_db (string): Database for the migration zone
        tracking_table (string): Migration tracking table
        tracking_table_location (string): Migration tracking table location
    """
    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_db}")

    # Create tracking schema and table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {migration_db}.{tracking_table} 
            ({tracking_table_schema})
        USING delta
        LOCATION '{tracking_table_location}'
    """)

# COMMAND ----------

# check for not allowed database or table names
pattern = re.compile("^(?:[a-zA-Z0-9_])+$")
if not re.match(pattern, tracking_table):
    raise Exception(f"tracking_table input does not match the required format: {pattern.pattern}")
elif not re.match(pattern, migration_db):
    raise Exception(f"migration_db input does not match the required format: {pattern.pattern}")

# Parse input csv
list_tables = parse_input_csv(config_file_dbfs_location)
               
# Create migration zone schema and table
create_migration_zone(migration_db, tracking_table, tracking_table_location)

# Thread setup for parallelizing the migration
migrate_databases(list_tables, migration_db, tracking_table, parallelization)

# COMMAND ----------

# CAUTION TO UNCOMMENT THIS CELL
# It will cause the metadata for the migration zone to be deleted. Data are preserved.
# spark.sql(f"DROP SCHEMA IF EXISTS {migration_db} CASCADE")
