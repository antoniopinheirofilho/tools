# Databricks notebook source
# MAGIC %md ## Utility to Bootstrap Migrated DBFS files

# COMMAND ----------

# widgets utilities
dbutils.widgets.removeAll() 

dbutils.widgets.text("s3_bucket", "", "S3 bucket")
dbutils.widgets.text("parallelization", "", "Parallization Threads")

# COMMAND ----------

# S3 Mount Point
s3_bucket = dbutils.widgets.get("s3_bucket")

# The number of threads which will execute this operation
parallelization = dbutils.widgets.get("parallelization")

# COMMAND ----------

# parallel dbutils cp feature
spark.conf.set("spark.databricks.service.dbutils.fs.parallel.enabled", True)

# COMMAND ----------

# Base dir to migrate DBFS files to
base_dir = f"{s3_bucket}/dbfs_migration"

# list of dirs to be migrated
dirs_list = [
    "apps",
    "apps-dev",
    "dbfs/FileStore",
    "FileStore"
]

for item in dirs_list:
    try:
        dbutils.fs.mkdirs(f"dbfs:/{item}")
    except:
        pass

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import itertools

def exec_migrate_folders(src_folder, dst_folder):
    print(f"Migrating Folder {src_folder}/{dst_folder} to dbfs:/{dst_folder}")
    dbutils.fs.cp(f"{src_folder}/{dst_folder}", f"dbfs:/{dst_folder}", recurse=True)

with ThreadPoolExecutor(max_workers = int(parallelization)) as executor:
    executor.map(exec_migrate_folders, itertools.repeat(base_dir), dirs_list)

# COMMAND ----------

# print(dbutils.fs.ls(f"{s3_bucket}/dbfs_migration/"))
# dbutils.fs.rm(f"{mount_point}/dbfs_migration", True)
