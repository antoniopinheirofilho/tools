# Databricks notebook source
# MAGIC %md ## Utility to Migrate DBFS files
# MAGIC - Developed by wagner.silveira@databricks.com

# COMMAND ----------

# widgets utilities
dbutils.widgets.removeAll() 

dbutils.widgets.text("mount_point", "", "S3 Mount Name")
dbutils.widgets.text("parallelization", "", "Parallization Threads")

# COMMAND ----------

# S3 Mount Point
mount_point = dbutils.widgets.get("mount_point")

# The number of threads which will execute this operation
parallelization = dbutils.widgets.get("parallelization")

# COMMAND ----------

# parallel dbutils cp feature
spark.conf.set("spark.databricks.service.dbutils.fs.parallel.enabled", True)

# COMMAND ----------

# Base dir to migrate DBFS files to
base_dir = f"{mount_point}/dbfs_migration"

# list of dirs to be migrated
dirs_list = [
    "/apps",
    "/apps-dev",
    "/dbfs/FileStore",
    "/FileStore"
]

try:
    # destination folder exists
    dbutils.fs.ls(base_dir)
except:
    # create destination folder
    dbutils.fs.mkdirs(base_dir)
    pass

for item in dirs_list:
    try:
        dbutils.fs.mkdirs(f"{base_dir}/{item}")
    except:
        pass

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import itertools

def exec_migrate_folders(src_folder, dst_folder):
    print(f"Migrating Folder {src_folder} to {dst_folder}")
    dbutils.fs.cp(f"dbfs:{src_folder}", f"{dst_folder}{src_folder}", recurse=True)

with ThreadPoolExecutor(max_workers = int(parallelization)) as executor:
    executor.map(exec_migrate_folders, dirs_list, itertools.repeat(base_dir))

# COMMAND ----------

# print(dbutils.fs.ls(f"{mount_point}/dbfs_migration/apps-dev"))
# dbutils.fs.rm(f"{mount_point}/dbfs_migration", True)
