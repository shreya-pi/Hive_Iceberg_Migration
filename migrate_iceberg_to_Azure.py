# migrate_table.py
from pyspark.sql import SparkSession

# The name of your source table, as known by the Hive catalog
source_table_name = "hive_catalog.itest.sales_iceberg_table"

# The name you want for the new table in the destination
destination_table_name = "azure_blob_catalog.itest.sales_iceberg_table_migrated"

# Initialize Spark Session (the configurations will be provided in spark-submit)
spark = SparkSession.builder.appName("IcebergTableMigration").getOrCreate()

# 1. Read the complete source table using the on-prem catalog.
#    Spark reads the Iceberg metadata, finds all the correct parquet files,
#    and loads them into a DataFrame.
print(f"Reading from source table: {source_table_name}")
source_df = spark.read.table(source_table_name)
source_df.printSchema()

# 2. Write the DataFrame to the destination as a new Iceberg table.
#    Spark writes new parquet files to Azure and creates the new,
#    correct Iceberg metadata in the location defined by the destination catalog.
print(f"Writing to destination table: {destination_table_name}")
source_df.writeTo(destination_table_name).create()

print("Migration complete!")
spark.stop()
