// PASTE THIS ENTIRE BLOCK INTO SPARK-SHELL

// 2) Rebuild SparkSession with Iceberg catalog configs
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("IcebergShellExample")
  .master("local[*]")   // adjust or remove for your cluster
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.hive_catalog.type", "hive")
  .config("spark.sql.catalog.hive_catalog.uri", "thrift://localhost:9083")  // update as needed
  .getOrCreate()

import spark.implicits._

// 3) Ensure the Iceberg namespace exists
spark.sql("CREATE DATABASE IF NOT EXISTS hive_catalog.itest")

// 4) Read your CSV
val csvPath   = "hdfs://0.0.0.0:9000/user/hive/warehouse/sales/load_date=2025-06-04/sales.csv"
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(csvPath)

// 5) Write to Iceberg via the V2 API
val tableName = "hive_catalog.itest.sales_iceberg_table"
df.writeTo(tableName)
  .using("iceberg")
  .createOrReplace()

// 6) Verify: list tables and show top rows
spark.sql("SHOW TABLES IN hive_catalog.itest").show(false)
spark.table(tableName).show(10, false)

// 7) Exit paste mode
//- “Ctrl-D” on Linux/macOS, “Ctrl-Z” then Enter on Windows
// Or simply type another single-line command to leave paste mode
println("Done!")



spark.sql("SHOW TBLPROPERTIES hive_catalog.itest.sales_iceberg_table").show(false) ….Iceberg tables expose certain properties under the hood
