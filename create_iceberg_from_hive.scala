// PASTE THIS ENTIRE BLOCK INTO SPARK-SHELL

// 2) Rebuild SparkSession with Iceberg catalog configs
import org.apache.spark.sql.SparkSession
import scala.language.postfixOps

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

//Load the existing Hive table into a DataFrame
val hiveDf = spark.table("default.productivity")      
val tableName = "hive_catalog.itest.productivity_iceberg_table"

// 2) Write that DataFrame into your Iceberg catalog
hiveDf
  .writeTo(tableName)
  .using("iceberg")
  .createOrReplace()                         


// 6) Verify: list tables and show top rows
spark.sql("SHOW TABLES IN hive_catalog.itest").show(false)
spark.table(tableName).show(10, false)

// 7) Exit paste mode
println("Done!")