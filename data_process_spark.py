import findspark

from pyspark.sql import SparkSession, functions as F

findspark.init("C:\Program Files\Spark\spark-3.3.1-bin-hadoop3")

spark = SparkSession.builder \
    .master("yarn") \
    .appName("Data Processing with Spark") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()
    
# Read data

thy = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .csv("datasets/thy_data.txt")
    
thy.show(5)
thy.columns
thy.count()
thy.printSchema()

thy.select("SEASON").distinct().show()

thy.select(F.countDistinct("Origin").alias("Origin nunique")).show()

seasonal_statistics = thy.groupBy(F.col("SEASON")) \
    .agg(F.sum("PSGR_COUNT").alias("TOTAL_PSGR_COUNT"),
         F.avg("PSGR_COUNT").alias("AVG_PSGR_COUNT"),
         F.count("PSGR_COUNT").alias("COUNT")) 
seasonal_statistics.show() 

summer_statistic = thy.filter("SEASON == 'SUMMER'").sort(F.col("PSGR_COUNT"), ascending=False)
summer_statistic.show(5)

season_origin = thy.groupBy(F.col("SEASON"), F.col("ORIGIN")) \
    .agg(F.sum("PSGR_COUNT").alias("COUNT")).sort(F.col("COUNT"), ascending=False)
season_origin.printSchema()
season_origin.show(5)
    
# Write on Apache Hive

season_origin.write \
    .format("orc") \
    .mode("overwrite") \
    .saveAsTable("thy.season_origin_orc")
    
# Read Table from Apache Hive

spark.sql("Select * from thy.season_origin_orc").limit(5).toPandas()
    
# Write on PostgreSQL

jdbcUrl = "jdbc:postgresql://localhost:5432/thy?user=postgres&password=antalya864GS"

season_origin.write \
    .mode("overwrite") \
    .jdbc(url=jdbcUrl,
          table="season_origin",
          mode="overwrite",
          properties={"driver": "org.postgresql.Driver"})
    
# Read Table from PostgreSQL

spark.read.jdbc(url=jdbcUrl,
                table="season_origin",
                properties={"driver": "org.postgresql.Driver"}).limit(5).toPandas()

# Write on Delta Lake

from delta.tables import *

deltaPath = "/user/talha/delta_db/thy_season"

season_origin.write.format("delta").mode("overwrite").save(deltaPath)

# Read Table from Delta Lake

season_origin_delta = DeltaTable.forPath(spark, deltaPath)
season_origin_delta.toDF().show(5)

spark.stop()
