from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "sujay_sales").option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()


df.write.mode("overwrite").saveAsTable("bigdata_nov_2024.sujay_sales")
print("Successfully Load to Hive")
