from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "person").option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()

# Define the calculation of age
df_age = df.withColumn("DOB", to_date(col("DOB"), "M/d/yyyy")) \
    .withColumn("age", floor(datediff(current_date(), col("DOB")) / 365))
df_age.show(10)


sorted_df.write.mode("overwrite").saveAsTable("bigdata_nov_2024.people_sujay")
print("Successfully Load to Hive")
