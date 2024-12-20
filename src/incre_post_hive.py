from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()
max_id = spark.sql("SELECT max(sale_id) FROM bigdata_nov_2024.sujay_sales")
m_id = max_id.collect()[0][0]
str(m_id)

query = 'SELECT * FROM sujay_sales WHERE "sale_id" > ' + str(m_id)

more_data = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("query", query) \
    .load()

df_increment.write.mode("append").saveAsTable("bigdata_nov_2024.sujay_sales")
print("Successfully Load to Hive")