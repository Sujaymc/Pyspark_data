from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()
max_id = spark.sql("SELECT max(sale_id) FROM bigdata_nov_2024.sujay_sales")
m_id = max_id.collect()[0][0]
str(m_id)

query = 'SELECT * FROM bigdata_nov_2024.sujay_sales WHERE sale_id > ' + str(m_id)

more_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()

more_data.write.mode("append").saveAsTable("bigdata_nov_2024.sujay_sales")
print("Successfully Load to Hive")
