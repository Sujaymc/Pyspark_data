from pyspark.sql import *
from pyspark.sql.functions import *


try:
    # Initialize SparkSession with Hive support
    spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

    # Read data from PostgreSQL
    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "sujay_sales").option("user", "consultants").option("password", "WelcomeItc@2022").load()
    
    # Print the schema of the DataFrame
    df.printSchema()

    # Write the DataFrame to Hive
    df.write.mode("overwrite").saveAsTable("bigdata_nov_2024.sujay_sales")
    print("Successfully Load to Hive")

except Exception as e:
    # Print the error message
    print("Error occurred:", str(e))
