from pyspark.sql import *
from pyspark.sql.functions import *

try:
    # Initialize SparkSession with Hive support
    spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

    # Fetch the maximum sale_id from Hive table
    max_id = spark.sql("SELECT max(sale_id) FROM bigdata_nov_2024.sujay_sales")
    m_id = max_id.collect()[0][0]

    # Ensure m_id is not None before proceeding
    if m_id is None:
        m_id = 0

    # Formulate the query to fetch new data from PostgreSQL
    query = 'SELECT * FROM sujay_sales WHERE sale_id > ' + str(m_id)

    # Read data from PostgreSQL using the query
    more_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()

    # Append the new data to Hive table
    more_data.write.mode("append").saveAsTable("bigdata_nov_2024.sujay_sales")
    print("Successfully Load to Hive")

except Exception as e:
    # Print the error message
    print("Error occurred:", str(e))
