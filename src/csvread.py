from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Postgres JDBC Load Example") \
    .getOrCreate()

# PostgreSQL JDBC connection details
jdbc_url = "jdbc:postgresql://18.132.73.146:5432/testdb"
db_properties = {
    "driver": "org.postgresql.Driver",
    "user": "consultants",
    "password": "Welcomeitc@2022"
}

# Load data from the PostgreSQL database into a PySpark DataFrame
df_post = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "person") \
    .option("driver", db_properties["driver"]) \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .load()

# Print the schema of the DataFrame
df_post.printSchema()

# Optionally, show the first few rows
df_post.show()
