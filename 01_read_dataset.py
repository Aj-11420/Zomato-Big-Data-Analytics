from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Zomato Big Data Analysis") \
    .getOrCreate()

# HDFS dataset path
file_path = "hdfs://localhost:9000/user/ajs_cachyos/zomato_project/input/zomato.csv"

# Read the CSV file
df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Display column names
print("\nColumn Names:")
print(df.columns)

# Display first 5 rows
print("\nFirst 5 Rows:")
df.show(5)

# Display number of records
print("\nTotal Records:", df.count())

# Display dataset structure
print("\nDataset Schema:")
df.printSchema()

# Stop Spark
spark.stop()