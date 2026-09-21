from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Zomato Big Data Analysis") \
    .getOrCreate()

# HDFS dataset path
file_path = "hdfs://localhost:9000/user/ajs_cachyos/zomato_project/input/zomato.csv"

# Read dataset
df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# ==========================================
# 1. BASIC DATASET INFORMATION
# ==========================================

print("\n========== ZOMATO DATASET INFORMATION ==========")

print("\nTotal Records:", df.count())
print("Total Columns:", len(df.columns))

print("\nColumn Names:")
print(df.columns)


# ==========================================
# 2. CHECK MISSING VALUES
# ==========================================

print("\n========== MISSING VALUES ==========")

from pyspark.sql.functions import col, when, count

missing_values = df.select([
    count(
        when(col(column).isNull(), column)
    ).alias(column)
    for column in df.columns
])

missing_values.show(truncate=False)


# ==========================================
# 3. CHECK DUPLICATE RECORDS
# ==========================================

print("\n========== DUPLICATE RECORDS ==========")

total_records = df.count()
unique_records = df.dropDuplicates().count()
duplicate_records = total_records - unique_records

print("Total Records:", total_records)
print("Unique Records:", unique_records)
print("Duplicate Records:", duplicate_records)


# ==========================================
# 4. DISPLAY DATA TYPES
# ==========================================

print("\n========== DATASET SCHEMA ==========")

df.printSchema()


# Stop Spark
spark.stop()