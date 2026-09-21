from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    mean,
    expr
)

# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("Zomato Data Cleaning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# INPUT AND OUTPUT PATHS
# ==========================================

input_path = (
    "hdfs://localhost:9000/"
    "user/ajs_cachyos/"
    "zomato_project/input/zomato.csv"
)

output_path = (
    "hdfs://localhost:9000/"
    "user/ajs_cachyos/"
    "zomato_project/cleaned_data"
)


# ==========================================
# DATA CLEANING
# ==========================================

print("\n==============================")
print("DATA CLEANING")
print("==============================\n")


# ==========================================
# READ RAW DATA
# ==========================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "PERMISSIVE")
    .csv(input_path)
)


# ==========================================
# RECORDS BEFORE CLEANING
# ==========================================

records_before = df.count()

print("Records before cleaning:", records_before)


# ==========================================
# REMOVE LEADING/TRAILING SPACES
# ==========================================

for column_name in df.columns:

    df = df.withColumn(
        column_name,
        trim(col(column_name))
    )


# ==========================================
# REMOVE COMPLETELY EMPTY ROWS
# ==========================================

df = df.dropna(how="all")


# ==========================================
# CONVERT NUMERIC COLUMNS SAFELY
# ==========================================
#
# IMPORTANT:
# We use try_cast instead of normal cast.
#
# If a value is invalid:
#
# "No"                 -> NULL
# "Indian Rupees..."   -> NULL
# "Unknown"            -> NULL
#
# This prevents SparkNumberFormatException.
#
# ==========================================


# Longitude
df = df.withColumn(
    "Longitude",
    expr("try_cast(`Longitude` AS DOUBLE)")
)


# Latitude
df = df.withColumn(
    "Latitude",
    expr("try_cast(`Latitude` AS DOUBLE)")
)


# Average Cost for two
df = df.withColumn(
    "Average Cost for two",
    expr("try_cast(`Average Cost for two` AS DOUBLE)")
)


# Price range
df = df.withColumn(
    "Price range",
    expr("try_cast(`Price range` AS INT)")
)


# Aggregate rating
df = df.withColumn(
    "Aggregate rating",
    expr("try_cast(`Aggregate rating` AS DOUBLE)")
)


# Votes
df = df.withColumn(
    "Votes",
    expr("try_cast(`Votes` AS INT)")
)


# ==========================================
# KEEP IDENTIFIER COLUMNS AS STRING
# ==========================================

df = df.withColumn(
    "Restaurant ID",
    col("Restaurant ID").cast("string")
)

df = df.withColumn(
    "Country Code",
    col("Country Code").cast("string")
)


# ==========================================
# HANDLE MISSING STRING VALUES
# ==========================================

string_columns = [
    "Restaurant ID",
    "Restaurant Name",
    "Country Code",
    "City",
    "Address",
    "Locality",
    "Locality Verbose",
    "Cuisines",
    "Currency",
    "Has Table booking",
    "Has Online delivery",
    "Is delivering now",
    "Switch to order menu",
    "Rating color",
    "Rating text"
]


for column_name in string_columns:

    df = df.withColumn(
        column_name,
        when(
            col(column_name).isNull()
            | (trim(col(column_name)) == ""),
            "Unknown"
        ).otherwise(col(column_name))
    )


# ==========================================
# HANDLE MISSING NUMERIC VALUES
# ==========================================

numeric_columns = [
    "Longitude",
    "Latitude",
    "Average Cost for two",
    "Price range",
    "Aggregate rating",
    "Votes"
]


print("\nHandling missing numeric values...")


for column_name in numeric_columns:

    # Calculate mean safely
    mean_value = (
        df.select(
            mean(col(column_name)).alias("mean_value")
        )
        .first()["mean_value"]
    )

    if mean_value is not None:

        if column_name in ["Price range", "Votes"]:

            fill_value = int(round(mean_value))

        else:

            fill_value = float(mean_value)

        df = df.fillna(
            {
                column_name: fill_value
            }
        )


# ==========================================
# REMOVE DUPLICATE RECORDS
# ==========================================

records_before_duplicates = df.count()

df = df.dropDuplicates()

records_after_duplicates = df.count()

duplicates_removed = (
    records_before_duplicates
    - records_after_duplicates
)


# ==========================================
# REMOVE ROWS WITHOUT IMPORTANT DATA
# ==========================================

df = df.dropna(
    subset=[
        "Restaurant ID",
        "Restaurant Name",
        "City"
    ]
)


# ==========================================
# RECORDS AFTER CLEANING
# ==========================================

records_after = df.count()

print(
    "\nRecords after cleaning:",
    records_after
)

print(
    "Duplicate records removed:",
    duplicates_removed
)


# ==========================================
# DISPLAY CLEANED DATA SCHEMA
# ==========================================

print("\nCleaned Dataset Schema:")

df.printSchema()


# ==========================================
# DISPLAY FIRST 5 CLEANED RECORDS
# ==========================================

print("\nFirst 5 Cleaned Records:")

df.show(
    5,
    truncate=True
)


# ==========================================
# VERIFY NUMERIC COLUMNS
# ==========================================

print("\nNumeric Column Validation:")

df.select(
    "Longitude",
    "Latitude",
    "Average Cost for two",
    "Price range",
    "Aggregate rating",
    "Votes"
).show(5)


# ==========================================
# SAVE CLEANED DATA TO HDFS
# ==========================================

print("\nSaving cleaned data to HDFS...")


(
    df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(output_path)
)


# ==========================================
# SUCCESS MESSAGE
# ==========================================

print("\n==============================")
print("DATA CLEANING COMPLETED SUCCESSFULLY")
print("==============================")

print("\nOutput saved to:")
print(output_path)


# ==========================================
# STOP SPARK
# ==========================================

spark.stop()