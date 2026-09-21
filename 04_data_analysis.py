from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    round as spark_round,
    desc
)

# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("Zomato Data Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# HDFS INPUT PATH
# ==========================================

input_path = "hdfs://localhost:9000/user/ajs_cachyos/zomato_project/cleaned_data"


# ==========================================
# READ CLEANED DATA
# ==========================================

print("\n==============================")
print("ZOMATO DATA ANALYSIS")
print("==============================\n")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)


print("Dataset Schema:")
df.printSchema()

print("\nTotal Records:")
print(df.count())


# ==========================================
# ANALYSIS 1
# TOTAL RESTAURANTS BY CITY
# ==========================================

print("\n==============================")
print("1. TOP 10 CITIES BY NUMBER OF RESTAURANTS")
print("==============================")

city_analysis = df.groupBy("City") \
    .agg(
        count("*").alias("Total Restaurants")
    ) \
    .orderBy(desc("Total Restaurants"))

city_analysis.show(10, truncate=False)


# ==========================================
# ANALYSIS 2
# AVERAGE RATING BY CITY
# ==========================================

print("\n==============================")
print("2. TOP 10 CITIES BY AVERAGE RATING")
print("==============================")

rating_by_city = df.groupBy("City") \
    .agg(
        spark_round(
            avg("Aggregate rating"), 2
        ).alias("Average Rating")
    ) \
    .orderBy(desc("Average Rating"))

rating_by_city.show(10, truncate=False)


# ==========================================
# ANALYSIS 3
# MOST POPULAR CUISINES
# ==========================================

print("\n==============================")
print("3. MOST POPULAR CUISINES")
print("==============================")

cuisine_analysis = df.groupBy("Cuisines") \
    .agg(
        count("*").alias("Number of Restaurants")
    ) \
    .orderBy(desc("Number of Restaurants"))

cuisine_analysis.show(10, truncate=False)


# ==========================================
# ANALYSIS 4
# ONLINE DELIVERY ANALYSIS
# ==========================================

print("\n==============================")
print("4. ONLINE DELIVERY ANALYSIS")
print("==============================")

online_delivery = df.groupBy("Has Online delivery") \
    .agg(
        count("*").alias("Number of Restaurants")
    ) \
    .orderBy(desc("Number of Restaurants"))

online_delivery.show()


# ==========================================
# ANALYSIS 5
# TABLE BOOKING ANALYSIS
# ==========================================

print("\n==============================")
print("5. TABLE BOOKING ANALYSIS")
print("==============================")

table_booking = df.groupBy("Has Table booking") \
    .agg(
        count("*").alias("Number of Restaurants")
    )

table_booking.show()


# ==========================================
# ANALYSIS 6
# AVERAGE COST BY PRICE RANGE
# ==========================================

print("\n==============================")
print("6. AVERAGE COST BY PRICE RANGE")
print("==============================")

price_range_analysis = df.groupBy("Price range") \
    .agg(
        spark_round(
            avg("Average Cost for two"), 2
        ).alias("Average Cost")
    ) \
    .orderBy("Price range")

price_range_analysis.show()


# ==========================================
# ANALYSIS 7
# RATING CATEGORY ANALYSIS
# ==========================================

print("\n==============================")
print("7. RESTAURANTS BY RATING CATEGORY")
print("==============================")

rating_text_analysis = df.groupBy("Rating text") \
    .agg(
        count("*").alias("Number of Restaurants")
    ) \
    .orderBy(desc("Number of Restaurants"))

rating_text_analysis.show()


# ==========================================
# ANALYSIS 8
# TOP 10 MOST VOTED RESTAURANTS
# ==========================================

print("\n==============================")
print("8. TOP 10 MOST VOTED RESTAURANTS")
print("==============================")

top_voted = df.select(
    "Restaurant Name",
    "City",
    "Aggregate rating",
    "Votes"
).orderBy(
    desc("Votes")
)

top_voted.show(10, truncate=False)


# ==========================================
# ANALYSIS 9
# TOP RATED RESTAURANTS
# ==========================================

print("\n==============================")
print("9. TOP 10 HIGHEST RATED RESTAURANTS")
print("==============================")

top_rated = df.select(
    "Restaurant Name",
    "City",
    "Cuisines",
    "Aggregate rating",
    "Votes"
).filter(
    col("Aggregate rating") > 0
).orderBy(
    desc("Aggregate rating"),
    desc("Votes")
)

top_rated.show(10, truncate=False)


# ==========================================
# ANALYSIS 10
# CURRENCY DISTRIBUTION
# ==========================================

print("\n==============================")
print("10. RESTAURANTS BY CURRENCY")
print("==============================")

currency_analysis = df.groupBy("Currency") \
    .agg(
        count("*").alias("Number of Restaurants")
    ) \
    .orderBy(desc("Number of Restaurants"))

currency_analysis.show(truncate=False)


# ==========================================
# SAVE ANALYSIS RESULTS TO HDFS
# ==========================================

output_path = "hdfs://localhost:9000/user/ajs_cachyos/zomato_project/analysis_results"


# Save City Analysis

city_analysis.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path + "/city_analysis")


# Save Rating Analysis

rating_by_city.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path + "/rating_by_city")


# Save Cuisine Analysis

cuisine_analysis.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path + "/cuisine_analysis")


# Save Online Delivery Analysis

online_delivery.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path + "/online_delivery_analysis")


# Save Price Range Analysis

price_range_analysis.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path + "/price_range_analysis")


print("\n==============================")
print("ANALYSIS COMPLETED SUCCESSFULLY")
print("==============================")

print("\nResults saved to:")
print(output_path)


# ==========================================
# STOP SPARK SESSION
# ==========================================

spark.stop()