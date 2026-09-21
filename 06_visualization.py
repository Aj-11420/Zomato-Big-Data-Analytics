from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    round as spark_round,
    desc
)

import matplotlib.pyplot as plt
import pandas as pd


# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("Zomato Data Visualization") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# HDFS INPUT PATH
# ==========================================

input_path = (
    "hdfs://localhost:9000/user/ajs_cachyos/"
    "zomato_project/cleaned_data"
)


print("\n==============================")
print("ZOMATO DATA VISUALIZATION")
print("==============================\n")


# ==========================================
# READ CLEANED DATA FROM HDFS
# ==========================================

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)


print("Dataset loaded successfully.")
print("Total Records:", df.count())


# ==========================================
# CREATE LOCAL OUTPUT DIRECTORY
# ==========================================

import os

output_dir = "visualizations"

os.makedirs(output_dir, exist_ok=True)


# ==========================================
# VISUALIZATION 1
# TOP 10 CITIES BY RESTAURANT COUNT
# ==========================================

print("\nCreating Visualization 1...")

city_analysis = df.groupBy("City") \
    .agg(
        count("*").alias("Total Restaurants")
    ) \
    .orderBy(desc("Total Restaurants")) \
    .limit(10)

city_pd = city_analysis.toPandas()

plt.figure(figsize=(10, 6))

plt.bar(
    city_pd["City"],
    city_pd["Total Restaurants"]
)

plt.xlabel("City")
plt.ylabel("Number of Restaurants")
plt.title("Top 10 Cities by Number of Restaurants")

plt.xticks(rotation=45, ha="right")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/top_cities_restaurants.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 2
# AVERAGE RATING BY TOP CITIES
# ==========================================

print("Creating Visualization 2...")

rating_city = df.groupBy("City") \
    .agg(
        spark_round(
            avg("Aggregate rating"),
            2
        ).alias("Average Rating")
    ) \
    .orderBy(desc("Average Rating")) \
    .limit(10)

rating_city_pd = rating_city.toPandas()

plt.figure(figsize=(10, 6))

plt.bar(
    rating_city_pd["City"],
    rating_city_pd["Average Rating"]
)

plt.xlabel("City")
plt.ylabel("Average Rating")
plt.title("Top 10 Cities by Average Restaurant Rating")

plt.xticks(rotation=45, ha="right")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/top_cities_average_rating.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 3
# ONLINE DELIVERY DISTRIBUTION
# ==========================================

print("Creating Visualization 3...")

delivery_analysis = df.groupBy(
    "Has Online delivery"
).agg(
    count("*").alias("Total Restaurants")
)

delivery_pd = delivery_analysis.toPandas()

plt.figure(figsize=(7, 7))

plt.pie(
    delivery_pd["Total Restaurants"],
    labels=delivery_pd["Has Online delivery"],
    autopct="%1.1f%%",
    startangle=90
)

plt.title("Online Delivery Availability")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/online_delivery_distribution.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 4
# TABLE BOOKING DISTRIBUTION
# ==========================================

print("Creating Visualization 4...")

booking_analysis = df.groupBy(
    "Has Table booking"
).agg(
    count("*").alias("Total Restaurants")
)

booking_pd = booking_analysis.toPandas()

plt.figure(figsize=(7, 7))

plt.pie(
    booking_pd["Total Restaurants"],
    labels=booking_pd["Has Table booking"],
    autopct="%1.1f%%",
    startangle=90
)

plt.title("Table Booking Availability")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/table_booking_distribution.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 5
# PRICE RANGE DISTRIBUTION
# ==========================================

print("Creating Visualization 5...")

price_range_analysis = df.groupBy(
    "Price range"
).agg(
    count("*").alias("Total Restaurants")
).orderBy(
    "Price range"
)

price_pd = price_range_analysis.toPandas()

plt.figure(figsize=(8, 6))

plt.bar(
    price_pd["Price range"].astype(str),
    price_pd["Total Restaurants"]
)

plt.xlabel("Price Range")
plt.ylabel("Number of Restaurants")
plt.title("Restaurant Distribution by Price Range")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/price_range_distribution.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 6
# RATING CATEGORY DISTRIBUTION
# ==========================================

print("Creating Visualization 6...")

rating_text_analysis = df.groupBy(
    "Rating text"
).agg(
    count("*").alias("Total Restaurants")
).orderBy(
    desc("Total Restaurants")
)

rating_text_pd = rating_text_analysis.toPandas()

plt.figure(figsize=(10, 6))

plt.bar(
    rating_text_pd["Rating text"],
    rating_text_pd["Total Restaurants"]
)

plt.xlabel("Rating Category")
plt.ylabel("Number of Restaurants")
plt.title("Restaurant Distribution by Rating Category")

plt.xticks(rotation=45, ha="right")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/rating_category_distribution.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 7
# AVERAGE COST BY PRICE RANGE
# ==========================================

print("Creating Visualization 7...")

cost_analysis = df.groupBy(
    "Price range"
).agg(
    spark_round(
        avg("Average Cost for two"),
        2
    ).alias("Average Cost")
).orderBy(
    "Price range"
)

cost_pd = cost_analysis.toPandas()

plt.figure(figsize=(8, 6))

plt.bar(
    cost_pd["Price range"].astype(str),
    cost_pd["Average Cost"]
)

plt.xlabel("Price Range")
plt.ylabel("Average Cost for Two")
plt.title("Average Cost for Two by Price Range")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/average_cost_by_price_range.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 8
# TOP 10 MOST VOTED RESTAURANTS
# ==========================================

print("Creating Visualization 8...")

votes_analysis = df.select(
    "Restaurant Name",
    "Votes"
).orderBy(
    desc("Votes")
).limit(10)

votes_pd = votes_analysis.toPandas()

# Reverse order for better horizontal bar display
votes_pd = votes_pd.sort_values(
    "Votes",
    ascending=True
)

plt.figure(figsize=(10, 7))

plt.barh(
    votes_pd["Restaurant Name"],
    votes_pd["Votes"]
)

plt.xlabel("Number of Votes")
plt.ylabel("Restaurant Name")
plt.title("Top 10 Most Voted Restaurants")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/top_voted_restaurants.png",
    dpi=300
)

plt.close()


# ==========================================
# VISUALIZATION 9
# RATING VS VOTES
# ==========================================

print("Creating Visualization 9...")

scatter_data = df.select(
    "Aggregate rating",
    "Votes"
).filter(
    col("Aggregate rating").isNotNull() &
    col("Votes").isNotNull()
)

scatter_pd = scatter_data.toPandas()

plt.figure(figsize=(10, 6))

plt.scatter(
    scatter_pd["Aggregate rating"],
    scatter_pd["Votes"],
    alpha=0.5
)

plt.xlabel("Aggregate Rating")
plt.ylabel("Votes")
plt.title("Relationship Between Restaurant Rating and Votes")

plt.tight_layout()

plt.savefig(
    f"{output_dir}/rating_vs_votes.png",
    dpi=300
)

plt.close()


# ==========================================
# DISPLAY COMPLETION MESSAGE
# ==========================================

print("\n==============================")
print("VISUALIZATION COMPLETED")
print("==============================")

print("\nGenerated Visualization Files:")

visualization_files = [
    "top_cities_restaurants.png",
    "top_cities_average_rating.png",
    "online_delivery_distribution.png",
    "table_booking_distribution.png",
    "price_range_distribution.png",
    "rating_category_distribution.png",
    "average_cost_by_price_range.png",
    "top_voted_restaurants.png",
    "rating_vs_votes.png"
]

for file in visualization_files:
    print(f"✓ {output_dir}/{file}")


# ==========================================
# STOP SPARK SESSION
# ==========================================

spark.stop()

print("\nSpark session stopped successfully.")