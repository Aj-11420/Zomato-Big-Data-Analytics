from pyspark.sql import SparkSession


# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("Zomato Spark SQL Analysis") \
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
print("ZOMATO SPARK SQL ANALYSIS")
print("==============================\n")


# ==========================================
# READ CLEANED DATA
# ==========================================

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)


print("Cleaned Dataset Schema:")
df.printSchema()


print("\nTotal Records:")
print(df.count())


# ==========================================
# CREATE TEMPORARY SQL VIEW
# ==========================================

df.createOrReplaceTempView("zomato")


print("\nTemporary View Created: zomato")


# ==========================================
# QUERY 1
# TOTAL RESTAURANTS BY CITY
# ==========================================

print("\n==============================")
print("1. TOP 10 CITIES BY RESTAURANT COUNT")
print("==============================")

spark.sql("""
    SELECT
        City,
        COUNT(*) AS total_restaurants
    FROM zomato
    GROUP BY City
    ORDER BY total_restaurants DESC
    LIMIT 10
""").show(truncate=False)


# ==========================================
# QUERY 2
# AVERAGE RATING BY CITY
# ==========================================

print("\n==============================")
print("2. TOP 10 CITIES BY AVERAGE RATING")
print("==============================")

spark.sql("""
    SELECT
        City,
        ROUND(AVG(`Aggregate rating`), 2) AS average_rating
    FROM zomato
    GROUP BY City
    ORDER BY average_rating DESC
    LIMIT 10
""").show(truncate=False)


# ==========================================
# QUERY 3
# RESTAURANTS WITH ONLINE DELIVERY
# ==========================================

print("\n==============================")
print("3. ONLINE DELIVERY ANALYSIS")
print("==============================")

spark.sql("""
    SELECT
        `Has Online delivery`,
        COUNT(*) AS total_restaurants
    FROM zomato
    GROUP BY `Has Online delivery`
    ORDER BY total_restaurants DESC
""").show()


# ==========================================
# QUERY 4
# TABLE BOOKING ANALYSIS
# ==========================================

print("\n==============================")
print("4. TABLE BOOKING ANALYSIS")
print("==============================")

spark.sql("""
    SELECT
        `Has Table booking`,
        COUNT(*) AS total_restaurants
    FROM zomato
    GROUP BY `Has Table booking`
    ORDER BY total_restaurants DESC
""").show()


# ==========================================
# QUERY 5
# AVERAGE COST BY PRICE RANGE
# ==========================================

print("\n==============================")
print("5. AVERAGE COST BY PRICE RANGE")
print("==============================")

spark.sql("""
    SELECT
        `Price range`,
        ROUND(AVG(`Average Cost for two`), 2) AS average_cost
    FROM zomato
    GROUP BY `Price range`
    ORDER BY `Price range`
""").show()


# ==========================================
# QUERY 6
# TOP 10 MOST VOTED RESTAURANTS
# ==========================================

print("\n==============================")
print("6. TOP 10 MOST VOTED RESTAURANTS")
print("==============================")

spark.sql("""
    SELECT
        `Restaurant Name`,
        City,
        `Aggregate rating`,
        Votes
    FROM zomato
    ORDER BY Votes DESC
    LIMIT 10
""").show(truncate=False)


# ==========================================
# QUERY 7
# TOP RATED RESTAURANTS
# ==========================================

print("\n==============================")
print("7. TOP 10 HIGHEST RATED RESTAURANTS")
print("==============================")

spark.sql("""
    SELECT
        `Restaurant Name`,
        City,
        Cuisines,
        `Aggregate rating`,
        Votes
    FROM zomato
    WHERE `Aggregate rating` > 0
    ORDER BY `Aggregate rating` DESC, Votes DESC
    LIMIT 10
""").show(truncate=False)


# ==========================================
# QUERY 8
# RATING CATEGORY DISTRIBUTION
# ==========================================

print("\n==============================")
print("8. RESTAURANTS BY RATING CATEGORY")
print("==============================")

spark.sql("""
    SELECT
        `Rating text`,
        COUNT(*) AS total_restaurants
    FROM zomato
    GROUP BY `Rating text`
    ORDER BY total_restaurants DESC
""").show()


# ==========================================
# QUERY 9
# AVERAGE RATING BY ONLINE DELIVERY
# ==========================================

print("\n==============================")
print("9. ONLINE DELIVERY VS AVERAGE RATING")
print("==============================")

spark.sql("""
    SELECT
        `Has Online delivery`,
        ROUND(AVG(`Aggregate rating`), 2) AS average_rating,
        COUNT(*) AS total_restaurants
    FROM zomato
    GROUP BY `Has Online delivery`
    ORDER BY average_rating DESC
""").show()


# ==========================================
# QUERY 10
# AVERAGE RATING BY TABLE BOOKING
# ==========================================

print("\n==============================")
print("10. TABLE BOOKING VS AVERAGE RATING")
print("==============================")

spark.sql("""
    SELECT
        `Has Table booking`,
        ROUND(AVG(`Aggregate rating`), 2) AS average_rating,
        COUNT(*) AS total_restaurants
    FROM zomato
    GROUP BY `Has Table booking`
    ORDER BY average_rating DESC
""").show()


# ==========================================
# STOP SPARK SESSION
# ==========================================

print("\n==============================")
print("SPARK SQL ANALYSIS COMPLETED")
print("==============================\n")

spark.stop()