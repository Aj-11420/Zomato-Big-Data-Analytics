# Zomato Restaurant Data Analysis Using Hadoop and PySpark

## 📌 Project Overview

This project performs Big Data analysis on the Zomato restaurant dataset using:

- Hadoop Distributed File System (HDFS)
- Apache Spark
- PySpark
- Spark SQL
- Python
- Matplotlib

The project demonstrates a complete Big Data processing pipeline, including data storage, cleaning, transformation, analysis, SQL queries, and visualization.

---

# 🏗️ Project Architecture

                 ZOMATO DATASET
                       │
                       ▼
               ┌───────────────┐
               │     HDFS      │
               │ Data Storage  │
               └───────┬───────┘
                       │
                       ▼
               ┌───────────────┐
               │    PySpark    │
               │ Data Cleaning │
               └───────┬───────┘
                       │
                       ▼
               ┌───────────────┐
               │ Cleaned Data  │
               │     HDFS      │
               └───────┬───────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   ┌────────────┐ ┌───────────┐ ┌──────────────┐
   │ Analysis   │ │ Spark SQL │ │Visualization │
   └────────────┘ └───────────┘ └──────────────┘
          │            │            │
          └────────────┼────────────┘
                       ▼
                FINAL INSIGHTS


                  TECHNOLOGIES USED

| Technology   | Purpose                     |
| ------------ | --------------------------- |
| Hadoop       | Distributed data storage    |
| HDFS         | Store large datasets        |
| Apache Spark | Distributed data processing |
| PySpark      | Python interface for Spark  |
| Spark SQL    | SQL-based data analysis     |
| Python       | Programming language        |
| Matplotlib   | Data visualization          |



📊 Dataset

The project uses the Zomato Restaurant Dataset.

The dataset contains information such as:

Restaurant ID
Restaurant Name
Country Code
City
Address
Locality
Cuisines
Average Cost for Two
Currency
Table Booking Availability
Online Delivery Availability
Price Range
Aggregate Rating
Rating Color
Rating Text
Number of Votes


The Hadoop filesystem configuration used in this project is:
              hdfs://localhost:9000   