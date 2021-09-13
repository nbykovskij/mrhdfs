from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, max

spark = SparkSession \
    .builder \
    .appName("advanced_spark_ht_2") \
    .getOrCreate()

csv_df = spark.read.option('header', True).option('inferSchema', True).csv('owid-covid-data.csv')

df_select = csv_df.filter((csv_df['date'] <= '2021-03-31') & (csv_df['date'] >= '2021-03-25'))\
            .filter(~csv_df.iso_code.contains('OWID'))\
            .select('date', 'location', 'new_cases')

df_groupby = df_select.groupBy('location').agg(max("new_cases").alias('max_new_cases'))

df = df_groupby.join(df_select, on='location', how='left')\
            .filter(col('max_new_cases') == col('new_cases'))\
            .select('date', 'location', 'new_cases')\
            .orderBy(col('new_cases').desc())

df.show(10)

"""
+----------+-------------+---------+
|      date|     location|new_cases|
+----------+-------------+---------+
|2021-03-25|       Brazil| 100158.0|
|2021-03-26|United States|  77321.0|
|2021-03-31|        India|  72330.0|
|2021-03-31|       France|  59054.0|
|2021-03-31|       Turkey|  39302.0|
|2021-03-26|       Poland|  35145.0|
|2021-03-31|      Germany|  25014.0|
|2021-03-26|        Italy|  24076.0|
|2021-03-25|         Peru|  19206.0|
|2021-03-26|      Ukraine|  18226.0|
+----------+-------------+---------+
