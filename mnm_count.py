import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())
mnm_file = "LearningSparkV2/chapter2/py/src/data/mnm_dataset.csv"

# read the file into a SPark Dataframe using the CSV format
# by inferring the schema and specifying that the
# file contains a header, which provides column names for
# comma-separated fields
mnm_df =  (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))

# we use the DataFrame high-level APIs. Note that we do not
# use RDDs at all. Because some of Spark's functions return
# the same object, we can chain function calls.
# 1. Select from the DataFrame the fields "State", "Color",
# and "Count".
# 2. Since we want to group each state and itws M&M color 
# count we use groupBy()
# 3. Aggregate counts of all colors and groupBy() State
# and Color 
# 4. orderBy() in descending order
count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False))

# Show the resulting aggregations for all the states and
# colors; a total count of each color per state
# Note show() is an action which will trigger the above
# query to be executed.
count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))

# While the above code aggregated and counted for all the
# states, what if we just want to see the data for single
# state e.g. CA?
# 1. Select from all rows in the DataFrame
# 2. Filter only CA state
# 3. groupBy() State and Color as we did above
# 4. Aggregate the counts for each color
# 5. orderBy() in descending order
# Find the aggregate count for California by fitlering
ca_count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False))
# Show the resulting aggregation for California
# As above show() is an action that will trigger the 
# execution of the entire computation
ca_count_mnm_df.show(n=10, truncate=False)
# stop the SparkSession
spark.stop()

