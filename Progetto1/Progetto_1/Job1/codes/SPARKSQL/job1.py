#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, rank, row_number, desc, explode, split, count
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import datetime
from pyspark.sql.window import Window

def time_to_date(x):
    try:
        return datetime.datetime.utcfromtimestamp(x).strftime('%Y')
    except:
        return 0
    
def count_words(text):
    words = text.split()
    return dict(zip(words, [1]*len(words)))

# Register the function as a UDF
time_to_date_udf = udf(time_to_date)

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark SQL Job1") \
    .getOrCreate()


# define the custom schema explicitly in the code
custom_schema = StructType([
    StructField(name="id", dataType=StringType(), nullable=True),
    StructField(name="product_id", dataType=StringType(), nullable=True),
    StructField(name="user_id", dataType=StringType(), nullable=True),
    StructField(name="profile_name", dataType=StringType(), nullable=True),
    StructField(name="helpfullness_numerator", dataType=StringType(), nullable=True),
    StructField(name="helpfullness_denominator", dataType=StringType(), nullable=True),
    StructField(name="score", dataType=StringType(), nullable=True),
    StructField(name="time", dataType=IntegerType(), nullable=True),
    StructField(name="summary", dataType=StringType(), nullable=True),
    StructField(name="text", dataType=StringType(), nullable=True)])

# import the csv file as a DataFrame passing the custom_schema
original_DF = spark.read.csv(input_filepath, schema=custom_schema).cache()

new_DF = original_DF.withColumn("year", time_to_date_udf(original_DF["time"])).cache()

# GET TOP 10 PRODUCT ID PER YEAR
product_id_count_per_year = new_DF.groupBy("year", "product_id").count().sort("count", ascending=False)
# Define the window specification for partitioning by year and ordering by count descending
window_spec = Window.partitionBy("year").orderBy(desc("count"))
# Add a new column that assigns a row number based on the window specification
df_with_row_number = product_id_count_per_year.withColumn("row_num", row_number().over(window_spec))
# Filter the rows with row number less than or equal to 10
top_10_productid_per_year = df_with_row_number.filter("row_num <= 10").select("year", "product_id").orderBy("year", "row_num")

# GET REVIEWS FOR THE PRODUCT IDS FOUND ABOVE
new_DF = new_DF.join(top_10_productid_per_year, ["year", "product_id"], "inner").select("year", "product_id", "text").cache()

# Split the text column into individual words and explode the resulting array into separate rows
words_df = new_DF.select('year', 'product_id', explode(split('text', ' ')).alias('word')).filter("LENGTH(word) >= 4")

# Count the occurrence of each word per (product_id, year) and order by descending count
counts_df = words_df.groupby('year','product_id', 'word').agg(count('*').alias('count')).orderBy('year','product_id', 'count', ascending=[True, True, False])

# Window function to rank the words by count within each (product_id, year) group
window = Window.partitionBy('year','product_id').orderBy(desc('count'))
counts_df_with_row_number = counts_df.withColumn("row_num", row_number().over(window))

# Add a rank column to the DataFrame and keep only the top 5 words per (product_id, year) group
top_10_word_per_productid_year = counts_df_with_row_number.filter("row_num <= 5").select("year", "product_id", "word", "count").orderBy("year", "product_id", "row_num")
top_10_word_per_productid_year.show()

top_10_word_per_productid_year.write.csv('/prog1/job1/output/SPARK/SPARKSQL.csv')

spark.stop()