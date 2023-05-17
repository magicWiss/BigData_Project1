#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
import datetime

def time_to_year(x):
    try:
        return datetime.datetime.utcfromtimestamp(int(x)).strftime('%Y')
    except Exception as e:
        return 0
    

PRODUCT_ID = 1
TIME = 7
TEXT = 9

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job1") \
    .config("spark.executor.memory", "500m") \
    .getOrCreate()

# read the input file into an RDD with a record for each line
reviewsRDD = spark.sparkContext.textFile(input_filepath).map(lambda x: x.split(","))

# Get the top 10 products x year list
yearproduct_countRDD = reviewsRDD.map(f=lambda x : ((time_to_year(x[TIME]), x[PRODUCT_ID]), 1)).reduceByKey(lambda a, b: a + b)
yearcount_productRDD = yearproduct_countRDD.map(lambda x: ((int(x[0][0]), int(x[1])), x[0][1]))
sortedRDD = yearcount_productRDD.sortByKey().map(lambda x: (x[0][0], (x[1], x[0][1])))
top10year_productLIST = sortedRDD.groupByKey().mapValues(lambda products: sorted(products, key=lambda x: x[1], reverse=True)[:10]).flatMap(lambda x: [(x[0], s[0]) for s in x[1]]).collect()

# filter the reviews by the results' list
reviews_with_yearRDD = reviewsRDD.map(f=lambda x : ((time_to_year(x[TIME]), x[PRODUCT_ID]), x[TEXT]))
filteredRDD = reviews_with_yearRDD.filter(lambda x: (int(x[0][0]),x[0][1]) in top10year_productLIST and int(x[0][0])>0)

# split text into individual words
wordRDD = filteredRDD.flatMap(lambda x: ((x[0][0], x[0][1], word) for word in x[1].split(' ') if len(word)>=4))

# count occurrences of each word for each (year, product_id)
countRDD = wordRDD.map(lambda x: ((x[0], x[1], x[2]), 1)).reduceByKey(lambda a, b: a + b)

mappedRDD = countRDD.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))
groupedRDD = mappedRDD.groupByKey()
sortedRDD = groupedRDD.mapValues(lambda values: sorted(values, key=lambda x: x[1], reverse=True))
top5words_per_yearproductRDD = sortedRDD.mapValues(lambda values: values[:5]).flatMap(lambda x: [(*x[0], word, count) for word, count in x[1]])

top5words_per_yearproductRDD.saveAsTextFile(output_filepath)

spark.stop()