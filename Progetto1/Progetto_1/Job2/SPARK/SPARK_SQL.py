#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# parse arguments
args = parser.parse_args()
input_path = args.input_path
output_path=args.output_path

columns=["UserId","HelpfulnessNumerator","HelpfulnessDenominator"]
# Create a SparkSession
spark = SparkSession.builder.appName("UserAvgRatio").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv(input_path, header=True).cache()

df.createOrReplaceTempView('data')
# Apply the necessary transformations
result = spark.sql("SELECT UserId, COALESCE(SUM(HelpfulnessNumerator/HelpfulnessDenominator),0)/COUNT(*) AS avg_ratio FROM (SELECT UserId, HelpfulnessNumerator, CASE WHEN HelpfulnessDenominator=0 THEN 0 ELSE HelpfulnessDenominator END AS HelpfulnessDenominator FROM data) subquery GROUP BY UserId ORDER BY avg_ratio DESC;")

result.write.csv(output_path, header=True)
# Show the result
result.show()

# Stop the SparkSession
spark.stop()
