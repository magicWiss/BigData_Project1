#!/usr/bin/env python3
"""spark application"""
import argparse
from pyspark.sql import SparkSession


USER_ID=2
NUM=4
DENOM=5
#metodo per la creazione della entry dell'RDD a partire dal record
def create_entry(input):
    user_id=input[USER_ID]
    numerator=input[NUM]
    denom=input[DENOM]
    try:
        numerator=float(numerator)
        denom=float(denom)
    except ValueError:
        pass

    ratio=0
    if denom!=0:
        ratio=numerator/denom
    
    return (user_id,(ratio,1))

def custom_reducer(vals1,vals2):

    return (vals1[0] + vals2[0],  vals1[1] +vals2[1])


def compute_ratio(vals):
    return vals[0]/vals[1]

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CSV as Text Processing").getOrCreate()

# restituisce un RDD dopo avere caricato il file dato in input
input_RDD = spark.sparkContext.textFile(input_filepath)

# per rimuovere l'header
header = input_RDD.first()

input2_RDD = input_RDD.filter(lambda row: row != header)

# splitta i record in campi sulla base della virgola 
rows = input2_RDD.map(lambda line: line.split(","))


#creazione dei record (userId, (sing_ratio,1))
user2values=rows.map(create_entry)

#aggregazione su userId e calcolo della somma dei rapporti e del numero di medesimi userId
user_2_data = user2values.reduceByKey(custom_reducer)

#calcolo del rapporto finale
user_2_ratio=user_2_data.mapValues(compute_ratio)

sorted_user_2_ratio = user_2_ratio.sortBy(
    lambda user_with_ratio : user_with_ratio[1],ascending=False)

# Transform the word_2_count RDD into an RDD of strings
output_strings_RDD = sorted_user_2_ratio.map(
    lambda user_values: "%s: %f" %(user_values[0],user_values[1]))



output_strings_RDD.saveAsTextFile(output_filepath)

