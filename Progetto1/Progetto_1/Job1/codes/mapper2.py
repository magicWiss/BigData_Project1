#!/usr/bin/env python3
"""mapper2.py"""
import sys
import datetime

PRODUCT_ID = 1
TIME = 7
TEXT = 9

id_list = set()


# Read in the output file and store the relevant data in id_list
with open("output.txt", "r") as f:
    for line in f:
        line = line.strip()
        fields = line.split("\t")
        id = fields[1]
        id_list.add(id)

for line in sys.stdin:
    line=line.strip()
    fields=line.split(",")

    id = fields[PRODUCT_ID]

    if id in id_list:
        text = fields[TEXT]

        try:
            unix_time = float(fields[TIME])
            year = datetime.datetime.utcfromtimestamp(unix_time).strftime('%Y')
        except:
            continue

        for word in text.split(" "):
            word = word.strip()
            if len(word)>=4:
                print("%s\t%s\t%s" %(year, id, word))