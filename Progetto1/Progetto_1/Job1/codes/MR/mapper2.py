#!/usr/bin/env python3
"""mapper2.py"""
import sys
import datetime

PRODUCT_ID = 1
TIME = 7
TEXT = 9

yearid_list = set()


# Read in the output file and store the relevant data in id_list
try:
    with open("output.txt", "r") as f:
        for line in f:
            line = line.strip()
            fields = line.split("\t")
            year = fields[0]
            id = fields[1]
            yearid_list.add((year, id))
except Exception as e:
    print(e)
for line in sys.stdin:
    line=line.strip()
    fields=line.split(",")

    id = fields[PRODUCT_ID]

    try:
        unix_time = float(fields[TIME])
        year = datetime.datetime.utcfromtimestamp(unix_time).strftime('%Y')
    except:
        continue

    if (year, id) in yearid_list:
        text = fields[TEXT]
        
        for word in text.split(" "):
            word = word.strip()
            if len(word)>=4:
                print("%s,%s\t%s" %(year, id, word))