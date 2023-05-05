#!/usr/bin/env python3
"""mapper2.py"""
import sys

PRODUCT_ID = 1
TEXT = 9

id_list = set()


# Read in the output file and store the relevant data in id_list
with open("output.txt", "r") as f:
    for line in f:
        # Parse the output file line by line
        # Here, we assume that the output file has one ID per line
        line = line.strip()
        words = line.split("\t")
        id = words[0] 
        id_list.add(id)

for line in sys.stdin:
    line=line.strip()
    fields=line.split(",")

    id = fields[PRODUCT_ID]
    if id in id_list:
        text = fields[TEXT]

        for word in text.split(" "):
            word = word.strip()
            if len(word)>=4:
                print("%s\t%i" %(word,1))