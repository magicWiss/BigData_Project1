#!/usr/bin/env python3
"""mapper2.py"""
import sys
from pathlib import Path

PRODUCT_ID = 0
TEXT = 9

id_list = set()

# Get the path to the output file from the previous job
output_path = Path(sys.argv[1])

# Read in the output file and store the relevant data in id_list
with output_path.open("r") as f:
    for line in f:
        # Parse the output file line by line
        # Here, we assume that the output file has one ID per line
        line = line.strip()
        words = line.split("\t")
        id = words[PRODUCT_ID] 
        id_list.add(id)

for id in id_list:
    print("%s", id)
"""
for line in sys.stdin:
    line=line.strip()
    fields=line.split(",")

    text = fields[TEXT]

    for word in text.split(" "):
        word = word.strip()
        if len(word)>=4:
            print("%s\t%i" %(word,1))"""