#!/usr/bin/env python3
"""reducer2.py"""
import sys

yearproductidword_2_count = {}

for line in sys.stdin:

    line = line.strip()
    
    year_product_id, word = line.split("\t")

    year, product_id = year_product_id.split(",")

    if (year, product_id, word) not in yearproductidword_2_count:
        yearproductidword_2_count[(year, product_id, word)] = 0

    yearproductidword_2_count[(year, product_id, word)] += 1

for (year, product_id) in set((key[0], key[1]) for key in yearproductidword_2_count.keys()):
    word_2_yearproductid = {k[2]: v for k, v in yearproductidword_2_count.items() if k[0]==year and k[1]==product_id}
    sorted_word_by_year = {k: v for k,v in sorted(word_2_yearproductid.items(), key=lambda item: item[1], reverse=True)}
    for k, v in list(sorted_word_by_year.items())[:5]:
        print("%s, %s, %s\t%i" % (year, product_id, k, v))