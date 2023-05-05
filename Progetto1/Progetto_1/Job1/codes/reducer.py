#!/usr/bin/env python3
"""reducer.py"""
import sys

productyear_2_count = {}
years = []

for line in sys.stdin:

    line = line.strip()

    current_year, current_product = line.split("\t")

    years.append(current_year)

    if (current_product, current_year) not in productyear_2_count:
        productyear_2_count[(current_product, current_year)] = 0

    productyear_2_count[(current_product, current_year)] += 1

for year in set(key[1] for key in productyear_2_count.keys()):
    product_2_year = {k[0]: v for k, v in productyear_2_count.items() if k[1]==year}
    sorted_product_by_year = {k: v for k,v in sorted(product_2_year.items(), key=lambda item: item[1], reverse=True)}
    for k, _ in list(sorted_product_by_year.items())[:10]:
        print("%s\t%s" % (year, k))