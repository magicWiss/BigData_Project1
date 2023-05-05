#!/usr/bin/env python3
"""reducer.py"""
import sys

product_2_count = {}

for line in sys.stdin:

    line = line.strip()

    current_product, one = line.split("\t")

    try:
        one = int(one)
    except ValueError:
        # count was not a number, so
        # silently ignore/discard this line
        continue

    # initialize words that were not seen before with 0
    if current_product not in product_2_count:
        product_2_count[current_product] = 0

    product_2_count[current_product] += 1

sorted_product_2_count = dict(sorted(product_2_count.items(),key=lambda x: x[1],reverse=True)[:10])

for product in sorted_product_2_count:
    print("%s\t%i" % (product, product_2_count[product]))