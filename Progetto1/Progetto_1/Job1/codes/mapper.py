#!/usr/bin/env python3
"""mapper.py"""
import sys
import datetime

PRODUCT_ID = 1
TIME = 7
for line in sys.stdin:
    line=line.strip()
    fields=line.split(",")
    
    try:
        unix_time = float(fields[TIME])
        year = datetime.datetime.utcfromtimestamp(unix_time).strftime('%Y')
    except:
        continue
    print("%s\t%s" %(year, fields[PRODUCT_ID]))