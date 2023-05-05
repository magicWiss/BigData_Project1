#!/usr/bin/env python3
"""mapper.py"""
import sys

PRODUCT_ID = 1
for line in sys.stdin:
    line=line.strip()
    words=line.split(",")
    
    print("%s\t%i" %(words[PRODUCT_ID],1))