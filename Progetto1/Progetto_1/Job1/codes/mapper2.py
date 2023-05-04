#!/usr/bin/env python3

import sys

for line in sys.stdin:
        

        line=line.strip()
        

        words=line.split("\t")
        if len(words)==2:
                print("%s\t%s" %(words[0],words[1]))