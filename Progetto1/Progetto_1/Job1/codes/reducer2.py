#!/usr/bin/env python3
"""reducer2.py"""
import sys

word_2_count = {}
i=0
for line in sys.stdin:

    line = line.strip()
    
    current_word, one = line.split("\t")

    try:
        one = int(one)
    except ValueError:
        # count was not a number, so
        # silently ignore/discard this line
        continue

    # initialize words that were not seen before with 0
    if current_word not in word_2_count:
        word_2_count[current_word] = 0

    word_2_count[current_word] += 1

sorted_word_2_count = dict(sorted(word_2_count.items(),key=lambda x: x[1],reverse=True)[:10])

for word in sorted_word_2_count:
    print("%s\t%i" % (word, word_2_count[word]))