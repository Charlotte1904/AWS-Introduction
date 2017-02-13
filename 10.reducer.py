#!/usr/bin/env python

# run in EMR 

import sys
from collections import Counter

hashtags = []
for line in sys.stdin:
	hashtags.append(line.split(' ')[0])

hashtag_count = Counter(hashtags)

print hashtag_count.most_common(10)
