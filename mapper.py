#!/usr/bin/env python

# run in EMR 

import sys
import json


for line in sys.stdin:
	try:
		if json.loads(line)['entities']['hashtags'] != []:
			print '%s %s' % (json.loads(line)['entities']['hashtags'][0]['text'].lower(), 1)
	except:
		continue


# tweet format: 
# tweet = {'entities': {'hashtags': [{'text': 'NewsPicks', 'indices': [126, 136]}]}}
# print(tweet['entities']['hashtags'][0]['text'])