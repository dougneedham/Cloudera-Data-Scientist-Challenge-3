#!/usr/bin/python
from __future__ import division
import sys
import json
import math

current_word = None
current_count = 0
word = None
output_data = {}
visitor_file = 'data/nonbot_visits.number'
vf = open(visitor_file)

for line in vf:
	visitors = int(line.strip())
vf.close()



# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split(',', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    (campaign,query) = word.split("\t")

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
	    if current_count > 1:
    		(campaign,query) = current_word.split("\t")
		output_data[campaign] = {}
		output_data[campaign][query] = current_count
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    (campaign,query) = current_word.split("\t")
    output_data[campaign] = {}
    output_data[campaign][query] = current_count

print(json.dumps(output_data,indent=True,sort_keys=True))
