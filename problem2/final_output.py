#!/usr/bin/python
from __future__ import division
import sys
import json
import uid_dict as ud

# Get ther number of bots
bot_uid_file = 'data/bot_uids.number'
buf = open(bot_uid_file)
for line in buf:
        number_of_bots = int(line.strip())
buf.close()

# Get ther number of visitors
non_bot_visit_file = 'data/nonbot_visits.number'
nbuf = open(non_bot_visit_file)
for line in nbuf:
        number_of_non_bots = int(line.strip())
nbuf.close()

adclick_file = 'data/adclick_count.number'
af = open(adclick_file)
for line in af:
        adclicks = int(line.strip())
af.close()
q3_file = 'json/question3.json'
q4_file = 'json/question4.json'
q5_file = 'json/question5.json'

q3_data = open(q3_file)
q4_data = open(q4_file)
q5_data = open(q5_file)

q3_json = json.load(q3_data)
q4_json = json.load(q4_data)
q5_json = json.load(q5_data)

output_data = {}

output_data[1] = {}
output_data[2] = {}
output_data[3] = {}
output_data[4] = {}
output_data[5] = {}
output_data[1]["number_of_distinct_visitors_that_are_bots"] = number_of_bots
# This could be a misspelling, it could not be a misspelling. 
# I am spitting out the line as defined in the requirement document. 
# I think what is meant here is "overall_ad_click_through_rate" 
# However, this is what is in the requirements document
output_data[2]["overall_as_click_through_rate"] = float(adclicks/number_of_non_bots)
output_data[3] = q3_json
output_data[4] = q4_json
output_data[5] = q5_json

print(json.dumps(output_data,indent=True,sort_keys=True))
