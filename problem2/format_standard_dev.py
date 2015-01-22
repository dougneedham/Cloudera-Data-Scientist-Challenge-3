#!/usr/bin/python
from __future__ import division
import sys
import json
import math
file_name = 'data/meandata.json'
json_data = open(file_name)
data = json.load(json_data)

visitor_file = 'data/nonbot_visits.number'
vf = open(visitor_file)
for line in vf:
        visitors = int(line.strip())
vf.close()

total_data = {}
output_data = {}
stddev_dict = {}

for campaign in data.keys():
	output_data['standard_deviations'] = []
	for query in data[campaign].keys():
		if query in total_data.keys():
			total_data[query] = total_data[query]+data[campaign][query]
		else:
			total_data[query] = data[campaign][query]
for query in total_data.keys():
	query_sum = 0.0
	query_stddev = 0.0
	campagin_index = 0
	for campaign in data.keys():
		if query in data[campaign]:
			query_sum = query_sum+math.pow(data[campaign][query]-(total_data[query]/2),2)
	query_stddev = math.sqrt(query_sum/2)
	stddev_dict[query] = query_stddev
	for campaign in data.keys():
		tmp_dict = {}
		if query in data[campaign]:
			tmp_dict['campaign'] = campaign
			tmp_dict['query_string'] = query
			tmp_dict['standard_deviation'] = stddev_dict[query]
			query_sum = query_sum+math.pow(data[campaign][query]-(total_data[query]/2),2)
		if len(tmp_dict) > 0:
			output_data['standard_deviations'].append(tmp_dict)
		
	#tmp_dict['standard_deviation'] = query_stddev
	#output_data['standard_deviations'].append(tmp_dict)


max = 0
output_data["highest_mean_value"] = {}

for key1 in data.keys():
	for key2 in data[key1].keys():
		if max < int(data[key1][key2])/visitors:
			output_data["highest_mean_value"]["campaign"] = key1 
			output_data["highest_mean_value"]["query_string"] = key2 
			max = int(data[key1][key2])/visitors

print(json.dumps(output_data,indent=True,sort_keys=True))


