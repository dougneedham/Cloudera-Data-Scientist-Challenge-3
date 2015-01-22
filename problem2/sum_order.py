#!/usr/bin/python
from __future__ import division
import sys
import json
import math
experiments = {}
visitor_file = 'data/nonbot_visits.number'
vf = open(visitor_file)
for line in vf:
        visitors = int(line.strip())
vf.close()

result = {}
result["mean_revenue_per_visit"] = {}
for data in sys.stdin:
	(index,value) = data.strip().split('\t')
	index = index[1:-1]
	(row,col) = index.split(',')
	if int(row) not in experiments:
		experiments[int(row)]= {}
		experiments[int(row)][int(col)] = int(value)
	else:
		experiments[int(row)][int(col)] = int(value)
	if int(col) not in experiments:
		experiments[int(col)]= {}
		experiments[int(col)][int(row)] = int(value)
	else:
		experiments[int(col)][int(row)] = int(value)
test1 = []
test2 = []
for keys in experiments.keys():
	if keys in (1,2):	
		if keys == 1:
			result["mean_revenue_per_visit"]["experiment1"] = sum(experiments[keys].values())/visitors
		else:
			result["mean_revenue_per_visit"]["experiment2"] = sum(experiments[keys].values())/visitors
		test1.append(sum(experiments[keys].values()))
	else:
		if keys == 3: 
			result["mean_revenue_per_visit"]["experiment3"] = sum(experiments[keys].values())/visitors
		else:
			result["mean_revenue_per_visit"]["experiment4"] = sum(experiments[keys].values())/visitors
		test2.append(sum(experiments[keys].values()))
		
	#print '%s,%s' % (keys,sum(experiments[keys].values()))

total_population = test2[0] + test2[1]

def get_days(population_size):
  Z = 2.57
  p = 0.5
  e = .01
  N = population_size
  n_0 = 0.0
  n = 0.0

  n_0 = ((Z**2) * p * (1-p)) / (e**2)

  # ADJUST SAMPLE SIZE FOR FINITE POPULATION
  n = n_0 / (1 + ((n_0 - 1) / float(N)) )
  sample_size = int(math.ceil(n))
  days_for_experiment = 15
  total_days = int(math.ceil((sample_size)/(population_size/days_for_experiment)))
  return total_days

report_size = get_days(total_population)

result["number_of_full_days_for_confidence"] = report_size


print json.dumps(result)

