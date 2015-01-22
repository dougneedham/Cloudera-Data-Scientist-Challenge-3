#!/usr/bin/python
from __future__ import division
import numpy as np
import sys
import json
import math
sys.path.append('.')
import port_lib as pl

total_file = sys.argv[1]
signup_file = sys.argv[2]
sf = open(signup_file)
tf = open(total_file)

visitor_file = 'data/uid_count.number'
vf = open(visitor_file)
for line in vf:
        visitors = int(line.strip())
vf.close()
experiments = {}
total = {}
result = {}
result["overall_signup_rate"] = {}
for data in sf:
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
sf.close()
for total_data in tf:
	(index,value) = total_data.strip().split('\t')
	index = index[1:-1]
	(row,col) = index.split(',')
	if int(row) not in total:
		total[int(row)]= {}
		total[int(row)][int(col)] = int(value)
	else:
		total[int(row)][int(col)] = int(value)
	if int(col) not in total:
		total[int(col)]= {}
		total[int(col)][int(row)] = int(value)
	else:
		total[int(col)][int(row)] = int(value)
tf.close()
test1 = []
test2 = []
for keys in experiments.keys():
	if keys in (1,2):	
		if keys == 1:
			result["overall_signup_rate"]["experiment1"] = float(sum(experiments[keys].values())/visitors)
		else:
			result["overall_signup_rate"]["experiment2"] = float(sum(experiments[keys].values())/visitors)
		test1.append(sum(experiments[keys].values()))
	else:
		if keys == 3:
			result["overall_signup_rate"]["experiment3"] = float(sum(experiments[keys].values())/visitors)
		else:
			result["overall_signup_rate"]["experiment4"] = float(sum(experiments[keys].values())/visitors)
		test2.append(sum(experiments[keys].values()))
		
	#print '%s,%s' % (keys,sum(experiments[keys].values()))
total1 = []
total2 = []
for keys in total.keys():
	if keys in (1,2):	
		total1.append(sum(total[keys].values()))
	else:
		total2.append(sum(total[keys].values()))

exp1_val1 = test1[0]
exp1_val2 = total1[0]-test1[0]
exp2_val1 = test1[1]
exp2_val2 = total1[1]-test1[1]
exp3_val1 = test2[0]
exp3_val2 = total2[0]-test2[0]
exp4_val1 = test2[1]
exp4_val2 = total2[1]-test2[1]
exp1_2_total = exp1_val1 + exp2_val1
observations1 = np.array([[exp1_val1,exp1_val2],[exp2_val1,exp2_val2]])
observations2 = np.array([[exp3_val1,exp3_val2],[exp4_val1,exp4_val2]])

test1_result,p1,dof1,expected1 = pl.chi2_contingency(observations1,lambda_="log-likelihood")
test2_result,p2,dof2,expected2 = pl.chi2_contingency(observations2,lambda_="log-likelihood")

#result["performance_of_experiment_1_versus_2"] = test1_result[1]
#result["performance_of_experiment_3_versus_4"] = test2_result[1]
result["performance_of_experiment_1_versus_2"] = p1
result["performance_of_experiment_3_versus_4"] = p2

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

report_size = get_days(exp1_2_total)

result["number_of_full_days_for_confidence"] = report_size


print json.dumps(result)

