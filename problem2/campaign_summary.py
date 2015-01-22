#!/usr/bin/python
import json
import sys
sys.path.append('.')

def main():
	currentvisit_id = None
	lastTime = None
	action1 = None
	action2 = None

	data_dict = {}

	for line in sys.stdin:
		(visit_id,timstamp,campaign,action,query) = line.strip().split(',')
		
		if visit_id != currentvisit_id:
			currentvisit_id = visit_id
			query_list = []
			campaign_list = []
			action_list = []
			action_list.append(action.strip())
			if query != 'NULL':
				query_list.append(query)
			if campaign != 'NULL':
				campaign_list.append(campaign)
		else:
			action_list.append(action.strip())
			if query_list != 'NULL':
				query_list.append(query)
			if campaign != 'NULL':
				campaign_list.append(campaign)
		if(len(action_list) >= 2 and 'landed' in action_list and 'order' in action_list):
			if(action_list.index('landed') < action_list.index('order')):
				for queries in set(query_list):
					if queries.strip() != 'NULL':
						print "%s\t%s,1" % (campaign_list[0],queries.strip())

	if visit_id == currentvisit_id:
		if query.strip() != 'NULL':
			print "%s\t%s,1" % (campaign,query.strip())


if __name__ == '__main__':
	main()
