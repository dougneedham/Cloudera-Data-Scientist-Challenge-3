#!/usr/bin/python

import json
import sys
# Spit out a visit_id if it is in the uid dictionary. 
# This tells us how many visitors are bots
sys.path.append('.')
import uid_dict as ud

for line in sys.stdin:
	data = json.loads(line)
	if data['uid'] not in ud.uid_dict:	
		print '%s' % (data['visit_id'])
