#!/usr/bin/python

import json
import sys
sys.path.append('.')
import uid_dict as ud

for line in sys.stdin:
	data = json.loads(line)
	if data['uid'] not in ud.uid_dict:	
		if data['action'] == 'signup':
			print '%s\t1' % (data['experiments'])
