#!/usr/bin/python

import dateutil.parser
import json
import sys
sys.path.append('.')
import uid_dict as ud
from datetime import tzinfo,timedelta,datetime

for line in sys.stdin:
	data = json.loads(line)
	for field in data.keys():
		if data['uid'] in ud.uid_dict:
			print 1
		else:
			print 0

#		print data['uid']
