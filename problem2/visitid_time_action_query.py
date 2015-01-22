#!/usr/bin/python

import dateutil.parser
import json
import sys
sys.path.append('.')
from datetime import tzinfo,timedelta,datetime

for line in sys.stdin:
	data = json.loads(line)
	if 'query' in data and 'campaign' in data:
		print '%s,%s,%s,%s,%s' % (data['visit_id'],data['tstamp'],data['campaign'],data['action'],data['query'])
	else:
		if 'campaign' in data:
			print '%s,%s,%s,%s,NULL' % (data['visit_id'],data['tstamp'],data['campaign'],data['action'])
		else:
			print '%s,%s,NULL,%s,NULL' % (data['visit_id'],data['tstamp'],data['action'])
