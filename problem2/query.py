#!/usr/bin/python

import dateutil.parser
import json
import sys
sys.path.append('.')
from datetime import tzinfo,timedelta,datetime

for line in sys.stdin:
	data = json.loads(line)
	for field in data.keys():
		print '%s' % (data['query'])
