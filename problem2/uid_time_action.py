#!/usr/bin/python

import json
import sys
sys.path.append('.')

for line in sys.stdin:
	data = json.loads(line)
	print '%s,%s,%s' % (data['uid'],data['tstamp'],data['action'])
