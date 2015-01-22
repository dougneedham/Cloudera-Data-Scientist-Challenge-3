#!/usr/bin/python

import dateutil.parser
import json
import sys
sys.path.append('.')
import uid_dict as ud
from datetime import tzinfo,timedelta,datetime

for line in sys.stdin:
	data = json.loads(line)
	if data['uid'] not in ud.uid_dict:	
		print '%s' % (data['uid'])
