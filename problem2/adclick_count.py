#!/usr/bin/python

import json
import sys
sys.path.append('.')
import uid_dict as ud
# ignore the uids if they are in the bot list
# Then check to see if at any time there was an adclick
# The reducer for this is simply a line counter, so we spit out a number from the 
# overall map-reduce job
#
for line in sys.stdin:
	data = json.loads(line)
	if data['uid'] not in ud.uid_dict:	
		if data['action'].strip() == 'adclick':
			print '%s' % (data['action'])
