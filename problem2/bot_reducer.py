#!/usr/bin/python
# cat ../famous/web.log | ./uid_time_action.py  | sort |uniq | ./bot_reducer.py  | ./gen_uid_dict.py > uid_dict.py
import dateutil.parser
import datetime
import json
import sys
sys.path.append('.')
from datetime import tzinfo,timedelta,datetime
def main():
	currentuid = None
	lastTime = None
	action1 = None
	action2 = None
	bot = None

	data_dict = {}

	for line in sys.stdin:
		(uid,timestamp,action) = line.split(',')
		
		if uid != currentuid:
			if bot:
				print ("%s") % (currentuid)
			
			currentuid = uid
			action_list = []
			time_list = []
			bot = False
			#print action.strip()
			action_list.append(action.strip())
			dt = datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
			time_list.append(dt)
		else:
			action_list.append(action.strip())
			dt = datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S')
			time_list.append(dt)
		

		if(len(action_list) >= 2 and action_list[0] == 'landed' and action_list[1] == 'adclick'):
			#print '%s,%s' %(action_list[0],action_list[1])
			diff = time_list[1] - time_list[0]
			if(diff.seconds > 11):
				bot = False
			else: 
				bot = True
		else:
			bot = False

	if uid == currentuid:
		if bot:
			print ("%s") % (currentuid)

if __name__ == '__main__':
	main()
