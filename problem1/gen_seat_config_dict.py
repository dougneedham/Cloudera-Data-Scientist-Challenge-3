#!/usr/bin/python
import sys
index = 1
for data in sys.stdin:
        if index == 1:
                print 'seat_dict = dict(['
                print "('%s',%i)" % (data.strip(),index)
        else:
                print ",('%s',%i)" % (data.strip(),index)
        index = index + 1
print "])"

