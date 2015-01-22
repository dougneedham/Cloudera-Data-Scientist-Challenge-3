#!/usr/bin/python
from __future__ import division
import sys
import os
sys.path.append('.')
#fromvertex = os.environ["VERTEX"]
fromvertex = sys.argv[1]
for data in sys.stdin:
	(tovertex,score) = data.strip().split(',')
	print "%s,%s,%s" % (score,fromvertex,tovertex)
