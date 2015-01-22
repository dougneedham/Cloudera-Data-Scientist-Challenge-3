#!/usr/bin/python
from __future__ import division, print_function, absolute_import
import sys
sys.path.append('.')
from functools import reduce
import numpy as np
from scipy import special
import port_lib as pl
#1:3,4 2:3,4

#obs = np.array([[45151,44199],[46812,42258]])
obs = np.array([[45151,636240],[44199,635592]])
g, p, dof, expctd = pl.chi2_contingency(obs, lambda_="log-likelihood")
print(g)
print(p)
print(dof)
print(expctd)
print
print 
obs = np.array([[46812,632901],[42258,639211]])
g, p, dof, expctd = pl.chi2_contingency(obs, lambda_="log-likelihood")
print(g)
print(p)
print(dof)
print(expctd)
