#!/usr/bin/python
import numpy as np
import matplotlib.pyplot as plt
import sys

path = str(sys.argv[1])

with open(path) as f:
	data = f.read()
data = data.splitlines()
rows = len(data)
#colnums = len(data[0].split(',')) - 1

figMR = plt.figure()
axMR = figMR.add_subplot(111)
axMR.set_title(path)
axMR.set_xlabel('Time')
#axMR.set_ylabel('%Utilization')

#Matrix = [[0 for x in range(colnums)] for x in range(rows)]

i = rows
for line in data[0:]:
	xAxis = []
	yAxis = []
	tokens = line.split(',')	
	xAxis.append(tokens[1])	
	xAxis.append(tokens[2])
	yAxis.append(i)
	yAxis.append(i)
	axMR.plot(xAxis, yAxis, label= tokens[0])
	i = i - 1


xAxis = []
yAxis = []
xAxis.append(sys.argv[2])	
xAxis.append(sys.argv[2])
yAxis.append(0)
yAxis.append(rows+1)
axMR.plot(xAxis, yAxis, 'r--')

xAxis = []
yAxis = []
xAxis.append(sys.argv[3])	
xAxis.append(sys.argv[3])
yAxis.append(0)
yAxis.append(rows+1)
axMR.plot(xAxis, yAxis, 'r--')

art = []
plt.grid(True)
lgd = axMR.legend(bbox_to_anchor=(1.01, 0.5), loc=6, borderaxespad=0.)
#lgd = axMR.legend(loc=9, bbox_to_anchor=(0.5, -0.1))
art.append(lgd)
plt.savefig(path+'.png',additional_artists=art, bbox_inches="tight")
print ('Created ' + path + '.png')