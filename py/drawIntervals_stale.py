#!/usr/bin/python
import numpy as np
import matplotlib.pyplot as plt
import sys
import math

path = str(sys.argv[1])

with open(path) as f:
	data = f.read()
data = data.splitlines()
rows = len(data)
#colnums = len(data[0].split(',')) - 1

figMR = plt.figure()
axMR = figMR.add_subplot(111)
#axMR.set_title(path)
axMR.set_xlabel('Time')
#ax1 = plt.axes(frameon=True)
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
	#axMR.plot(xAxis, yAxis, linewidth=10, label= tokens[0])
	axMR.plot(xAxis, yAxis, color=tokens[3], linewidth=10)
	i = i - 1
	axMR.text(int(tokens[1])+((int(tokens[2])-int(tokens[1]))/2), i+1+(rows*0.04)-((math.ceil(rows/2)-1)*0.02), tokens[0],#1+(rows*0.04)-((math.ceil(row/2)-1)*0.02)
        horizontalalignment='center',
        verticalalignment='center',
        fontsize=10, fontweight='bold', color='black'
        #,transform=axMR.transAxes
		)

plt.ylim([0,rows+1])
#xAxis = []
#yAxis = []
#xAxis.append(sys.argv[2])	
#xAxis.append(sys.argv[2])
#yAxis.append(0)
#yAxis.append(rows+1)
#axMR.plot(xAxis, yAxis, 'r--')

#xAxis = []
#yAxis = []
#xAxis.append(sys.argv[3])	
#xAxis.append(sys.argv[3])
#yAxis.append(0)
#yAxis.append(rows+1)
#axMR.plot(xAxis, yAxis, 'r--')




art = []
#plt.axes.get_yaxis().set_visible(False)
#plt.set(yticklabels=[])
#plt.axis('off')
#.axes.get_yaxis().set_ticks([])
plt.grid(True)
axMR.axes.get_yaxis().set_visible(False)
#lgd = axMR.legend(bbox_to_anchor=(1.01, 0.5), loc=6, borderaxespad=0.)
#lgd = axMR.legend(loc=9, bbox_to_anchor=(0.5, -0.1))
#art.append(lgd)
#plt.savefig(path+'.png',additional_artists=art, bbox_inches="tight")
plt.savefig(path+'.png', bbox_inches="tight")
print ('Created ' + path + '.png')