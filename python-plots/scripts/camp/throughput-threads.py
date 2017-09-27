import os
import sys
import matplotlib.pyplot as plt

# get the path 
path = sys.argv[0]
path = "C:\\Users\\yaz\\FinalResults-1444222129106-p-4cl-9.txt.txt"

data = {}
with open(path, 'r') as f:
	for line in f:
		if 'InitialSOAR' in line:
			thread = int(line.split(',')[6])
			#print(thread)
			throughput = float(line.split(',')[7])
			#print(throughput)
			data[thread] = throughput

xAxis = sorted(data)
yAxis = []
for k in xAxis:
	yAxis.append(data[k])
	
figMR = plt.figure()
axMR = figMR.add_subplot(111)
axMR.set_title("Throughput-Threads")
axMR.set_xlabel('Threads')
axMR.set_ylabel('Throughput')

axMR.plot(xAxis, yAxis)
leg = axMR.legend()
plt.show()