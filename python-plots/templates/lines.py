import numpy as np
import matplotlib.pyplot as plt

#file = "prob.8-cache.75.csv"
file = "max-prob.8-cache.75.csv"
#file = "prob.8-cache.75.csv"
#file = "prob.8-cache.75.csv"
#file = "prob.8-cache.75.csv"
file = "prob1-cache.75.csv"
with open('/home/hieun/Desktop/plot_data/' + file) as f:
	data = f.read()

data = data.split('\n')
data = data[0:len(data)-1]
#print data
numRequests = [row.split(',')[0] for row in data]
maxKeys = int(data[0].split(',')[1])+1
remainKeys = [row.split(',')[1] for row in data]

#print numRequests

y = []
for i in range(0, len(remainKeys)):
	p = float(remainKeys[i]) / float(maxKeys)
	y.append(p)

#print y

figMR = plt.figure()
axMR = figMR.add_subplot(111)
axMR.set_title(file)
axMR.set_xlabel('Num of Requests')
axMR.set_ylabel('Percent Remaining Keys Trace 1')
axMR.plot(numRequests, y, c='b')
#axMR.plot(probs, offline_missrate, c='b', label='offline')
leg = axMR.legend()
#plt.savefig(file+'-cachesize'+str(col/10.0)+'-missrate.png')

plt.show()

