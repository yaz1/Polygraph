import numpy as np
import matplotlib.pyplot as plt

file = "prob.8-cache.75.csv"
file = "max-prob.8-cache.75.csv"
file = "prob.995-cache.75.csv"
file = "max-prob.995-cache.75.csv"
#file = "prob1-cache.75.csv"
#file = "max-prob1-cache.75.csv"
#with open('/home/hieun/Desktop/plot_data/new_data/'+file) as f:
with open('/home/hieun/Desktop/log.csv') as f:
	data = f.read()

data = data.split('\n')
data = data[0:len(data)-1]
#print data
numRequests = [row.split(',')[0] for row in data]
fractions = [row.split(',')[1] for row in data]

figMR = plt.figure()
axMR = figMR.add_subplot(111)
#axMR.set_title(file)
axMR.set_xlabel('Num of Requests after Trace 1 executes')
axMR.set_ylabel('Fraction of cache occupied by Trace 1')
axMR.plot(numRequests, fractions, c='b')
#axMR.plot(probs, offline_missrate, c='b', label='offline')
leg = axMR.legend()
#plt.savefig('/home/hieun/Desktop/plot_figures/'+file+'.png')

axMR.set_ylim([0,1])
axMR.set_xlim([0,8000000])
plt.show()

