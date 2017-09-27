"""
Simple demo of a scatter plot.
"""
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from matplotlib import rc
#import pylab

rc('text', usetex=True)

filename = "/home/hieun/Desktop/plot1.csv"

data = np.genfromtxt(filename, delimiter=',',skip_footer=4, names=['x','y'])
data1 = np.genfromtxt(filename, delimiter=',', skip_header=20000,names=['x'])
print data1['x'][0]
plt.plot([data1['x'][0], data1['x'][0]], [0, 7], color='r', linestyle='--', linewidth=1)
plt.plot([data1['x'][1], data1['x'][1]], [0, 7], color='b', linestyle='--', linewidth=1)
plt.plot([data1['x'][2], data1['x'][2]], [0, 7], color='c', linestyle='--', linewidth=1)
plt.plot([data1['x'][3], data1['x'][3]], [0, 7], color='k', linestyle='--', linewidth=1)


x = []
for i in range(1, len(data)+1):
  x.append(i)

#N = 50
##x = data[:,0]
#y = data[:,1]
#colors = np.random.rand(N)
#area = np.pi * (15 * np.random.rand(N))**2 # 0 to 15 point radiuses

plt.scatter(x, data['y'])
plt.xlim([-100,20000])

plt.ylim([-0.1,7])

plt.xlabel('Key')
plt.ylabel('Weight')
plt.title('traceZipf0.27-7200_trc8.log')

fig = plt.figure()
#ax = fig.add_subplot(2,1,1)
#ax.set_yscale('log')

ax = fig.add_subplot(111)
ax.text(5000, 3, r'$this is just a latex line$', fontsize=15)

plt.show()
