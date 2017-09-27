import matplotlib
matplotlib.use('Agg')
import os
import sys
import matplotlib.pyplot as plt

# get the path 
#path = "C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1445887180947-nummembers100000-numclients1-bmoderetain-threadcount32-logfalse-workload9SymmetricWorkload-clientKosarScalabilityClient.RelaxedConsistencyClientFiler-1cl-9.txt\\"
path = str(sys.argv[1]) #'C:\\Users\\yaz\\Documents\\BG\\BGServerListenerLogs\\Exp1426007481104-nummembers100000-numclients3-bmoderetain-threadcount100-logfalse\\'
files = [i for i in os.listdir(path) if 'ratinglistenerLog' in i]
print(files)
#u"\\\\?\\" + os.getcwd() + u"\\" + dirname + u"\\" + f
clients = {}	
i = 0
for file in files:
	i += 1
	data = {}
	with open(path+"/"+file, 'r') as f:
		#print("File: "+ file)
		for line in f:
			if 'current acts/sec;' in line:
				time = int(line.split()[0])
				#print(time)
				try:
					throughput = float(line.split()[4])
				except:
					print(line.split()[4])
					throughput = 0
				#print(throughput)
				data[time] = throughput
	clients[i] = data
	
figMR = plt.figure()
axMR = figMR.add_subplot(111)
axMR.set_title("Throughput")
axMR.set_xlabel('Time')
axMR.set_ylabel('Throughput')


for cli in clients:	
	xAxis = []
	yAxis = []
	for t in clients[cli]:
		xAxis.append(t)
	xAxis.sort()
	
	for x in xAxis:
		yAxis.append(clients[cli][x])
	axMR.plot(xAxis, yAxis)

leg = axMR.legend()
#plt.show()
plt.savefig(path+"/thru"+'.png')
