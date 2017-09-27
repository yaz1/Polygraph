import numpy as np
import matplotlib.pyplot as plt

filenames = []

filenames.append('traceZipf0.27-hint.csv')
filenames.append('traceZipf0.27-hint-expcost.csv')
filenames.append('traceZipf0.99-hint.csv')
filenames.append('traceZipf0.99-hint-expcost.csv')

filenames.append('traceZipf0.27-7200_trc1.log.csv')
filenames.append('traceZipf0.27-7200_trc2.log.csv')
filenames.append('traceZipf0.27-7200_trc3.log.csv')
filenames.append('traceZipf0.27-7200_trc4.log.csv')
filenames.append('traceZipf0.27-7200_trc5.log.csv')
filenames.append('traceZipf0.27-7200_trc6.log.csv')
filenames.append('traceZipf0.27-7200_trc7.log.csv')
filenames.append('traceZipf0.27-7200_trc8.log.csv')
filenames.append('traceZipf0.27-7200_trc9.log.csv')
filenames.append('traceZipf0.27-7200_trc10.log.csv')

for file in filenames:
	with open('traces/'+file) as f:
		data = f.read()

	data = data.split('\n')
	
	# get the name
	name = data[0].split(',')[0]

	# get offlines value
	offlines = data[len(data)-2]

	# remove some unsed data
	data = data[3:len(data)-2]

	# get probabilities data
	probs = [row.split(',')[1] for row in data]
	print(probs)

	# process drawing plots
	cols = [2, 4, 6, 8, 10]
	for col in cols:
		offline_mr = offlines.split(',')[col]
		offline_cm = offlines.split(',')[col+1]

		mrdata = [row.split(',')[col] for row in data]
		missrate = []
		for mrdat in mrdata:
			x = float(offline_mr)
			if x == 0:
				missrate.append(0)
			else:
				missrate.append((float(mrdat) / float(offline_mr) - 1) * 100)
		
		cmdata = [row.split(',')[col+1] for row in data]
		costmiss = []
		for cmdat in cmdata:
			x = float(offline_cm)
			if x == 0:
				costmiss.append(0)
			else:
				costmiss.append((float(cmdat) / float(offline_cm) - 1) * 100)

		figMR = plt.figure()
		axMR = figMR.add_subplot(111)
		axMR.set_title(file+'-CacheSize '+str(col/10.0))
		axMR.set_xlabel('Admission Control Probability')
		axMR.set_ylabel('MissRate (CAMP/Offline - 1)*100')
		axMR.plot(probs, missrate, c='b', label='online')
		#axMR.plot(probs, offline_missrate, c='b', label='offline')

		leg = axMR.legend()
		plt.savefig('figs/'+file+'-percent-cachesize'+str(col/10.0)+'-missrate.png')

		figMR = plt.figure()
		axMR = figMR.add_subplot(111)
		axMR.set_title(file+'-CacheSize '+str(col/10.0))
		axMR.set_xlabel('Admission Control Probability')
		axMR.set_ylabel('CostMiss (CAMP/Offline - 1)*100')
		axMR.plot(probs, costmiss, c='b', label='online')
		#axMR.plot(probs, offline_costmiss, c='b', label='offline')

		leg = axMR.legend()
		plt.savefig('figs/'+file+'-percent-cachesize'+str(col/10.0)+'-costmiss.png')



