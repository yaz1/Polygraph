import os
import sys
import matplotlib.pyplot as plt
import re

# get the path 
#path = sys.argv[0]
paths = [
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441677377990-nummembers100000-numclients1-bmoderetain-threadcount72-logfalse-workloadViewProfileAction-clientrelational.JdbcDBClient_KOSARFilegreedy-6c-1cl-99.txt\\",
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441682871904-nummembers100000-numclients2-bmoderetain-threadcount68-logfalse-workloadViewProfileAction-clientrelational.JdbcDBClient_KOSARFilegreedy-6c-2cl-99.txt\\",	
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441688346252-nummembers100000-numclients4-bmoderetain-threadcount96-logfalse-workloadViewProfileAction-clientrelational.JdbcDBClient_KOSARFilegreedy-6c-4cl-99.txt\\",
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441644624823-nummembers100000-numclients1-bmoderetain-threadcount160-logfalse-workload99SymmetricWorkload-clientrelational.JdbcDBClient_KOSARFileingest-6c-1cl-99.txt\\",	
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441658244225-nummembers100000-numclients2-bmoderetain-threadcount69-logfalse-workload99SymmetricWorkload-clientrelational.JdbcDBClient_KOSARFileingest-6c-2cl-99.txt\\",	
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441663546178-nummembers100000-numclients4-bmoderetain-threadcount112-logfalse-workload99SymmetricWorkload-clientrelational.JdbcDBClient_KOSARFileingest-6c-4cl-99.txt\\",	
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441706900561-nummembers100000-numclients1-bmoderetain-threadcount97-logfalse-workload99SymmetricWorkload-clientrelational.JdbcDBClient_KOSARFilegreedy-6c-1cl-99.txt\\",	
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441716293728-nummembers100000-numclients2-bmoderetain-threadcount101-logfalse-workload99SymmetricWorkload-clientrelational.JdbcDBClient_KOSARFilegreedy-6c-2cl-99.txt\\",	
	"C:\\Users\\yaz\\Documents\\Experiments\\RatingExp1441723054420-nummembers100000-numclients4-bmoderetain-threadcount66-logfalse-workload99SymmetricWorkload-clientrelational.JdbcDBClient_KOSARFilegreedy-6c-4cl-99.txt\\",	
]

out = ""

# get all client stats file
for path in paths: 
	files = [i for i in os.listdir(path) if 'cache-stats' in i]
	print(files)

	totalCacheHit = 0
	totalRequests = 0
	totalEvictions = 0
	totalQueryResponseTime = 0
	totalRoundTripTime = 0
	totalUpdates = 0
	totalKeyInvalidated = 0
	totalInvalidatedAttempted = 0
	totalAskOtherClients = 0
	totalGotFromOtherClients = 0
	totalILeaseGranted = 0
	totalILeaseReleasedSuccess = 0	# result is STORED
	totalILeaseBackOff = 0
	totalQLeaseGranted = 0
	totalCopies = 0	# copies keys from this client to other clients
	totalSteal = 0
	avrReadTime = 0
	avrReadTimeFromOthers = 0
	avrReadTimeFromDBMS = 0
	avrReadTimeFromOthersAndDBMS = 0
	avrWriteTime = 0
	for file in files:
		data = {}
		with open(path+file, 'r') as f:
			for line in f:
				res = re.search(r'[-+]?\d*\.\d+|\d+', line)
				if res is None:
					val = 0
				else:
					val = float(re.search(r'[-+]?\d*\.\d+|\d+', line).group())
				if 'Num Cache hit' in line:
					totalCacheHit += val
				elif 'Num Query requests' in line:
					totalRequests += val
				elif 'Num Evictions' in line:
					totalEvictions += val
				elif 'Total Query Response Time' in line:
					totalQueryResponseTime += val		
				elif 'Total Roundtrip time' in line:
					totalRoundTripTime += val
				elif 'Num Updates' in line:
					totalUpdates += val
				elif 'Num Key Invalidated' in line:
					totalKeyInvalidated += val
				elif 'Total Invalidate attempted' in line:
					totalInvalidatedAttempted += val
				elif 'Num Ask other clients' in line:
					totalAskOtherClients += val
				elif 'Num Got from other clients' in line:
					totalGotFromOtherClients += val
				elif 'Num I lease granted' in line:
					totalILeaseGranted += val
				elif 'Num I lease released' in line:
					totalILeaseReleasedSuccess += val		
				elif 'Num I lease backoff' in line:
					totalILeaseBackOff += val		
				elif 'Num Q lease granted' in line:
					totalQLeaseGranted += val	
				elif 'Num USEONCE' in line:
					totalCopies += val
				elif 'Num STEAL' in line:
					totalSteal += val
				elif 'Average time per read' in line:
					avrReadTime += val
				elif 'Average time read from others:' in line:
					avrReadTimeFromOthers += val
				elif 'Average time read from dbms' in line:
					avrReadTimeFromDBMS += val
				elif 'Average time read from others then dbms' in line:
					avrReadTimeFromOthersAndDBMS += val
				elif 'Average time per update' in line:
					avrWriteTime += val

	out += path + "\r\n"
	out += "Clients: " + "\r\n"
	out += "Total Cache Hit (from local cache): " + str(totalCacheHit) + "\r\n"
	out += "Total Ask Other Clients: " + str(totalAskOtherClients) + "\r\n"
	out += "Total Got From Other Clients: " + str(totalGotFromOtherClients) + "\r\n"
	out += "Total Request: " + str(totalRequests) + "\r\n"
	out += "Percentage hit: " + str(float(totalCacheHit)/totalRequests) + "\r\n"
	if totalAskOtherClients != 0:
		out += "Percentage ask-others hit: " + str(float(totalGotFromOtherClients)/totalAskOtherClients) + "\r\n"
	out += "Total Queries to RDBMS: " + str(totalRequests - totalCacheHit - totalGotFromOtherClients) + "\r\n"
	out += "Total Evictions: " + str(totalEvictions) + "\r\n"
	out += "Avr resp time per read: " + str(avrReadTime / len(files)) + "\r\n" 
	out += "Avr resp time per read from dbms: " + str(avrReadTimeFromDBMS / len(files)) + "\r\n"
	out += "Avr resp time per read from others: " + str(avrReadTimeFromOthers / len(files)) + "\r\n"
	out += "Avr resp time per read from others then dbms: " + str(avrReadTimeFromOthersAndDBMS / len(files)) + "\r\n"
	out += "Avr resp time per write: " + str(avrWriteTime / len(files)) + "\r\n"
	out += "Total Query Response Time (from when the query is issued to when the client gets the result) in milliseconds: " + str(totalQueryResponseTime) + "\r\n"
	out += "Total RoundTripTime (end-start for each action): " + str(totalRoundTripTime) + "\r\n"
	out += "Total Updates: " + str(totalUpdates) + "\r\n"
	out += "Total Keys actually be invalidated at its local cache: " + str(totalKeyInvalidated) + "\r\n"
	out += "Total Invalidated Attempt (may or may not find keys): " + str(totalInvalidatedAttempted) + "\r\n"
	out += "Total I Lease Granted: " + str(totalILeaseGranted) + "\r\n"
	out += "Total I Lease Released Success (the results are STORED): " + str(totalILeaseReleasedSuccess) + "\r\n"
	out += "Total I Lease Backoff: " + str(totalILeaseBackOff) + "\r\n"
	out += "Total Q Lease Granted: " + str(totalQLeaseGranted) + "\r\n"
	out += "Total COPIES msg received from other clients: " + str(totalCopies) + "\r\n"
	out += "Total STEAL msg recv from others: " + str(totalSteal) + "\r\n"
	
	if totalAskOtherClients != 0:
		out += str(totalCacheHit) + "," + str(totalAskOtherClients) + "," + str(totalGotFromOtherClients) + "," + \
			str(totalRequests) + "," + str(float(totalCacheHit)/totalRequests) + "," + str(float(totalGotFromOtherClients)/totalAskOtherClients) + "," + \
			str(totalRequests - totalCacheHit - totalGotFromOtherClients) + "," + \
			str(totalEvictions) + "," + str(totalQueryResponseTime) + "," + str(totalRoundTripTime) + "," + str(totalUpdates) + "," + \
			str(totalKeyInvalidated) + "," + str(totalInvalidatedAttempted) + "," + str(totalILeaseGranted) + "," + str(totalILeaseReleasedSuccess) + "," + \
			str(totalILeaseBackOff) + "," + str(totalQLeaseGranted) + "," + str(totalCopies) + "," + str(totalSteal) + "\r\n"
	else:
		out += str(totalCacheHit) + "," + str(totalAskOtherClients) + "," + str(totalGotFromOtherClients) + "," + \
			str(totalRequests) + "," + str(float(totalCacheHit)/totalRequests) + "," + str(0) + "," + \
			str(totalRequests - totalCacheHit - totalGotFromOtherClients) + "," + \
			str(totalEvictions) + "," + str(totalQueryResponseTime) + "," + str(totalRoundTripTime) + "," + str(totalUpdates) + "," + \
			str(totalKeyInvalidated) + "," + str(totalInvalidatedAttempted) + "," + str(totalILeaseGranted) + "," + str(totalILeaseReleasedSuccess) + "," + \
			str(totalILeaseBackOff) + "," + str(totalQLeaseGranted) + "," + str(totalCopies) + "," + str(totalSteal) + "\r\n"	
	out += "\r\n"
	
	files = [i for i in os.listdir(path) if 'metrics' in i]
	print(files)

	totalRequests = 0	
	totalReads = 0
	totalWrites = 0
	totalILeaseGranted = 0
	totalILeaseReleasedSuccess = 0
	totalILeaseRevoke = 0
	totalILeaseBackOff = 0
	totalMidFlightReads = 0
	totalMidFlightWrites = 0
	totalQLeaseGranted = 0
	for file in files:
		with open(path+file, 'r') as f:
			for line in f:
				vals = map(int, re.findall(r'\d+', line))
				val_str = ""
				for v in vals:
					val_str += str(v)
					
				val = int(val_str)				
				if 'total request' in line:
					totalRequests += val
				elif 'total reads' in line:
					totalReads += val
				elif 'total writes' in line:
					totalWrites += val
				elif 'total number of  I lease granted' in line:
					totalILeaseGranted += val
				elif 'total number of I lease released' in line:
					totalILeaseReleasedSuccess += val
				elif 'total number of I lease revoked' in line:
					totalILeaseRevoke += val
				elif 'total number of  backoff I lease' in line:
					totalILeaseBackOff += val
				elif 'total number of committed writes' in line:
					totalQLeaseGranted += val
				elif 'total number of  mid-flight read miss' in line:
					totalMidFlightReads += val
				elif 'total number of  mid-flight writes' in line:
					totalMidFlightWrites += val

	out += "Cores: " + "\r\n"
	out += "Total Reads: " + str(totalReads) + "\r\n"
	out += "Total Writes: " + str(totalWrites) + "\r\n"
	out += "Total I Leases granted: " + str(totalILeaseGranted) + "\r\n"
	out += "Total I Leases released success: " + str(totalILeaseReleasedSuccess) + "\r\n"
	out += "Total I Leases revoked: " + str(totalILeaseRevoke) + "\r\n"
	out += "Total I Leases backoff: " + str(totalILeaseBackOff) + "\r\n"
	out += "Total Q Leases granted: " + str(totalQLeaseGranted) + "\r\n"
	out += "Total Mid Fly Reads: " + str(totalMidFlightReads) + "\r\n"
	out += "Total Mid Fly Writes: " + str(totalMidFlightWrites) + "\r\n"
	out += str(totalReads) + "," + str(totalWrites) + "," + str(totalILeaseGranted) + "," + str(totalILeaseReleasedSuccess) + "," + \
		str(totalILeaseRevoke) + "," + str(totalILeaseBackOff) + "," + str(totalQLeaseGranted) + "," + str(totalMidFlightReads) + "," + str(totalMidFlightWrites) + "\r\n"
	out += "\r\n"
	
f = open('summary.txt','w')
f.write(out) # python will convert \n to os.linesep
f.close() #