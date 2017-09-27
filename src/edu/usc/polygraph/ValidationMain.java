package edu.usc.polygraph;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.PropertyConfigurator;

import edu.usc.polygraph.utils.CommonUtilities;
import edu.usc.polygraph.utils.ConvertLinuxLogs;
import edu.usc.polygraph.utils.CreateResourcesFiles;
import edu.usc.polygraph.utils.UtilConstants;

public class ValidationMain {

	public static void main(String[] args) {

		// if(ValidationConstants.countDiscardedWrites){
		// discardedRecs= new HashSet<String>();
		// propertiesWithState=new HashSet<String>();
		// }

		PropertyConfigurator.configure("log4j.properties");
		boolean osState=false;
		Validator.init(args);
		if (osState){
		CommonUtilities.startLinuxstats("127.0.0.1", "valid-" + Validator.clientId + "-" + Validator.numValidators,
				UtilConstants.USER, UtilConstants.tempOSStatsFolder);
		}
		if (ValidationParams.KAFKA_OS_STATS) {
			try {
				// Thread.sleep(10000);
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
			if (ValidationParams.USE_KAFKA) {
				String servers[] = ValidationParams.KAFKA_HOST.split(",");
				int index = 0;
				for (String server : servers) {
					CommonUtilities.startLinuxstats(server.split(":")[0],
							"kafkaserver-" + index + "-" + Validator.numValidators, "UtilConstants.USER",
							UtilConstants.tempOSStatsFolder);
					index++;
				}
			}
		}
		SimpleDateFormat ft = new SimpleDateFormat("'at' hh:mm:ss a");
		long runStartTime = System.currentTimeMillis();
		String startedString = "Staretd " + ft.format(new Date());
		System.out.println(startedString);
		ValidationStats validationStats = new ValidationStats(10000, 100000000L, Validator.application);

		ArrayList<Validator> validators = new ArrayList<Validator>();
		HashMap<Integer, Long> times = runValidators(validationStats, Validator.clientId, Validator.numClients, Validator.numPartitions,
				validators);
		
		long end = System.currentTimeMillis();
		long sumWriteLogsCount = 0, sumStaleCounter = 0, sumReadLogsCount = 0;
		long maxSchedulesAll = 0, fullyDiscardedWritesCountAll = 0, partiallyDiscardedWritesCountAll = 0;
		long sumCDSE=0;
		long sumReadOver=0;
		long sumfactorialCount=0;
		// long
		// maxRecords=0,fullyDiscardedWritesCount=0,partiallyDiscardedWritesCount=0;
		// long avgSchedules=0,avgRecords=0,maxSchedules=0;
		ArrayList<HashMap<Long,Bucket>> bucketsArr= new ArrayList<HashMap<Long,Bucket>>();
		try {

			for (Validator v : validators) {
				
				sumfactorialCount+=v.factorialCount;
				sumCDSE+=v.cdseCount;
				sumReadOver+=v.readOverLappingCount;
				for(ValidatorData vd:v.validatorData){
					if (ValidationParams.COMPUTE_FRESHNESS)
					bucketsArr.add(vd.freshnessBuckets);
					sumWriteLogsCount += vd.writeLogsCount;
					sumReadLogsCount += vd.readLogsCount;
					sumStaleCounter += vd.staleCount;
					fullyDiscardedWritesCountAll += vd.fullyDiscardedWritesCount;
					partiallyDiscardedWritesCountAll += vd.partiallyDiscardedWritesCount;



					
				}

				maxSchedulesAll = Math.max(maxSchedulesAll, v.maxSchedules);
			}

		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

		System.out.printf("Total Read count: %d\nTotal Write count: %d\nTotal stale count: %d\n", sumReadLogsCount,
				sumWriteLogsCount, sumStaleCounter );
		if (ValidationParams.PRINT_ALL_STATS){
			System.out.printf("Total CDSE count: %d\nTotal Read Overlapping count: %d\nTotal Factorial count: %d\n",sumCDSE,sumReadOver,sumfactorialCount);
		}
		if (ValidationParams.COMPUTE_FRESHNESS)
		{
		Bucket.printFreshnessBuckets(bucketsArr);
		Bucket.computeFreshnessConfidence(bucketsArr);
		}
		System.out.println(startedString);
		
		System.out.println("Finished " + ft.format(new Date()));
		long durationM = (long) Math.floor((end - runStartTime) / 60000.0);
		long durationS = ((end - runStartTime) % 60000) / 1000;
		long durationSS = ((end - runStartTime) % 60000) % 1000;
		// String timeUnit = " miniutes";
		// if (duration == 0) {
		// duration = (end - start) / 1000;
		// timeUnit = " seconds";
		// }
		String durationStr = String.format("%02d:%02d.%03d", durationM, durationS, durationSS);
		System.out.println("Duration = " + durationStr);

		

		double avgMem = validationStats.getSumMU() / validationStats.getCountMU();
		double maxMem = validationStats.getMemoryUsageMax();

		System.out.println("Average memory usage = " + avgMem + " MB");
		System.out.println("Max memory usage = " + maxMem + " MB");
		System.out.print("ID, Reads, Updates, Stale Count");
		if (ValidationParams.PRINT_ALL_STATS){
			System.out.print(",Average Schedules (Max),Average Records (Max),Fully Discarded Writes Count, Partially Discarded Writes Count, Unordered Read Count, Unordered Write Count");
		}
		
			System.out.println();
		
		long avgSchAll = 0;
		for (Validator v : validators) {
			long readLogsCount=0;
			long writeLogsCount=0;
			long unorderedR=0;
			long unorderedW=0;
			long staleCounter=0;
			long fullyDiscardedWritesCount=0;
			long partiallyDiscardedWritesCount=0;
			for(ValidatorData vd:v.validatorData){
				readLogsCount+=vd.readLogsCount;
				writeLogsCount+=vd.writeLogsCount;
				staleCounter+=vd.staleCount;
				unorderedR+=vd.unorderedReadsCount;
				unorderedW+=vd.unorderedWritesCount;
				fullyDiscardedWritesCount+=vd.fullyDiscardedWritesCount;
				partiallyDiscardedWritesCount+=vd.partiallyDiscardedWritesCount;
			}
			long avgSch = 0;
			try {
				avgSch = v.avgSchedules / readLogsCount;
			} catch (ArithmeticException e) {
				e.printStackTrace(System.out);
			}
			avgSchAll += avgSch;
			
			System.out.printf("%d,%d,%d,%d",v.validatorID, readLogsCount, writeLogsCount,staleCounter);
			//System.out.printf(",%d (%d)", avgSch, v.maxSchedules);
			long avgRec = 0;
			try {
				avgRec = v.avgRecords / readLogsCount;
			} catch (ArithmeticException e) {
				e.printStackTrace(System.out);
			}
			if (ValidationParams.PRINT_ALL_STATS)
			System.out.printf(",%d (%d),", avgRec, v.maxRecords);
			String addedstr = "";
			if (ValidationParams.countDiscardedWrites && ValidationParams.PRINT_ALL_STATS) {
				System.out.print(fullyDiscardedWritesCount + "," + partiallyDiscardedWritesCount + ",");
			}
			// MR1
			long durationM_pullTime = (long) Math.floor(v.pullTime / 60000.0);
			long durationS_pullTime = (v.pullTime % 60000) / 1000;
			long durationSS_pullTime = (v.pullTime % 60000) % 1000;
			String durationStr_pullTime = String.format("%02d:%02d.%03d", durationM_pullTime, durationS_pullTime, durationSS_pullTime);
			if (ValidationParams.PRINT_ALL_STATS)
			System.out.printf(",%d,%s",v.pullTime,durationStr_pullTime);

			//long duration = times.get(v.validatorID) - runStartTime;
			
			long duration2=v.finishTime-runStartTime;
			long duration=duration2;
			long durationM_FTE = (long) Math.floor(duration / 60000.0);
			long durationS_FTE = (duration % 60000) / 1000;
			long durationSS_FTE = (duration % 60000) % 1000;
			String durationStr_FTE = String.format("%02d:%02d.%03d", durationM_FTE, durationS_FTE, durationSS_FTE);			
			
			long validation = duration - v.pullTime;
			
			long validationM_FTE = (long) Math.floor(validation / 60000.0);
			long validationS_FTE = (validation % 60000) / 1000;
			long validationSS_FTE = (validation % 60000) % 1000;
			String validationStr_FTE = String.format("%02d:%02d.%03d", validationM_FTE, validationS_FTE, validationSS_FTE);
			if (ValidationParams.PRINT_ALL_STATS){
			System.out.printf(",%d,%s",validation,validationStr_FTE);
			System.out.printf(",%d,%s",duration,durationStr_FTE);
			if(v.measureCount==0)
				v.measureCount=1;
			double avgDBSize=v.dbStateSize/v.measureCount;
			double avgSSSize=v.serialSchedSize/v.measureCount;
		
			System.out.print(","+avgSSSize+","+avgDBSize+","+v.totalScheds/v.measureCount+","+v.uniqueSS/v.measureCount);
			System.out.print(","+unorderedR+","+unorderedW);
			}
			System.out.println();

		}
		String id = String.valueOf(Validator.numValidators);
		if (!ValidationParams.USE_KAFKA) {
			id = Validator.getFileName(Validator.logDir);
		}
		if (osState){
		//System.out.println("Starting  stopLinuxstats.");
		CommonUtilities.stopLinuxstats(UtilConstants.OSStatsFolder, "127.0.0.1", UtilConstants.USER,
				UtilConstants.tempOSStatsFolder);
	//	System.out.println("Completed stopLinuxstats.");
		ConvertLinuxLogs.ConvertFilesToCSV(UtilConstants.OSStatsFolder + "/",
				"valid-" + Validator.clientId + "-" + Validator.numValidators);
	//	System.out.println("Completed ConvertFilesToCSV");
		CreateResourcesFiles.createCharts(UtilConstants.OSStatsFolder,
				"valid-" + Validator.clientId + "-" + Validator.numValidators, UtilConstants.pythonscriptpath);
	//	System.out.println("Completed createCharts");

//		System.out.println(id + ",," + sumReadLogsCount + "," + sumWriteLogsCount + "," + durationStr + ",," + sumStaleCounter + "," + avgMem + "," + maxMem + ","
//				+ (avgSchAll / Validator.numValidators) + "," + maxSchedulesAll + "," + fullyDiscardedWritesCountAll
//				+ "," + partiallyDiscardedWritesCountAll);
		}
		if (ValidationParams.KAFKA_OS_STATS) {

			// kafka server
			if (ValidationParams.USE_KAFKA) {
				String servers[] = ValidationParams.KAFKA_HOST.split(",");
				int index = 0;
				for (String server : servers) {
					CommonUtilities.stopLinuxstats(UtilConstants.OSStatsFolder, server.split(":")[0],
							UtilConstants.USER, UtilConstants.tempOSStatsFolder);

					ConvertLinuxLogs.ConvertFilesToCSV(UtilConstants.OSStatsFolder + "/",
							"kafkaserver-" + index + "-" + Validator.numValidators);
					CreateResourcesFiles.createCharts(UtilConstants.OSStatsFolder,
							"kafkaserver-" + index + "-" + Validator.numValidators, UtilConstants.pythonscriptpath);
					index++;
				}
			}

		}
		Validator.close();

	}

	private static HashMap<Integer, Long> runValidators(ValidationStats validationStats, int clientId, int numClients,
			int numPartitions, ArrayList<Validator> validators) {
		
		int numThreads = Validator.numValidators / numClients;
		int remainingThreads = Validator.numValidators % numClients;

		if (clientId < remainingThreads)
			numThreads++;
		Validator.myNumThreads = numThreads;
		Validator.threadsStart= new CountDownLatch(numThreads);
		ArrayList<ArrayList<Integer>> threadsPartitions = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < numPartitions; i++) {
			int threadId = i % Validator.numValidators;

			ArrayList<Integer> partitions;
			if (threadsPartitions.size() <= threadId) {
				partitions = new ArrayList<Integer>();
				threadsPartitions.add(partitions);
			} else {
				partitions = threadsPartitions.get(threadId);
			}

			partitions.add(i);
		}
		ExecutorService exec = Executors.newFixedThreadPool(numThreads);
		Set<Future<Integer>> set = new HashSet<Future<Integer>>();
		// ArrayList<Validator> validators = new ArrayList<Validator>();
		System.out.printf("Starting %d Validator threads with %d partitions.%n", numThreads, numPartitions);
		for (int i = 0; i < Validator.numValidators; i++) {
			if (i % numClients == clientId/* && i==0*/) {
				Validator validatorThread = new Validator(i, threadsPartitions.get(i));
				validators.add(validatorThread);
				Future<Integer> future = exec.submit(validatorThread);
				set.add(future);
			}
		}
		// ExecutorService exec =
		// Executors.newFixedThreadPool(endIndex-startIndex+1);
		// Set<Future<Long>> set = new HashSet<Future<Long>>();
		// ArrayList<Validator> validators = new ArrayList<Validator>();
		// System.out.printf("Starting %d Validator threads. Starting from %d
		// and end index %d..%n", (endIndex-startIndex+1), startIndex,endIndex);
		// for (int i = startIndex; i <= endIndex; i++) {
		// Validator validatorThread = new Validator(i);
		// validators.add(validatorThread);
		// Future<Long> future = exec.submit(validatorThread);
		// set.add(future);
		// }
		validationStats.setValidators(validators);
		validationStats.setStartTime();
		Thread statsThread = new Thread(validationStats);
		statsThread.start();
		// long[] times = new long[numThreads];
		HashMap<Integer, Long> times = new HashMap<Integer, Long>();

		try {
			for (Future<Integer> future : set) {
				int vID = future.get();
				assert !times.containsKey(vID):"Each validator id should be inserted once";
				times.put(vID, System.currentTimeMillis());
			}
			exec.shutdown();
			validationStats.termentate();
			statsThread.join();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

		System.out.printf("Validator threads has finished %n");
		return times;
	}

}

class ValidationStats implements Runnable {
	private Runtime runtime = Runtime.getRuntime();
	private long sumMU = 0, countMU = 0;
	private long memoryUsageMax = -1;
	private long sleepTime = 1000, overflowConstant = 10000;
	private boolean stop = false;
	private long runStartTime;
	private String statsTopic;
	KafkaProducer<String, String> statsProducer;
	private ArrayList<Validator> validators;

	private String getStatsString() {
		long end = System.currentTimeMillis();
		long durationM = (long) Math.floor((end - runStartTime) / 60000.0);
		long durationS = ((end - runStartTime) % 60000) / 1000;
		long durationSS = ((end - runStartTime) % 60000) % 1000;
		// String timeUnit = " miniutes";
		// if (duration == 0) {
		// duration = (end - start) / 1000;
		// timeUnit = " seconds";
		// }

		String durationStr = String.format("%02d:%02d.%03d", durationM, durationS, durationSS);

		StringBuilder sb = new StringBuilder();
		long readLogsCount = 0;
		long writeLogsCount = 0;
		long staleCounter = 0;
		long maxSchedules = 0;
		long fullyDiscardedWritesCount = 0;
		long partiallyDiscardedWritesCount = 0;
		long avgSchedules = 0;

		for (Validator v : validators) {
			for(ValidatorData vd:v.validatorData){

			readLogsCount += vd.readLogsCount;
			writeLogsCount += vd.writeLogsCount;
			staleCounter += vd.staleCount;
			partiallyDiscardedWritesCount += vd.partiallyDiscardedWritesCount;
			fullyDiscardedWritesCount += vd.fullyDiscardedWritesCount;

			}
			maxSchedules = Math.max(maxSchedules, v.maxSchedules);
	
			avgSchedules += v.avgSchedules;
		}
		long avgSched = avgSchedules / validators.size();
		sb.append(Validator.numClients);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(Validator.clientId);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(readLogsCount);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(writeLogsCount);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(staleCounter);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append( avgSched);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append( maxSchedules);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		long count = countMU;
		if (count == 0)
			count = 1;
		sb.append((sumMU / count));
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append( memoryUsageMax);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append( partiallyDiscardedWritesCount);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(fullyDiscardedWritesCount);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(durationStr);
		sb.append(ValidationParams.lineSeparator);
		return sb.toString();
	}

	public void setValidators(ArrayList<Validator> validators) {
		this.validators = validators;
	}

	public void setStartTime() {
		runStartTime = System.currentTimeMillis();
	}

	ValidationStats(int sleepTime, long overflowConstant, String topic) {
		statsTopic = "STATS_" + topic;

		if (ValidationParams.USE_KAFKA&&ValidationParams.PRODUCE_STATS) {
			statsProducer = Validator.initStatsProducer(topic);
		}
		runStartTime = System.currentTimeMillis();
		if (sleepTime <= 1000)
			sleepTime = 1000;
		this.sleepTime = sleepTime;
		if (overflowConstant <= 0)
			overflowConstant = 1;
		this.overflowConstant = overflowConstant;
	}

	public long getSumMU() {
		return sumMU;
	}

	public long getCountMU() {
		return countMU;
	}

	public long getMemoryUsageMax() {
		return memoryUsageMax;
	}

	@Override
	public void run() {
		while (!stop) {
			long currentMU_inBytes = runtime.totalMemory() - runtime.freeMemory();
			long currentMU_MB = currentMU_inBytes / 1048576;
			if (memoryUsageMax < currentMU_MB)
				memoryUsageMax = currentMU_MB;
			sumMU += currentMU_MB;
			countMU++;
			if (countMU >= overflowConstant) {
				if (countMU == 0)
					countMU = 1;
				sumMU = sumMU / countMU;
				countMU = 1;
			}
			if (sumMU < 0) {
				sumMU = 0;
				countMU = 0;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			if (ValidationParams.USE_KAFKA&&ValidationParams.PRODUCE_STATS)
				statsProducer.send(new ProducerRecord<String, String>(statsTopic, 0, null, getStatsString()));
		}

		if (ValidationParams.USE_KAFKA&&ValidationParams.PRODUCE_STATS){
			
			statsProducer.flush();
			statsProducer.close();
		}
	}

	public void resetCounters() {
		sumMU = 0;
		countMU = 0;
		memoryUsageMax = 0;
	}

	public void termentate() {

		stop = true;
	}

}
