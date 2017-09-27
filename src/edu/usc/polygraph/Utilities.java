package edu.usc.polygraph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


class BGLogObject {
	String optype = "", mopType = "", seqId, threadId, rid, value, starttime, endtime, updatetype; // only for update log records
	String actionType;

	public BGLogObject(String opt, String mot, String sid, String tid, String resid, String start, String end, String val, String type, String actionType) {
		optype = opt;
		mopType = mot;
		seqId = sid;
		threadId = tid;
		rid = resid;
		value = val;
		starttime = start;
		endtime = end;
		updatetype = type;
		this.actionType = actionType;
	}

	public String getActionType() {
		return actionType;
	}

	public void setActionType(String actionType) {
		this.actionType = actionType;
	}

	public String getOptype() {
		return optype;
	}

	public void setOptype(String optype) {
		this.optype = optype;
	}

	public String getMopType() {
		return mopType;
	}

	public void setMopType(String mopType) {
		this.mopType = mopType;
	}

	public String getSeqId() {
		return seqId;
	}

	public void setSeqId(String seqId) {
		this.seqId = seqId;
	}

	public String getThreadId() {
		return threadId;
	}

	public void setThreadId(String threadId) {
		this.threadId = threadId;
	}

	public String getRid() {
		return rid;
	}

	public void setRid(String rid) {
		this.rid = rid;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getStarttime() {
		return starttime;
	}

	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}

	public String getEndtime() {
		return endtime;
	}

	public void setEndtime(String endtime) {
		this.endtime = endtime;
	}

	public String getUpdatetype() {
		return updatetype;
	}

	public void setUpdatetype(String updatetype) {
		this.updatetype = updatetype;
	}

}

public class Utilities {

	private static final String APPLICATION = "application";
	private static final String APPLICATION_NAME = "name";
	private static final String APPLICATION_ENTITIES = "entities";
	private static final String ENTITY_NAME = "name";
	private static final String ENTITY_PROPERTIES = "properties";
	private static final String ENTITY_INSERT_TRANSACTIONS = "inserttrans";
	private static final String ENTITY_TYPE_INSERT = "INSERT";
	private static final String ENTITY_TYPE_UPDATE = "UPDATE";
	private static final String ENTITY_TYPE_RW = "READ&WRITE";
	private static final String SPLIT_TRANSACTION_INFO = "[:]";
	private static final String SPLIT_ENTITY_INFO = "[,]";
	private static final String SPLIT_PROPERTY_INFO = "[;]";
	private static final int TNAME_INDEX = 1;
	private static final int ENAME_INDEX = 1;
	private static final int PNAME_INDEX = 1;
	private static final int E_EID_INDEX = 0;
	private static final int TE_EID_INDEX = 2;
	private static final int INDEX_OF_FIRST_PROPERTY_IN_E = 2;
	private static final int INDEX_OF_FIRST_PROPERTY_IN_TE = 4;
	private static final int INDEX_OF_FIRST_ENTITY = 2;
	private static final int INDEX_OF_ENTITY_TYPE = 3;
	
	
	public static final String newline = System.getProperty("line.separator");

	public static String concatWithSeperator(char Seperator, String... params) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < params.length; i++) {
			sb.append(params[i]);
			if (i + 1 != params.length)
				sb.append(Seperator);
		}
		return sb.toString();
	}

	public static boolean compareValues(String value1, String value2) {
		if (isNumeric(value1) && isNumeric(value2)) {
			double a = Double.parseDouble(value1);
			double b = Double.parseDouble(value2);
			double c = Math.abs(a - b);
			return ValidationParams.ERROR_MARGIN > c;

		} else {
			return value1.equals(value2);
		}
	}

	public static boolean isNumeric(String value) {
		try {
			Double.parseDouble(value);
			return true;
		} catch (Exception e) {
			return false;
		}

	}

	public static String concat(String... params) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < params.length; i++) {
			sb.append(params[i]);
		}
		return sb.toString();
	}

	public static String concat(String st1, char ch, String st2) {
		StringBuffer sb = new StringBuffer();
		sb.append(st1);
		sb.append(ch);
		sb.append(st2);
		return sb.toString();

	}

	public static String concat(String st1, String st2, char ch, String st3) {
		StringBuffer sb = new StringBuffer();
		sb.append(st1);
		sb.append(st2);
		sb.append(ch);
		sb.append(st3);
		return sb.toString();

	}

	public static String applyIncrements(String v1, String v2) {
		double result = Double.parseDouble(v1) + Double.parseDouble(v2);
		String str = null;
		if (result == 0)
			str = "0.00";
		else
			str = ValidationParams.DECIMAL_FORMAT.format(result);
		return str;
	}

	// private String getStocksLogString(int ol_count) {
	// StringBuilder sb = new StringBuilder();
	// for (int i = 1; i <= ol_count; i++) {
	// String tokens[] = ((String) this.transactionResults.get("stock" + i)).split("_");
	// String stockId = generateID(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
	// String properties = getPropertiesString(ValidationConstants.STOCK_PROPERIES, tokens[2], ValidationConstants.NEW_VALUE_UPDATE);
	// sb.append(getEntityLogString(ValidationConstants.STOCK_ENTITY, stockId, properties));
	// if (i != ol_count)
	// sb.append(ValidationConstants.ENTITY_SEPERATOR);
	//
	// }
	// return sb.toString();
	// }
	public static String getLogString(LogRecord r) {

		StringBuilder sb = new StringBuilder();
		sb.append(r.getType());
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getActionName());
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
//		if (ValidationConstants.USE_SEQ){
//		String tokens[] = r.getId().split(ValidationConstants.KEY_SEPERATOR + "");
//		sb.append(tokens[0]);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(tokens[1]);
//		}
		//else{
			sb.append(r.getId());
		//}
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getStartTime());
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getEndTime());
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		String entities = Utilities.generateEntitiesLog(r.getEntities());
		sb.append(entities);
		sb.append(newline);
		return sb.toString();
	}


	public static String getLogString(char type, String actionName, int threadId, int sequenceId, long startTime, long endTime, String... entities) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(threadId);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(sequenceId);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(startTime);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < entities.length; i++) {
			sb.append(entities[i]);
			if ((i + 1) != entities.length)
				sb.append(ValidationParams.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}
	public static void executeRuntime(String cmd, boolean wait, String dist) {
		Process p;
		//File ufile = new File(dist);
		//FileWriter ufstream = null;
//		try {
//		//	ufstream = new FileWriter(ufile);
//		} catch (IOException e) {
//			e.printStackTrace(System.out);
//		}
		StringBuilder sb = new StringBuilder();
		//BufferedWriter file = new BufferedWriter(ufstream);
		try {

			p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmd });
			if (wait) {
				InputStream stdout = p.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line + "\n");
				}
				p.waitFor();
			} else
				Thread.sleep(5000);
		} catch (Exception e2) {
			e2.printStackTrace(System.out);
		}
//		try {
//			if (sb.length() > 0)
//				sb.deleteCharAt(sb.length() - 1);
////			file.write(sb.toString());
////			file.flush();
////			file.close();
//		} catch (IOException e) {
//			e.printStackTrace(System.out);
//		}
	}

	public static int getNumOfFiles(String logDir) {
		File dir = new File(logDir);
		return dir.list().length;
	}

	public static String executeRuntime(String cmd, boolean wait) {
		Process p;

		StringBuilder sb = new StringBuilder();
		try {

			p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmd });
			if (wait) {
				InputStream stdout = p.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}
				p.waitFor();
			} else
				Thread.sleep(5000);
		} catch (Exception e2) {
			e2.printStackTrace(System.out);
		}

		return sb.toString();

	}

	public static void replayBGLogs(boolean sort,String logDir, String topic, int numValidators, boolean multiThreads, boolean multiTopic) {
		int threadCount = getNumOfFiles(logDir) / 2;
		if (!multiThreads) {
			threadCount = 1;
		}
		KafkaProducer<String, String> kafkaProducer = null;
		try {
			Properties props = new Properties();
			props.put("bootstrap.servers", ValidationParams.KAFKA_HOST);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			//props.put(" key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			kafkaProducer = new KafkaProducer<String, String>(props);
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

		ExecutorService exec = Executors.newFixedThreadPool(threadCount);
		Set<Future<Long>> set = new HashSet<Future<Long>>();
		for (int i = 0; i < threadCount; i++) {
			ReplayThread t = new ReplayThread(sort,i + "", kafkaProducer, numValidators, logDir, topic, multiThreads, multiTopic);

			Future<Long> future = exec.submit(t);
			set.add(future);
		}

		try {
			long sum = 0;
			for (Future<Long> future : set) {
				sum += future.get();
			}
			System.out.println("The sum of the logs:" + sum);
			System.out.println("Done");
			kafkaProducer.flush();
			kafkaProducer.close();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

	}

	static class ReplayThread implements Callable<Long> {
		private static final boolean REMOVE_DELTA = true;
		long totalLogsLen=0;
		String id;
		KafkaProducer<String, String> producer;
		int numValidators;
		String logDir;
		String topic;
		boolean multiThreads;
		boolean multiTopic;
		private long countAll=0;
		long maxLog=0;
		boolean sorted=false;
		public ReplayThread(boolean sort,String id, KafkaProducer<String, String> producer, int validators, String dir, String topic, boolean multi, boolean multiTopic) {
			this.id = id;
			this.producer = producer;
			this.numValidators = validators;
			this.logDir = dir;
			this.topic = topic;
			multiThreads = multi;
			this.multiTopic = multiTopic;
			this.sorted=sort;

		}

		@Override
		public Long call() throws Exception {
			boolean doneCheck =false;
			int threadCount = 2;
			if (!multiThreads) {
				threadCount = getNumOfFiles(logDir);

			}
			FileInputStream[] fstreams = new FileInputStream[threadCount];
			DataInputStream[] dataInStreams = new DataInputStream[threadCount];
			BufferedReader[] bReaders = new BufferedReader[threadCount];
			long logsCount = 0;
			HashSet<String> logs = new HashSet<String>();
			for (int i = 0; i < threadCount; i = i + 2) {

				try {
					int machineid = 0;
					if (!multiThreads) {
						id = i / 2 + "";
					}
					fstreams[i] = new FileInputStream(logDir + "/read" + machineid + "-" + id + ".txt");
					dataInStreams[i] = new DataInputStream(fstreams[i]);
					bReaders[i] = new BufferedReader(new InputStreamReader(dataInStreams[i]));

					fstreams[i + 1] = new FileInputStream(logDir + "/update" + machineid + "-" + id + ".txt");
					dataInStreams[i + 1] = new DataInputStream(fstreams[i + 1]);
					bReaders[i + 1] = new BufferedReader(new InputStreamReader(dataInStreams[i + 1]));
				} catch (FileNotFoundException e) {
					e.printStackTrace(System.out);
					System.out.println("Log file not found " + e.getMessage());
				}
			}

			// ==================================================================
			int seq = 0;
			try {
				String line = null;
				boolean allDone = false;
				LogRecord[] records = new LogRecord[threadCount];
				while (!allDone) {
					allDone = true;
					for (int i = 0; i < threadCount; i++) {
						if (records[i] == null) {
							if ((line = bReaders[i].readLine()) != null) {
								if (!doneCheck){
									doneCheck=true;
									
									if (line.split(",").length>6)
										ValidationParams.USE_SEQ = true;
									System.out.println("SEQ="+ValidationParams.USE_SEQ);
								}
								records[i] = LogRecord.createLogRecord(line);
								allDone = false;
							}
						} else {
							allDone = false;
						}
					}
					LogRecord currentRecord = null;
					if (allDone) {
						;
						// if (ReadWrite.size() != 0) {
						// allDone = false;
						// currentRecord = ReadWrite.get(0);
						// ReadWrite.remove(0);
						// }
					} else {
						if(sorted)
						currentRecord = getEarilestRecord(records);
						else
							currentRecord=getRecord(records);
					}
					if (!allDone) {
						// if(currentRecord.getId().equals("57-0"))
						// System.out.println();
						while (logs.contains(currentRecord.getId())) {
							System.out.println("Log:" + currentRecord.getId() + " already exit");

							currentRecord.setId("101-" + seq);
							seq++;
							// System.out.println("Log:"+ currentRecord.getId() +" already exit");
							// System.exit(0);
						}
						logs.add(currentRecord.getId());
						
						if (REMOVE_DELTA){
							removeDeltas(currentRecord);
						}
						sendToKafka(currentRecord);
						logsCount++;
						if (logsCount >25000000)
							System.exit(0);
					}
				}
			} catch (Exception e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}
			try {
				for (int i = 0; i < threadCount; i++) {
					if (dataInStreams[i] != null)
						dataInStreams[i].close();
					if (bReaders[i] != null)
						bReaders[i].close();
				}
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			System.out.println("total length="+totalLogsLen);
			System.out.println("max log length="+maxLog);

			return logsCount;
		}

		private void removeDeltas(LogRecord currentRecord) {
			if (currentRecord.getActionName().equalsIgnoreCase("NO")){
				for(Entity e: currentRecord.getEntities()){
					if (e.getName().equalsIgnoreCase("DIST")){
						Property[] props = e.getProperties();
						assert props.length==2;
						int deltaIndex=0;
						int readIndex=1;
						if (props[1].getType()==ValidationParams.INCREMENT_UPDATE){
							deltaIndex=1;
							readIndex=0;
						}
						int rvalue = Integer.parseInt(props[readIndex].getValue());
						props[deltaIndex].setValue(String.valueOf(rvalue+1));
						props[deltaIndex].setType(ValidationParams.NEW_VALUE_UPDATE);
						
					}
					
				}
			}
			
			
		}

		private int roundRobin=0;
		private LogRecord getRecord(LogRecord[] records) {
			if (records == null || records.length == 0) {
				System.out.println("ERROR: (records == null || records.length == 0) returned true");
				System.exit(0);
			}
			boolean done=false;
			int start=roundRobin;LogRecord min = null;
			while(!done){
				 min = records[roundRobin];records[roundRobin]=null;
				roundRobin=(roundRobin+1)%records.length;
				if(min!=null)
					done=true;
				if (roundRobin==start)
					break;
			}
			return min;
		}

		private void sendToKafka(LogRecord currentRecord) {
			countAll++;
			if(countAll % 100000 == 0){
				System.out.println("sending:"+countAll);
			}
			Entity[] entities = currentRecord.getEntities();
//			if (entities.length > 1) {
//				System.out.println("Error: Expecting one entity");
//				System.exit(0);
//			}
			int key = Integer.parseInt(entities[0].key.split("-")[0]);
			for (Entity e: entities){
				if (numValidators!=1&&key!=Integer.parseInt(e.key.split("-")[0])){
					System.out.println("Error partitioning key not match");
					System.exit(0);
				}
			}
			// R_S = (TopicId % NumVal)
			// R_E = (TopicId % NumVal) + (NumVal)
			// W_S = (TopicId % NumVal) + (2 * NumVal)
			// W_E = (TopicId % NumVal) + (3 * NumVal)

			String tempTopic = topic;
			if (multiTopic) {
				numValidators = 2;
				tempTopic += (key % numValidators);
				numValidators = 1;
			}
			int partition = (key % numValidators);
			if (currentRecord.getType() != ValidationParams.READ_RECORD)
				partition = (key % numValidators) + (2 * numValidators);

			//producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), Long.toString(currentRecord.getStartTime())));
			partition = (key % numValidators) + 0*numValidators;
			if (currentRecord.getType() != ValidationParams.READ_RECORD)
				partition = (key % numValidators) + (1 * numValidators);
			String value = Utilities.getLogString(currentRecord);
			producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), value));
			StringSerializer ser= new StringSerializer();
			totalLogsLen+=value.length();
			if (value.length()> maxLog)
				maxLog=value.length();
		//if(ser.serialize(tempTopic,value).length>1000)
		//	System.out.println(ser.serialize(tempTopic,value).length);
		}

	}

	public static LogRecord getEarilestRecord(LogRecord[] records) {
		if (records == null || records.length == 0) {
			System.out.println("ERROR: (records == null || records.length == 0) returned true");
			System.exit(0);
		}
		LogRecord min = records[0];
		int index = 0;
		for (int i = 1; i < records.length; i++) {
			if (records[i] == null)
				continue;
			if (min == null || min.getStartTime() > records[i].getStartTime()) {
				index = i;
				min = records[i];
			}
		}
		records[index] = null;
		return min;
	}

//	public static String getStaleLogString(char type, String actionName, String id, long startTime, long endTime, long readOffset, long updateOffset, String... entities) {
//		StringBuilder sb = new StringBuilder();
//		sb.append(type);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(actionName);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(id);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(startTime);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(endTime);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(readOffset);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(updateOffset);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		for (int i = 0; i < entities.length; i++) {
//			sb.append(entities[i]);
//			if ((i + 1) != entities.length)
//				sb.append(ValidationConstants.ENTITY_SEPERATOR);
//		}
//		sb.append(newline);
//		return sb.toString();
//	}

	public static String getEntityLogString(String name, String key, String properies) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(key);
		sb.append(ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(properies);
		return sb.toString();
	}

	public static final String ValidationLogDir = "C:/Users/MR1/Documents/Validation/logs";

	private static void restartTopics(String topic, int numOfValidators, boolean multiTopics) {
		for (int i = 0; i < (multiTopics ? numOfValidators : 1); i++) {
			KafkaScripts.deleteTopic(topic + (multiTopics ? i : ""));
			KafkaScripts.deleteTopic("STATS_" + topic + (multiTopics ? i : ""));
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String result = KafkaScripts.createTopic(topic + (multiTopics ? i : ""), (multiTopics ? 2 : 2 * numOfValidators));
			if (result.equals("already exists") || result.toLowerCase().contains("error")) {
				System.out.println(result+ ". Retrying...");
				restartTopics(topic, numOfValidators, multiTopics);
			}
			if (!KafkaScripts.isTopicExist(topic)) {
				System.out.println("topic was not created. Retrying ...");
				restartTopics(topic, numOfValidators, multiTopics);

			}
		}
	}

	public static void main(String[] args) throws IOException {

		ValidationParams.USE_SEQ = false;
		boolean sort=true;
		System.out.println("Sort="+sort);
		System.out.println("RemoveDelta="+ReplayThread.REMOVE_DELTA);
		String logDir=args[0];
		int numPart = Integer.parseInt(args[1]);
		String topic = args[2];
		boolean multiTopic = false;
		restartTopics(topic, numPart, multiTopic);
		int numS=5;
//	System.exit(0);
//		System.out.println("Reassigning" + numPart+"Partitions on "+ numS+" brokers..");
//		KafkaScripts.reAssignPartitions(topic, numPart, numS);
//		
		try {
			int i=1;
			if (numPart>100)
				i=numPart/100;
			Thread.sleep(60000);//180000
		} catch (Exception e) {

			e.printStackTrace();
		}
		System.out.println("Start producing logs...");
		System.out.println("numPartitions:"+numPart);

		replayBGLogs(sort,logDir, topic, numPart, false, multiTopic);
		System.exit(0);
		logDir = "C:/Users/MR1/Documents/Validation/BG-ValidationLogs/" + "BGTrace1/" + "10K-100fpm-100Threads-1Min-RjctInvtFrd-decInc";
		// String logDir = "C:/Users/MR1/Documents/Validation/BG-ValidationLogs/" + "short10sec/bg1k2friends10thread10secstale";
		String newLog = logDir + "/V";
		boolean b = createDirectory(newLog);
		ValidationParams.THREAD_COUNT = getNumOfFiles(logDir) / 2;
		parseBGLogFiles(logDir, ValidationParams.THREAD_COUNT, 0, newLog);
		System.out.println("Done Parsing.");
		moveToValidation(newLog, ValidationLogDir);
		System.out.println("Done Copying.");

		// ===========================

		// ValidationConstants.debug = true;
		// String dir = "C:/Users/MR1/Documents/Validation/debug/";
		// String src = dir + "src.txt";
		// String dest = dir + "dest.txt";
		// sortLogsInAFile(src, dest);
		// System.out.println("Done.");
	}

	public static String generateEntitiesLog(Entity[] entities) {
		// CUS;1-2-493;BALANCE:-374.44:N#YTD_P:374.44:N#P_CNT:2:N
		StringBuilder sb = new StringBuilder();
		try {

			for (Entity e : entities) {
				sb.append(concat(e.name, ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR + "", e.key, ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR + ""));
				for (Property p : e.getProperties()) {
					sb.append(concat(p.getName(), ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getValue(), ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getType() + "" + ValidationParams.PROPERY_SEPERATOR + ""));

				}
				sb.deleteCharAt(sb.length() - 1); // remove last #
				sb.append(concat(ValidationParams.ENTITY_SEPERATOR + ""));
			}

			sb.deleteCharAt(sb.length() - 1); // remove last &
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}

		return sb.toString();
	}
	public static String generateEntitiesString(ArrayList<Entity> entities) {
		// CUS;1-2-493;BALANCE:-374.44:N#YTD_P:374.44:N#P_CNT:2:N
		StringBuilder sb = new StringBuilder();
		try {

			for (Entity e : entities) {
				if (e.getProperties().length<1)
					continue;
				sb.append(concat(e.name, ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR + "", e.key, ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR + ""));
				for (Property p : e.getProperties()) {
					sb.append(concat(p.getName(), ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getValue(), ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getType() + "" + ValidationParams.PROPERY_SEPERATOR + ""));

				}
				sb.deleteCharAt(sb.length() - 1); // remove last #
				sb.append(concat(ValidationParams.ENTITY_SEPERATOR + ""));
			}

			sb.deleteCharAt(sb.length() - 1); // remove last &
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}

		return sb.toString();
	}
	
	public static void test(){
		

	}
	public static String getLogRecordString(char type, String actionName, String recordKey, long startTime, long endTime,ArrayList<Entity> entitiesArr) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(recordKey);
//		sb.append(ValidationConstants.RECORD_ATTRIBUTE_SEPERATOR);
//		sb.append(sequenceId);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(startTime);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
	
			sb.append(generateEntitiesString(entitiesArr));
		
		sb.append(newline);
		return sb.toString();
	}
	

	private static void moveToValidation(String oldDir, String newDir) throws IOException {
		File source = new File(oldDir);
		File dest = new File(newDir);
		FileUtils.cleanDirectory(dest);
		try {
			FileUtils.copyDirectory(source, dest);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean createDirectory(String newDir) throws IOException {
		File dir = new File(newDir);
		boolean b = dir.mkdir();
		FileUtils.cleanDirectory(dir);
		return b;
	}

	public static HashMap<String, LinkedList<DBState>> generateBGInitialState(int membersCount, int friendCountPerMember, int pendingCountPerMember) {
		HashMap<String, LinkedList<DBState>> initState = new HashMap<String, LinkedList<DBState>>();

		for (int i = 0; i < membersCount; i++) {
			String fCount = Integer.toString(friendCountPerMember);
			String pCount = Integer.toString(pendingCountPerMember);
			String [] memberInfo={fCount,pCount};
			DBState member = new DBState(0, memberInfo);
			LinkedList<DBState> memberLL = new LinkedList<DBState>();
			memberLL.add(member);
			initState.put(ValidationParams.MEMBER_ENTITY + ValidationParams.KEY_SEPERATOR + i, memberLL);
		}

		return initState;

	}

	public static void parseBGLogFiles(String dir, int threadCount, int machineid, String outputDir) {

		parseLogFiles(dir, threadCount, machineid, "update", outputDir);
		parseLogFiles(dir, threadCount, machineid, "read", outputDir);

	}

	private static int parseLogFiles(String dir, int threadCount, int machineid, String type, String outputDir) {
		int count = 0;

		for (int i = 0; i < threadCount; i++) {
			BufferedReader br = null;
			String line = null;
			String[] tokens = null;
			BufferedWriter log = null;
			FileWriter newfstream = null;
			FileInputStream fstream = null;
			DataInputStream in = null;

			try {
				File newfile = new File(outputDir + "/" + type + machineid + "-" + i + ".txt");
				newfstream = new FileWriter(newfile);

				log = new BufferedWriter(newfstream);

				fstream = new FileInputStream(dir + "/" + type + machineid + "-" + i + ".txt");
				in = new DataInputStream(fstream);
				br = new BufferedReader(new InputStreamReader(in));

			} catch (FileNotFoundException e) {
				e.printStackTrace(System.out);
				System.out.println("Log file not found " + e.getMessage());
				// Since the file isn't found - move to the next iteration
				continue;
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}

			try {
				int seq = 0;
				while ((line = br.readLine()) != null) {
					BGLogObject record = null;
					count++;
					tokens = line.split(",");
					if (type.equalsIgnoreCase("update")) {
						// UPDATE,PENDFRND,4,0,8519,556175393867908,556175407016902,1,I,InviteFriends
						// opt, item, itemId, String val, String start, String end, String actionType, String updateType, String threadId, String seqId
						record = new BGLogObject(tokens[0], tokens[1], String.valueOf(seq++)/* tokens[2] */, tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], tokens[8], tokens[9]);

						String propertyName[] = null, propertyValue = null;
						if (record.getMopType().equals("PENDFRND")) {
							propertyName = ValidationParams.PENDING_COUNT_PROPERIES;
						} else if (record.getMopType().equals("ACCEPTFRND")) {
							propertyName = ValidationParams.FRIEND_COUNT_PROPERIES;
						}

						String v = record.getUpdatetype();
						if (v.equals("I")) {
							propertyValue = "+1";
						}
						if (v.equals("D")) {
							propertyValue = "-1";
						}
						String memberId = record.getRid();

						String memberProperties = Utilities.getPropertiesString(propertyName, propertyValue, ValidationParams.INCREMENT_UPDATE);

						String membersLogString = Utilities.getEntityLogString(ValidationParams.MEMBER_ENTITY, memberId, memberProperties);

						String UpdateLogString = Utilities.getLogString(ValidationParams.UPDATE_RECORD, record.getActionType(), Integer.parseInt(record.getThreadId()), Integer.parseInt(record.getSeqId()), Long.parseLong(record.getStarttime()), Long.parseLong(record.getEndtime()), membersLogString);
						log.write(UpdateLogString);
					} else { // read file

						record = new BGLogObject(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "", tokens[8]);
						String propertyName[] = null;
						if (record.getMopType().equals("PENDFRND")) {
							propertyName = ValidationParams.PENDING_COUNT_PROPERIES;
						} else if (record.getMopType().equals("ACCEPTFRND")) {
							propertyName = ValidationParams.FRIEND_COUNT_PROPERIES;
						}
						String memberId = record.getRid();

						String memberProperties = Utilities.getPropertiesString(propertyName, record.getValue(), ValidationParams.VALUE_READ);

						String membersLogString = Utilities.getEntityLogString(ValidationParams.MEMBER_ENTITY, memberId, memberProperties);

						String readLogString = Utilities.getLogString(ValidationParams.READ_RECORD, record.getActionType(), Integer.parseInt(record.getThreadId()), Integer.parseInt(record.getSeqId()), Long.parseLong(record.getStarttime()), Long.parseLong(record.getEndtime()), membersLogString);
						log.write(readLogString);

						// record = new BGLogObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "", tokens[8]);

						// record = new BGLogObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "", tokens[8]));

					}
				} // end while
			} // end try
			catch (IOException e) {
				e.printStackTrace(System.out);
			}
			try {
				if (log != null)
					log.flush();
				log.close();

				if (newfstream != null)
					newfstream.close();
				if (br != null)
					br.close();
				if (fstream != null)
					fstream.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

		return count;
	}

	public static String getPropertiesString(String[] properiesNames, Object... params) {
		StringBuilder sb = new StringBuilder();
		int j = 0;
		for (int i = 0; i < properiesNames.length; i++, j++) {
			sb.append(properiesNames[i]);
			sb.append(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(params[j]);
			sb.append(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(params[++j]);
			if ((i + 1) != properiesNames.length)
				sb.append(ValidationParams.PROPERY_SEPERATOR);
		}
		return sb.toString();
	}

	private static void sortLogsInAFile(String src, String dist) throws IOException {
		FileInputStream fstreams = new FileInputStream(src);
		DataInputStream dataInStreams = new DataInputStream(fstreams);
		BufferedReader bReaders = new BufferedReader(new InputStreamReader(dataInStreams));

		File distFile = new File(dist);
		FileWriter ufstream = new FileWriter(distFile);
		BufferedWriter bWriter = new BufferedWriter(ufstream);

		String line = null;

		try {
			ArrayList<LogRecord> records = new ArrayList<LogRecord>();
			while ((line = bReaders.readLine()) != null) {
				records.add(LogRecord.createLogRecord(line));
			}
			Collections.sort(records, LogRecord.Comparators.START);
			for (int i = 0; i < records.size(); i++) {
				bWriter.write(records.get(i).toString() + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
		try {
			if (dataInStreams != null)
				dataInStreams.close();
			if (bReaders != null)
				bReaders.close();
			fstreams.close();
			bWriter.flush();
			bWriter.close();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	public static String getPropertiesString2(String[] pNames, String[] pValues, char[] pTypes) {
		StringBuilder sb = new StringBuilder();
		boolean firstOne = true;
		for (int i = 0; i < pNames.length; i++) {
			if (pTypes[i] != ValidationParams.NO_READ_UPDATE) {
				if (!firstOne) {
					sb.append(ValidationParams.PROPERY_SEPERATOR);
				} else {
					firstOne = false;
				}
				sb.append(pNames[i]);
				sb.append(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR);
				sb.append(pValues[i]);
				sb.append(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR);
				sb.append(pTypes[i]);
			}
		}
		return sb.toString();
	}

	public static String getLogString(char type, String actionName, int threadId, int sequenceId, long startTime, long endTime, ArrayList<String> customers, ArrayList<String> orders) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(threadId);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(sequenceId);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(startTime);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < customers.size(); i++) {
			sb.append(customers.get(i));
			sb.append(ValidationParams.ENTITY_SEPERATOR);
			sb.append(orders.get(i));
			if ((i + 1) != customers.size())
				sb.append(ValidationParams.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}
	
	public static void writeJSONFile(String text, String filePath, String appName) {
		// {"application":{ "name":"BG",
		// "entities":[{"name":"MEMBER","properties":["FRIEND_CNT","PENDING_CNT"]}]
		// }}
		PrintWriter printWriter = null;
		try {

			File file = new File(filePath);
			file.createNewFile();
			printWriter = new PrintWriter(filePath);
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		ArrayList<String> Elines = new ArrayList<String>();
		ArrayList<String> Tlines = new ArrayList<String>();
		String[] lines = text.split("[\n]");
		int stage = 1;
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].equals("#")) {
				stage = 2;
			} else if (stage == 1) {
				Elines.add(lines[i]);
			} else {
				Tlines.add(lines[i]);
			}
		}

		printWriter.write("{\"" + APPLICATION + "\":\n\t{\"" + APPLICATION_NAME + "\":\"" + appName + "\",\""
				+ APPLICATION_ENTITIES + "\":[\n");
		for (int i = 0; i < Elines.size(); i++) {
			if (Elines.get(i) == null || Elines.get(i).equals(""))
				continue;
			printWriter.write("\t\t{\"" + ENTITY_NAME + "\":\"");
			String[] Etokens = Elines.get(i).split("[,]");
			printWriter.write(Etokens[ENAME_INDEX] + "\",\"" + ENTITY_PROPERTIES + "\":[");
			for (int j = INDEX_OF_FIRST_PROPERTY_IN_E; j < Etokens.length; j++) {
				String[] Ptokens = Etokens[j].split(SPLIT_PROPERTY_INFO);
				printWriter.write("\"" + Ptokens[PNAME_INDEX] + "\"");
				if (j + 1 != Etokens.length) {
					printWriter.write(",");
				}
			}
			String insertTrans = getInsertTrans(Tlines, Etokens[E_EID_INDEX],
					Etokens.length - INDEX_OF_FIRST_PROPERTY_IN_E);
			printWriter.write("]"+insertTrans+"}");
			if (i + 1 != Elines.size()) {
				printWriter.write(",\n");
			} else {
				printWriter.write("\n");
			}
		}
		printWriter.write("\t]}\n}");

		printWriter.close();
	}
	private static String getInsertTrans(ArrayList<String> Tlines, String eID, int numOfProp) {
		boolean isInserted = false;
		boolean isPartiallyUpdated = false;
		HashSet<String> insertTransactions = new HashSet<String>();
		for (int i = 0; i < Tlines.size(); i++) {
			String[] Ttokens = Tlines.get(i).split(SPLIT_TRANSACTION_INFO);
			for (int j = INDEX_OF_FIRST_ENTITY; j < Ttokens.length; j++) {
				String[] Etokens = Ttokens[j].split(SPLIT_ENTITY_INFO);
				if (eID.equals(Etokens[TE_EID_INDEX])) {
					switch (Etokens[INDEX_OF_ENTITY_TYPE]) {
					case ENTITY_TYPE_INSERT:
						isInserted = true;
						insertTransactions.add(Ttokens[TNAME_INDEX]);
						break;
					case ENTITY_TYPE_UPDATE:
						int pNum = Etokens.length - INDEX_OF_FIRST_PROPERTY_IN_TE;
						if (numOfProp > pNum)
							isPartiallyUpdated = true;
						break;
					case ENTITY_TYPE_RW:
						break;
					}
				}
			}
			// 1,e1,1,READ,1;e1_p2;2;true;Integer;;v11;,2;e1_p4;4;true;Boolean;;v12;,4;e1_p1;1;false;String;R;v13;,
		}
		String result = "";
		if(isInserted && isPartiallyUpdated){
			result=",\""+ENTITY_INSERT_TRANSACTIONS+"\":[";
			String comma = "";
			for(String t : insertTransactions){
				result += comma+"\""+t+"\"";
				comma = ",";
			}
			result += "]";
		}
		return result;
	}

	public static int getEarilestRecordIndex(LogRecord[] records) {
		if (records == null || records.length == 0) {
			return -1;
		}
		LogRecord min = records[0];
		int index = 0;
		for (int i = 1; i < records.length; i++) {
			if (records[i] == null)
				continue;
			if (min == null || min.getStartTime() > records[i].getStartTime()) {
				index = i;
				min = records[i];
			}
		}
	//	records[index] = null;
		return index;
	}

	public static String getStaleLogString(char type, String id, long staleOffset, int rPartition, long lastReadOffset,
			int uPartition, long lastWriteOffset, String action, HashMap<Schedule, HashMap<String, String>> expectedValues) {
	
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(id);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(staleOffset);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(rPartition);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(lastReadOffset);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(uPartition);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(lastWriteOffset);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(action);
		sb.append(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR);
		String semi="";
		String pound="";
		for (Schedule s : expectedValues.keySet()) {
			sb.append(pound);
			pound="#";
			HashMap<String, String> values = expectedValues.get(s);
			for (String key : values.keySet()) {
				sb.append(semi);
				semi=";";
				sb.append(key + ":" + LogRecord.escapeCharacters(values.get(key)));			
			}
		}
		return sb.toString();
	}
}
