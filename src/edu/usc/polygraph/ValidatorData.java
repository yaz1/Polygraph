package edu.usc.polygraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ValidatorData {

	public HashMap<String, LinkedList<DBState>> dbState;//SS
	public HashMap<String, ScheduleList> currentSS;//SS
	public ArrayList<LogRecord> readWrite;//SS
	public resourceUpdateStat intervalTrees ;//SS
	public HashMap<String,HashMap<String, ArrayList<LogRecord>>> bucket;//SS
	public Buffer bufferedReads = null;//SS
	public Buffer bufferedWrites = null;//SS
	public HashMap<String, HashMap<String, Boolean>> notAllowedList;//SS
	public Set<LogRecord> collapsedIntervals;//SS
	
	public KafkaConsumer<String, String> consumerRead = null;//NoSS
	public KafkaConsumer<String, String> consumerUpdate = null;//NoSS
	public TopicPartition readTopicPartitions;//NoSS
	public TopicPartition updateTopicPartitions;//NoSS
	public LogRecord currentRead=null;//NoSS
	
	public long readLogsCount = 0;//SS
	public long writeLogsCount = 0;//SS
	public long readStartTime = 0;//SS
	public long readEndTime = 0;//SS
	public long staleCount = 0;//SS
	public int partiallyDiscardedWritesCount = 0;//SS
	public int fullyDiscardedWritesCount = 0;//SS
	public long unorderedReadsCount=0;//SS
	public long unorderedWritesCount=0;//SS
	public int myPartition = -1;//NoSS	
	public HashMap<Long, Bucket> freshnessBuckets;
	
//	public HashSet<String> processesLogs; //TODO: to be removed
	
	public ValidatorData(Integer partition){
		if (ValidationParams.COMPUTE_FRESHNESS)
		freshnessBuckets= new HashMap<Long,Bucket>();
		dbState = new HashMap<String, LinkedList<DBState>>();
		currentSS = new HashMap<String, ScheduleList>();
		bucket = new HashMap<String,HashMap<String, ArrayList<LogRecord>>>();
		for (String e: ValidationParams.ENTITY_NAMES){
			HashMap<String, ArrayList<LogRecord>> map= new HashMap<String, ArrayList<LogRecord>>();
			bucket.put(e, map);
		}
		notAllowedList = new HashMap<String, HashMap<String, Boolean>>();
		collapsedIntervals = new HashSet<LogRecord>();
		readWrite = new ArrayList<LogRecord>();
		intervalTrees = new resourceUpdateStat();
		myPartition=partition;
		
//		processesLogs = new HashSet<String>(); //TODO: to be removed
	}
	
	public ValidatorData(){
		
	}
}
