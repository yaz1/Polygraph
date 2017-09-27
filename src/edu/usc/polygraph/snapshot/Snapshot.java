package edu.usc.polygraph.snapshot;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import edu.usc.polygraph.Buffer;
import edu.usc.polygraph.DBState;
import edu.usc.polygraph.LogRecord;
import edu.usc.polygraph.ScheduleList;
import edu.usc.polygraph.ValidationParams;
import edu.usc.polygraph.Validator;
import edu.usc.polygraph.ValidatorData;
import edu.usc.polygraph.resourceUpdateStat;
import edu.usc.polygraph.utils.Common;

public class Snapshot {
	public static Semaphore semaphore = new Semaphore(1, true);
	
	public static int currentThread;

	private final static String notAllowedListFile = "notAllowedListFile.bin";
	private final static String readWriteFile = "readWrite.bin";
	private final static String bucketFile = "bucket.bin";
	private final static String collapsedIntervalsFile = "collapsedIntervals.bin";
	private final static String intervalTreesFile = "intervalTrees.bin";
	private final static String logsFile = "logsFile.bin";
	private final static String currentSSFile = "currentSSFile.bin";
	private final static String currentSSAllFile = "currentSSAllFile.bin";

	private final static String dbStateFile = "dbStateFile.bin";
	private final static String variablesFile = "variablesFile.bin";
	public final static String unorderedReadsFile = "unorderedReadsFile.bin";
	public final static String unorderedWritesFile = "unorderedWritesFile.bin";
	private final static String skipReadsFile = "skipReadsFile.bin";
	private final static String skipWritesFile = "skipWritesFile.bin";
	private final static String readBufferFile = "readBufferFile.bin";
	private final static String writeBufferFile = "writeBufferFile.bin";

	private final static String snapshotsInfoFile = "snapshotsInfoPath.bin";

	public final static String SANPSHOT_DIR = "./snapshots/";

	public final static int nullString = -1;
	public final static int nullObject = -2;

	private static final int No_MORE_SCHEDULES = -3;
	public static int SnapshotDelta = 500 * 60 * 1000;// 120

	public static void initSnapshotsInfo(String dir) {
		currentThread = 0;
		try {
			File a = new File(dir);
			if (a.exists()) {
				deleteFileOrFolder(a.toPath());
			}
			Common.CreateDir(dir);
			String path = dir + snapshotsInfoFile;
			DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
			out.writeInt(0);
			out.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	public static ArrayList<SnapshotInfo> getSnapshotsInfo(String dir) throws IOException {
		String path = dir + snapshotsInfoFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		int size = in.readInt();
		ArrayList<SnapshotInfo> array = new ArrayList<SnapshotInfo>();
		for (int i = 0; i < size; i++) {
			SnapshotInfo ssi = new SnapshotInfo();
			ssi.id = in.readInt();
			ssi.startTime = in.readLong();
			ssi.endTime = in.readLong();
			int pathSize = in.readInt();
			byte[] buffer = new byte[pathSize];
			in.read(buffer, 0, pathSize);
			ssi.path = new String(buffer);
			array.add(ssi);
		}
		in.close();
		return array;
	}

	private static void addNewSnapshotInfo(String dir, SnapshotInfo snapshotInfo) throws IOException {
		ArrayList<SnapshotInfo> array = getSnapshotsInfo(dir);
		array.add(snapshotInfo);
		writeSnapshotsInfoFile(dir, array);
	}

	private static void writeSnapshotsInfoFile(String dir, ArrayList<SnapshotInfo> array) throws IOException {
		String path = dir + snapshotsInfoFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(array.size());
		for (SnapshotInfo ssi : array) {
			out.writeInt(ssi.id);
			out.writeLong(ssi.startTime);
			out.writeLong(ssi.endTime);
			byte[] buffer = ssi.path.getBytes();
			out.writeInt(buffer.length);
			out.write(buffer);
		}
		out.close();
	}

	public static void createSnapshot(int snapshotID, String ssiPath, String path, ValidatorData data, long startTime, long endTime) throws IOException {
		path = Common.fixDir(path);
		Common.CreateDir(path);
		DataOutputStream outLog = serializeLogs(path);
		HashSet<String> logsHS = new HashSet<String>();
		serializeDB(path, data);
		serializeSS(path, data, outLog, logsHS);
		serializeBucket(path, data, outLog, logsHS);
		serializeReadWrite(path, data, outLog, logsHS);
		serializeCollapsedIntervals(path, data, outLog, logsHS);
		serializeWrites(path, data, outLog, logsHS);
		serializeNotAllowedList(path, data);
		serializeBuffer(path, readBufferFile, data.bufferedReads);
		serializeBuffer(path, writeBufferFile, data.bufferedWrites);
//		serializeStringSet(path, "processedLogs.bin", data.processesLogs); // TODO: to be removed
		serializeVariable(path, data);
		serializeArrayListLong(path, unorderedReadsFile, data.bufferedReads.skipList);
		serializeArrayListLong(path, unorderedWritesFile, data.bufferedWrites.skipList);
		// serializeArrayListLong(path, skipReadsFile, new ArrayList<Long>());
		// serializeArrayListLong(path, skipWritesFile, new ArrayList<Long>());

		outLog.close();
		addNewSnapshotInfo(ssiPath, new SnapshotInfo(snapshotID, startTime, endTime, path));


	}

	public static SnapshotResult restoreSnapshot(String path, int ssi, String pathss, ValidatorData oldData, int validatorID, int index, Validator v, boolean b) throws IOException {
		Date st= new Date();
		System.out.println(validatorID+": start restoring partition:"+index+" at:"+ValidationParams.DATE_FORMAT.format(st));
		pathss = Common.fixDir(pathss);
		ValidatorData data = new ValidatorData();
		HashMap<String, LogRecord> logs = deserializeLogs(pathss);
		data.dbState = deserializeDB(pathss);
		data.currentSS = deserializeSS(pathss, data.dbState, logs);
		data.bucket = deserializeBucket(pathss, logs);
		data.readWrite = deserializeReadWrite(pathss, logs);
		data.collapsedIntervals = deserializeCollapsedIntervals(pathss, logs);
		data.intervalTrees = deserializeWrites(pathss, logs);
		data.notAllowedList = deserializeNotAllowedList(pathss);
		data.bufferedReads = deserializeBuffer(pathss, readBufferFile, oldData.bufferedReads);
		data.bufferedWrites = deserializeBuffer(pathss, writeBufferFile, oldData.bufferedWrites);
//		data.processesLogs = deserializeStringSet(pathss, "processedLogs.bin"); // TODO: to be removed
		deserializeVariable(pathss, data);
		data.unorderedReadsCount = oldData.unorderedReadsCount;
		data.unorderedWritesCount = oldData.unorderedWritesCount;
		SnapshotResult result = new SnapshotResult();

		data.consumerRead = oldData.consumerRead;// NoSS
		data.consumerUpdate = oldData.consumerUpdate;// NoSS
		data.readTopicPartitions = oldData.readTopicPartitions;// NoSS
		data.updateTopicPartitions = oldData.updateTopicPartitions;// NoSS
		data.myPartition = oldData.myPartition;

		result.unorderedReads = deserializeArrayListLong(pathss, unorderedReadsFile);
		result.unorderedWrites = deserializeArrayListLong(pathss, unorderedWritesFile);
		// result.skippedReads = deserializeArrayListLong(path, skipReadsFile);
		// result.skippedWrites = deserializeArrayListLong(path,
		// skipWritesFile);

		getLogFromKafka(data.bufferedReads, data.consumerRead, data.readTopicPartitions, result.unorderedReads, index);
		getLogFromKafka(data.bufferedWrites, data.consumerUpdate, data.updateTopicPartitions, result.unorderedWrites, index);

		data.bufferedReads.skipList.addAll(result.unorderedReads);
		data.bufferedWrites.skipList.addAll(result.unorderedWrites);

		data.consumerRead.seek(data.readTopicPartitions, data.bufferedReads.getOffset());
		Validator.fillBuffer(validatorID, data.bufferedReads, index, data.consumerRead, 'R', null,data);
		data.consumerRead.seek(data.readTopicPartitions, data.bufferedReads.getOffset() + 1);

		data.consumerUpdate.seek(data.updateTopicPartitions, data.bufferedWrites.getOffset());
		Validator.fillBuffer(validatorID, data.bufferedWrites, index, data.consumerUpdate, 'U', null,data);
		data.consumerUpdate.seek(data.updateTopicPartitions, data.bufferedWrites.getOffset() + 1);

		// Assuming 1 thread per partition
		// Assuming partition thread is doing the restore
		v.totalReadLogsCount += data.readLogsCount - oldData.readLogsCount;
		v.totalStaleCount += data.staleCount - oldData.staleCount;

		result.data = data;

		// System.out.println(result.unorderedReads);

		if (b) {
			deleteFollowingSnapshot(path, ssi);
		}
		long secs=(System.currentTimeMillis()-st.getTime())/1000;
		String duration=String.format("%02d:%02d:%02d", secs / 3600, (secs % 3600) / 60, (secs % 60));
		
		System.out.println(validatorID+": Done restoring partition:"+index+". Duration:"+duration);

		return result;
	}

	private static void deleteFollowingSnapshot(String path, int ssi) throws IOException {
		ArrayList<SnapshotInfo> al = getSnapshotsInfo(path);
		for (int i = 0; i < al.size(); i++) {
			if (ssi < al.get(i).id) {
				al.remove(i);
				i--;
			}
			// Common.deleteFileOrFolder(path); Not sure if works properly
		}
		writeSnapshotsInfoFile(path, al);
	}

	private static void getLogFromKafka(Buffer buffer, KafkaConsumer<String, String> consumer, TopicPartition partitions, HashSet<Long> unordered, int index) {
		// Collections.sort(unordered);
		for (long offset : unordered) {
			consumer.seek(partitions, offset);
			LogRecord log = getLogFromKafka(consumer, index);
			assert log != null && log.getOffset() == offset : "Incorrect offset.";
			if (log != null) {
				buffer.add(log, 0);
			}
		}
	}

	private static LogRecord getLogFromKafka(KafkaConsumer<String, String> consumer, int index) {
		boolean done = false;
		int i = 1;
		while (!done && i <= 10) {
			ConsumerRecords<String, String> records = consumer.poll(1000 * i);
			if (!records.isEmpty()) {
				Iterator<ConsumerRecord<String, String>> it = records.iterator();
				ConsumerRecord<String, String> rec = it.next();
				LogRecord result = LogRecord.createLogRecord(rec.value());
				Validator.adjustSkew(result);
				result.setOffset(rec.offset());
				result.setPartitionID(index);
				return result;
			}
			i++;
		}
		return null;
	}

	public static void updateSnapshot(String path, HashSet<Long> ur, HashSet<Long> uw, ArrayList<Long> sr, ArrayList<Long> sw) throws IOException {
		serializeArrayListLong(path, unorderedReadsFile, ur);
		serializeArrayListLong(path, unorderedWritesFile, uw);
		// serializeArrayListLong(path, skipReadsFile, sr);
		// serializeArrayListLong(path, skipWritesFile, sw);
	}

	public static void updateUnordered(String dir, String file, long offset) throws IOException {
		HashSet<Long> al = deserializeArrayListLong(dir, file);
		al.add(offset);
		serializeArrayListLong(dir, file, al);
	}
	
	public static void updateUnordered(String dir, String file,ArrayList<Long> offsets) throws IOException {
		HashSet<Long> al = deserializeArrayListLong(dir, file);
		for(long offset:offsets)
		al.add(offset);
		serializeArrayListLong(dir, file, al);
	}

	private static void serializeBuffer(String dir, String file, Buffer buffer) throws IOException {
		String path = dir + file;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		buffer.serialize(out);
		out.close();
	}

	private static Buffer deserializeBuffer(String dir, String file, Buffer oldBuffer) throws IOException {
		String path = dir + file;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		Buffer buffer = Buffer.deserialize(in, oldBuffer.maxSize(), oldBuffer.getBufferThreshold());
		in.close();
		return buffer;
	}

	private static void serializeWrites(String dir, ValidatorData data, DataOutputStream outLog, HashSet<String> logsHS) throws IOException {
		String path = dir + intervalTreesFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(data.intervalTrees.size());
		data.intervalTrees.serializeWrites(out, outLog, logsHS);
		out.close();
	}

	private static resourceUpdateStat deserializeWrites(String dir, HashMap<String, LogRecord> logs) throws IOException {
		String path = dir + intervalTreesFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		resourceUpdateStat tree = new resourceUpdateStat();
		tree.deserializeWrites(in, logs);
		in.close();
		return tree;

	}

	private static void serializeNotAllowedList(String dir, ValidatorData data) throws IOException {
		String path = dir + notAllowedListFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(data.notAllowedList.size());
		for (String key : data.notAllowedList.keySet()) {
			byte[] buffer = key.getBytes();
			out.writeInt(buffer.length);
			out.write(buffer);
			HashMap<String, Boolean> hm = data.notAllowedList.get(key);
			out.writeInt(hm.size());
			for (String key2 : hm.keySet()) {
				buffer = key2.getBytes();
				out.writeInt(buffer.length);
				out.write(buffer);
				out.writeBoolean(hm.get(key2));
			}
		}
		out.close();
	}

	private static HashMap<String, HashMap<String, Boolean>> deserializeNotAllowedList(String dir) throws IOException {
		String path = dir + notAllowedListFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		HashMap<String, HashMap<String, Boolean>> mainHS = new HashMap<String, HashMap<String, Boolean>>();
		int mainHSSize = in.readInt();
		for (int i = 0; i < mainHSSize; i++) {
			int key1Size = in.readInt();
			byte[] buffer = new byte[key1Size];
			in.read(buffer, 0, key1Size);
			String key1 = new String(buffer);
			int hsSize = in.readInt();
			HashMap<String, Boolean> hs = new HashMap<String, Boolean>();
			for (int j = 0; j < hsSize; j++) {
				int key2Size = in.readInt();
				buffer = new byte[key2Size];
				in.read(buffer, 0, key2Size);
				String key2 = new String(buffer);
				hs.put(key2, in.readBoolean());
			}
			mainHS.put(key1, hs);
		}
		in.close();
		return mainHS;
	}

	private static void serializeCollapsedIntervals(String dir, ValidatorData data, DataOutputStream outLog, HashSet<String> logsHS) throws IOException {
		String path = dir + collapsedIntervalsFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(data.collapsedIntervals.size());
		for (LogRecord r : data.collapsedIntervals) {
			r.serializeID(out);
			if (!logsHS.contains(r.getId())) {
				logsHS.add(r.getId());
				r.serialize(outLog);
			}
		}
		out.close();
	}

	private static Set<LogRecord> deserializeCollapsedIntervals(String dir, HashMap<String, LogRecord> logs) throws IOException {
		String path = dir + collapsedIntervalsFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		HashSet<LogRecord> hs = new HashSet<LogRecord>();
		int size = in.readInt();
		for (int j = 0; j < size; j++) {
			int idSize = in.readInt();
			byte[] buffer = new byte[idSize];
			in.read(buffer, 0, idSize);
			String logId = new String(buffer);
			hs.add(logs.get(logId));
		}
		in.close();
		return hs;
	}

	private static void serializeReadWrite(String dir, ValidatorData data, DataOutputStream outLog, HashSet<String> logsHS) throws IOException {
		String path = dir + readWriteFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(data.readWrite.size());
		for (LogRecord r : data.readWrite) {
			r.serializeID(out);
			if (!logsHS.contains(r.getId())) {
				logsHS.add(r.getId());
				r.serialize(outLog);
			}
		}
		out.close();
	}

	private static ArrayList<LogRecord> deserializeReadWrite(String dir, HashMap<String, LogRecord> logs) throws IOException {
		String path = dir + readWriteFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		ArrayList<LogRecord> al = new ArrayList<LogRecord>();
		int size = in.readInt();
		for (int j = 0; j < size; j++) {
			int idSize = in.readInt();
			byte[] buffer = new byte[idSize];
			in.read(buffer, 0, idSize);
			String logId = new String(buffer);
			al.add(logs.get(logId));
		}
		in.close();
		return al;
	}

	private static void serializeBucket(String dir, ValidatorData data, DataOutputStream outLog, HashSet<String> logs) throws IOException {
		String path = dir + bucketFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
	
		for (String ename: ValidationParams.ENTITY_NAMES){
			out.writeInt(data.bucket.get(ename).size());
		for (String key : data.bucket.get(ename).keySet()) {
			byte[] buffer = key.getBytes();
			out.writeInt(buffer.length);
			out.write(buffer);
			ArrayList<LogRecord> al = data.bucket.get(ename).get(key);
			out.writeInt(al.size());
			for (LogRecord r : al) {
				r.serializeID(out);
				if (!logs.contains(r.getId())) {
					logs.add(r.getId());
					r.serialize(outLog);
				}
			}
		}
	}
		
		out.close();
	}

	private static HashMap<String, HashMap<String, ArrayList<LogRecord>>> deserializeBucket(String dir, HashMap<String, LogRecord> logs) throws IOException {
		HashMap<String,HashMap<String, ArrayList<LogRecord>>> bucket = new HashMap<String,HashMap<String, ArrayList<LogRecord>>>();
		
		String path = dir + bucketFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		for (String ename: ValidationParams.ENTITY_NAMES){
			HashMap<String, ArrayList<LogRecord>> map= new HashMap<String, ArrayList<LogRecord>>();
			bucket.put(ename, map);
		int bucketSize = in.readInt();
		for (int i = 0; i < bucketSize; i++) {
			int keySize = in.readInt();
			byte[] buffer = new byte[keySize];
			in.read(buffer, 0, keySize);
			String key = new String(buffer);
			ArrayList<LogRecord> al = new ArrayList<LogRecord>();
			int size = in.readInt();
			for (int j = 0; j < size; j++) {
				int idSize = in.readInt();
				buffer = new byte[idSize];
				in.read(buffer, 0, idSize);
				String logId = new String(buffer);
				al.add(logs.get(logId));
			}
			bucket.get(ename).put(key, al);
		}
	}
		in.close();
		return bucket;
	}

	private static DataOutputStream serializeLogs(String dir) throws IOException {
		String path = dir + logsFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		return out;
	}

	private static HashMap<String, LogRecord> deserializeLogs(String dir) throws IOException {
		String path = dir + logsFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		HashMap<String, LogRecord> al = new HashMap<String, LogRecord>();
		while (in.available() > 0) { // for (int i = 0; i < size; i++) {
			LogRecord r = LogRecord.deserialize(in);
			al.put(r.getId(), r);
		}
		in.close();
		return al;
	}

	private static HashMap<String, ScheduleList> deserializeSS(String dir, HashMap<String, LinkedList<DBState>> db, HashMap<String, LogRecord> logs) throws IOException {
		String path = dir + currentSSFile;
		String pathAll = dir + currentSSAllFile;

		HashMap<String, ScheduleList> currentSS = new HashMap<String, ScheduleList>();

		try {
			DataInputStream is = new DataInputStream(new FileInputStream(path));
			DataInputStream isAll = new DataInputStream(new FileInputStream(pathAll));
			int count = 0;
			ArrayList<ScheduleList> uniqueScheds = new ArrayList<ScheduleList>();
			while (true) {
				count = is.readInt();
				if (count == No_MORE_SCHEDULES)
					break;
				uniqueScheds.add(ScheduleList.deserialize(is, db, logs));

			}

			int numKeys = isAll.readInt();
			for (int i = 0; i < numKeys; i++) {
				int keysize = isAll.readInt();
				byte[] b = new byte[keysize];
				isAll.read(b, 0, keysize);
				String key = new String(b);
				int index = isAll.readInt();

				currentSS.put(key, uniqueScheds.get(index));

			}
			is.close();
			isAll.close();

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
		return currentSS;

	}

	private static void serializeSS(String dir, ValidatorData data, DataOutputStream osLog, HashSet<String> logs) throws IOException {
		String path = dir + currentSSFile;
		String pathAll = dir + currentSSAllFile;

		HashMap<ScheduleList, Integer> map = new HashMap<ScheduleList, Integer>();
		int count = -1;
		DataOutputStream os = new DataOutputStream(new FileOutputStream(path));
		DataOutputStream osAll = new DataOutputStream(new FileOutputStream(pathAll));

		int numKeys = data.currentSS.size();
		osAll.writeInt(numKeys);
		for (String key : data.currentSS.keySet()) {
			ScheduleList ss = data.currentSS.get(key);
			if (!map.containsKey(ss)) {
				map.put(ss, ++count);
				os.writeInt(count);
				// byte[] b = key.getBytes();
				// os.writeInt(b.length);
				// os.write(b);

				ss.serialize(os, data.dbState, logs, osLog);
			}
			byte[] b = key.getBytes();
			osAll.writeInt(b.length);
			osAll.write(b);
			osAll.writeInt(map.get(ss));
		}
		osAll.close();
		os.writeInt(No_MORE_SCHEDULES);
		os.close();

	}

	private static void serializeDB(String dir, ValidatorData data) throws IOException {
		String path = dir + dbStateFile;
		DataOutputStream os = new DataOutputStream(new FileOutputStream(path));
		int numKeys = data.dbState.size();
		os.writeInt(numKeys);
		for (String key : data.dbState.keySet()) {
			byte[] b = key.getBytes();
			os.writeInt(b.length);
			os.write(b);
			LinkedList<DBState> ll = data.dbState.get(key);
			os.writeInt(ll.size());
			for (DBState state : ll) {
				os.writeInt(state.getRefrenceCount());
				os.writeInt(state.size());
				for (String v : state.getValue()) {
					if (v == null) {
						os.writeInt(nullString);
					} else {
						byte[] bb = v.getBytes();
						os.writeInt(bb.length);
						os.write(bb);
					}
				}
			}
		}
		os.close();
	}

	private static HashMap<String, LinkedList<DBState>> deserializeDB(String dir) throws IOException {
		HashMap<String, LinkedList<DBState>> dbState = new HashMap<String, LinkedList<DBState>>();
		String path = dir + dbStateFile;
		DataInputStream is = new DataInputStream(new FileInputStream(path));
		int numKeys = is.readInt();

		for (int i = 0; i < numKeys; i++) {
			int keylength = is.readInt();
			byte[] b = new byte[keylength];
			is.read(b, 0, keylength);
			String key = new String(b);
			LinkedList<DBState> ll = dbState.get(key);
			assert ll == null : "ll not null!";
			ll = new LinkedList<DBState>();

			int llsize = is.readInt();
			for (int l = 0; l < llsize; l++) {

				int refCount = is.readInt();
				int stateSize = is.readInt();
				String values[] = new String[stateSize];
				for (int v = 0; v < values.length; v++) {
					int vlen = is.readInt();
					if (vlen == nullString) {
						values[v] = null;
					} else {
						byte[] vb = new byte[vlen];
						is.read(vb, 0, vlen);
						values[v] = new String(vb);
					}
				}
				DBState state = new DBState(refCount, values);
				ll.add(state);

			}
			dbState.put(key, ll);

		}
		is.close();

		return dbState;
	}

	private static void serializeArrayListLong(String dir, String file, HashSet<Long> arrayList) throws IOException {
		String path = dir + file;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(arrayList.size());
		for (long offset : arrayList) {
			out.writeLong(offset);
		}
		out.close();
	}

	private static HashSet<Long> deserializeArrayListLong(String dir, String file) throws IOException {
		String path = dir + file;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		HashSet<Long> array = new HashSet<Long>();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			array.add(in.readLong());
		}
		in.close();
		return array;
	}

	private static HashSet<String> deserializeStringSet(String dir, String file) throws IOException {
		String path = dir + file;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		HashSet<String> array = new HashSet<String>();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			int strSize = in.readInt();
			byte[] buffer = new byte[strSize];
			in.read(buffer, 0, strSize);
			String a = new String(buffer);
			array.add(a);
		}
		in.close();
		return array;
	}

	private static void serializeStringSet(String dir, String file, HashSet<String> arrayList) throws IOException {
		String path = dir + file;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(arrayList.size());
		for (String str : arrayList) {
			byte[] bStr = str.getBytes();
			out.writeInt(bStr.length);
			out.write(bStr);
		}
		out.close();
	}

	private static void serializeVariable(String dir, ValidatorData data) throws IOException {
		String path = dir + variablesFile;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeLong(data.readLogsCount);
		out.writeLong(data.writeLogsCount);
		out.writeLong(data.staleCount);
		out.writeLong(data.readStartTime);
		out.writeLong(data.readEndTime);
		out.writeInt(data.partiallyDiscardedWritesCount);
		out.writeInt(data.fullyDiscardedWritesCount);
		if (data.currentRead != null) {
			data.currentRead.serialize(out);
		} else {
			out.writeInt(nullObject);
		}
		// out.writeLong(data.unorderedReadsCount);
		// out.writeLong(data.unorderedWritesCount);
		out.close();
	}

	private static void deserializeVariable(String dir, ValidatorData data) throws IOException {
		String path = dir + variablesFile;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		data.readLogsCount = in.readLong();
		data.writeLogsCount = in.readLong();
		data.staleCount = in.readLong();
		data.readStartTime = in.readLong();
		data.readEndTime = in.readLong();
		data.partiallyDiscardedWritesCount = in.readInt();
		data.fullyDiscardedWritesCount = in.readInt();
		data.currentRead = LogRecord.deserialize(in);
		// data.unorderedReadsCount = in.readLong();
		// data.unorderedWritesCount = in.readLong();
		in.close();
	}

	public static void deleteFileOrFolder(final Path path) throws IOException {
		Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(final Path file, final IOException e) {
				return handleException(e);
			}

			private FileVisitResult handleException(final IOException e) {
				e.printStackTrace(); // replace with more robust error handling
				return FileVisitResult.TERMINATE;
			}

			@Override
			public FileVisitResult postVisitDirectory(final Path dir, final IOException e) throws IOException {
				if (e != null)
					return handleException(e);
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	};

}