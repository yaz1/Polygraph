package snapshot;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.usc.polygraph.Entity;
import edu.usc.polygraph.LogRecord;
import edu.usc.polygraph.Property;
import edu.usc.polygraph.resourceUpdateStat;
import edu.usc.polygraph.utils.Common;

public class bucketTest {
	public static HashMap<String, ArrayList<LogRecord>> bucket = new HashMap<String, ArrayList<LogRecord>>();
	public static ArrayList<LogRecord> readWrite = new ArrayList<LogRecord>();
	public static Set<LogRecord> collapsedIntervals = new HashSet<LogRecord>();
	public static HashMap<Integer, resourceUpdateStat> intervalTrees = new HashMap<Integer, resourceUpdateStat>();
	public static HashMap<String, HashMap<String, Boolean>> notAllowedList = new HashMap<String, HashMap<String, Boolean>>();

	public static String notAllowedListFile = "NotAllowedListFile.bin", readWriteFile = "ReadWrite.bin", bucketFile = "Bucket.bin", collapsedIntervalsFile = "CollapsedIntervals.bin", intervalTreesFile = "IntervalTrees.bin", logsFile = "LogsFile.bin";

	public static void main(String[] args) {
		String[] lines = { "Z,Payment,273-0,8027748459549077,8027766262621683,Customer;92-3-1731;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:3991.2:N#c_balance:-3991.2:N#c_ytd_payment:10.0:R#c_balance:-10.0:R", "Z,Payment,240-0,8027748435437233,8027766262621635,Customer;81-1-1184;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:3319.77:N#c_balance:-3319.77:N#c_ytd_payment:10.0:R#c_balance:-10.0:R",
				"Z,Payment,232-0,8027748438111304,8027766404368783,Customer;92-3-1731;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:1824.37:N#c_balance:-1824.37:N#c_ytd_payment:10.0:R#c_balance:-10.0:R", "Z,Payment,78-0,8027748476603132,8027766404372545,Customer;27-1-2594;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:1488.87:N#c_balance:-1488.87:N#c_ytd_payment:10.0:R#c_balance:-10.0:R",
				"Z,Payment,147-0,8027748467938422,8027766971499003,Customer;92-3-1731;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:4319.96:N#c_balance:-4319.96:N#c_ytd_payment:10.0:R#c_balance:-10.0:R", "Z,Payment,215-0,8027748444780714,8027766971508006,Customer;72-10-2115;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:2740.36:N#c_balance:-2740.36:N#c_ytd_payment:10.0:R#c_balance:-10.0:R",
				"Z,Payment,227-0,8027748440653963,8027767188208994,Customer;92-3-2338;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:741.98:N#c_balance:-741.98:N#c_ytd_payment:10.0:R#c_balance:-10.0:R", "Z,Payment,137-0,8027748469132009,8027767188211499,Customer;46-9-695;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:1149.3:N#c_balance:-1149.3:N#c_ytd_payment:10.0:R#c_balance:-10.0:R",
				"Z,Payment,99-0,8027748473856664,8027767188221244,Customer;92-3-2338;c_payment_cnt:2:N#c_payment_cnt:1:R#c_ytd_payment:882.91:N#c_balance:-882.91:N#c_ytd_payment:10.0:R#c_balance:-10.0:R" };

		int partition = 0;
		fillNotAllowedList();
		resourceUpdateStat rUS = new resourceUpdateStat();
		for (String line : lines) {
			LogRecord r = LogRecord.createLogRecord(line);
			addToBucket(r);
			readWrite.add(r);
			collapsedIntervals.add(r);
			rUS.addIntervalSorted(r);
		}
		intervalTrees.put(partition, rUS);
		try {
			DataOutputStream outLog = serializeLogs();
			HashSet<String> logsHS = new HashSet<String>();
			serializeBucket(outLog, logsHS);
			serializeReadWrite(outLog, logsHS);
			serializeCollapsedIntervals(outLog, logsHS);
			serializeWrites(partition, outLog, logsHS);
			serializeNotAllowedList();
			outLog.close();

			print(bucket);
			print(readWrite);
			print(collapsedIntervals);
			print(intervalTrees, partition);
			print2(notAllowedList);
			
			System.out.println("**************");

			HashMap<String, LogRecord> logs = deserializeLogs();
			bucket = deserializeBucket(logs);
			readWrite = deserializeReadWrite(logs);
			collapsedIntervals = deserializeCollapsedIntervals(logs);
			intervalTrees.put(partition, deserializeWrites(partition, logs));
			notAllowedList = deserializeNotAllowedList();

			print(bucket);
			print(readWrite);
			print(collapsedIntervals);
			print(intervalTrees, partition);
			print2(notAllowedList);

		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	private static void serializeWrites(int partition, DataOutputStream outLog, HashSet<String> logsHS) throws IOException {
		String fileName = intervalTreesFile;
		String path = "./snapshots/" + fileName;
		Common.CreateDir("./snapshots/");
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		resourceUpdateStat tree = intervalTrees.get(partition);
		out.writeInt(tree.size());
		tree.serializeWrites(out, outLog, logsHS);
		out.close();
	}

	private static resourceUpdateStat deserializeWrites(int partition, HashMap<String, LogRecord> logs) throws IOException {
		String fileName = intervalTreesFile;
		String path = "./snapshots/" + fileName;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		resourceUpdateStat tree = new resourceUpdateStat();
		tree.deserializeWrites(in, logs);
		in.close();
		return tree;

	}

	private static void serializeNotAllowedList() throws IOException {
		String fileName = notAllowedListFile;
		String path = "./snapshots/" + fileName;
		Common.CreateDir("./snapshots/");
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(notAllowedList.size());
		for (String key : notAllowedList.keySet()) {
			byte[] buffer = key.getBytes();
			out.writeInt(buffer.length);
			out.write(buffer);
			HashMap<String, Boolean> hm = notAllowedList.get(key);
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

	private static HashMap<String, HashMap<String, Boolean>> deserializeNotAllowedList() throws IOException {
		String fileName = notAllowedListFile;
		String path = "./snapshots/" + fileName;
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

	private static void serializeCollapsedIntervals(DataOutputStream outLog, HashSet<String> logsHS) throws IOException {
		String fileName = collapsedIntervalsFile;
		String path = "./snapshots/" + fileName;
		Common.CreateDir("./snapshots/");
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(collapsedIntervals.size());
		for (LogRecord r : collapsedIntervals) {
			r.serializeID(out);
			if (!logsHS.contains(r.getId())) {
				logsHS.add(r.getId());
				r.serialize(outLog);
			}
		}
		out.close();
	}

	private static Set<LogRecord> deserializeCollapsedIntervals(HashMap<String, LogRecord> logs) throws IOException {
		String fileName = collapsedIntervalsFile;
		String path = "./snapshots/" + fileName;
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

	private static void serializeReadWrite(DataOutputStream outLog, HashSet<String> logsHS) throws IOException {
		String fileName = readWriteFile;
		String path = "./snapshots/" + fileName;
		Common.CreateDir("./snapshots/");
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(readWrite.size());
		for (LogRecord r : readWrite) {
			r.serializeID(out);
			if (!logsHS.contains(r.getId())) {
				logsHS.add(r.getId());
				r.serialize(outLog);
			}
		}
		out.close();
	}

	private static ArrayList<LogRecord> deserializeReadWrite(HashMap<String, LogRecord> logs) throws IOException {
		String fileName = readWriteFile;
		String path = "./snapshots/" + fileName;
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

	private static void serializeBucket(DataOutputStream outLog, HashSet<String> logs) throws IOException {
		String fileName = bucketFile;
		String path = "./snapshots/" + fileName;
		Common.CreateDir("./snapshots/");
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		out.writeInt(bucket.size());
		for (String key : bucket.keySet()) {
			byte[] buffer = key.getBytes();
			out.writeInt(buffer.length);
			out.write(buffer);
			ArrayList<LogRecord> al = bucket.get(key);
			out.writeInt(al.size());
			for (LogRecord r : al) {
				r.serializeID(out);
				if (!logs.contains(r.getId())) {
					logs.add(r.getId());
					r.serialize(outLog);
				}
			}
		}
		out.close();
	}

	private static HashMap<String, ArrayList<LogRecord>> deserializeBucket(HashMap<String, LogRecord> logs) throws IOException {
		HashMap<String, ArrayList<LogRecord>> bucket = new HashMap<String, ArrayList<LogRecord>>();
		String fileName = bucketFile;
		String path = "./snapshots/" + fileName;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
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
			bucket.put(key, al);
		}
		in.close();
		return bucket;
	}

	public static DataOutputStream serializeLogs() throws IOException {
		String fileName = logsFile;
		String path = "./snapshots/" + fileName;
		Common.CreateDir("./snapshots/");
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		// out.writeInt(al.size());
		// for(LogRecord r : al){
		// r.serialize(out);
		// }
		return out;
	}

	public static HashMap<String, LogRecord> deserializeLogs() throws IOException {
		String fileName = logsFile;
		String path = "./snapshots/" + fileName;
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		HashMap<String, LogRecord> al = new HashMap<String, LogRecord>();
		while (in.available() > 0) { // for (int i = 0; i < size; i++) {
			LogRecord r = LogRecord.deserialize(in);
			al.put(r.getId(), r);
		}
		in.close();
		return al;
	}

	private static void addToBucket(LogRecord i) {
		for (Entity e : i.getEntities()) {
			for (Property p : e.getProperties()) {
				String pKey = Property.getProprtyKey(e, p);
				if (!bucket.containsKey(pKey)) {
					ArrayList<LogRecord> temp = new ArrayList<LogRecord>();
					temp.add(i);
					bucket.put(pKey, temp);
				} else {
					ArrayList<LogRecord> b = bucket.get(pKey);
					boolean found = false;
					for (LogRecord r : b) {
						if (i.getId().equals(r.getId())) {
							found = true;
							break;
						}
					}
					if (!found) {
						bucket.get(pKey).add(i);
					}
				}
			}
		}
	}

	private static void fillNotAllowedList() {
		for (int i = 0; i < 10; i++) {
			HashMap<String, Boolean> bC = new HashMap<String, Boolean>();
			HashMap<String, Boolean> aC = new HashMap<String, Boolean>();
			notAllowedList.put("Before-Log-0" + i, bC);
			notAllowedList.put("After-Log-0" + i, aC);
			for (int j = 0; j < 10; j++) {
				if (i > j) {
					bC.put("Log-0" + j, (j % 2 == 0 ? true : false));
				} else if (i < j) {
					aC.put("Log-0" + j, (j % 2 != 0 ? true : false));
				}
			}
		}
	}

	private static void print(HashMap<String, ArrayList<LogRecord>> bucket1) {
		System.out.println("================================================");
		System.out.println("Bucket:-");
		for (String key : bucket1.keySet()) {
			System.out.println("Key: " + key + ",,, " + bucket1.get(key));
		}
		System.out.println("================================================");
	}

	private static void print(ArrayList<LogRecord> readWrite2) {
		System.out.println("================================================");
		System.out.println("readWrite:-");
		for (LogRecord r : readWrite2) {
			System.out.println(r.toPrint());
		}
		System.out.println("================================================");
	}

	public static void print2(HashMap<String, HashMap<String, Boolean>> notAllowedList2) {
		System.out.println("================================================");
		System.out.println("collapsedIntervals:-");
		for (String key1 : notAllowedList2.keySet()) {
			HashMap<String, Boolean> hs = notAllowedList2.get(key1);
			System.out.println("------------------------------------------");
			System.out.println("Key: " + key1);
			for (String key2 : hs.keySet()) {
				System.out.println("\t\tKey: " + key2 + " (" + hs.get(key2) + ")");
			}
			System.out.println("------------------------------------------");
		}
		System.out.println("================================================");
	}

	private static void print(HashMap<Integer, resourceUpdateStat> intervalTrees2, int partition) {
		System.out.println("================================================");
		System.out.println("intervalTrees:-");
		resourceUpdateStat tree = intervalTrees2.get(partition);
		tree.print();
		System.out.println("================================================");
	}

	private static void print(Set<LogRecord> collapsedIntervals2) {
		System.out.println("================================================");
		System.out.println("collapsedIntervals:-");
		for (LogRecord r : collapsedIntervals2) {
			System.out.println(r.toPrint());
		}
		System.out.println("================================================");
	}

}
