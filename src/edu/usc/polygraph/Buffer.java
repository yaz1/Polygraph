package edu.usc.polygraph;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

import kafka.log.SkimpyOffsetMap;

public class Buffer {
	private long minOffset = 0;
	private long earlist = -1;
	private long latest = -1;
	private long currentSize = 0;
	private long offset = -1;
	public LinkedList<Long> processedOffsets;
	public HashSet<Long> skipList;

	public long getOffset() {
		return offset;
	}
	
	public void initOffset(long o){
		this.minOffset = o;
		this.offset = o-1;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	boolean noLogs = false;

	LinkedList<LogRecord> logs;
	private long maxSize;
	long maxStartTime;
	long minStartTime;
	boolean isSorted = false;
	private float bufferThreshold;

	Buffer(long size, float threshold) {
		logs = new LinkedList<LogRecord>();
		maxSize = size;
		processedOffsets = new LinkedList<Long>();
		skipList = new HashSet<Long>();
		bufferThreshold = threshold;
	}

	void setEarliestLatest() {
		sort();
		if(logs.size()>0){
		earlist = logs.get(0).getStartTime();
		latest = logs.get(logs.size() - 1).getStartTime();
		}
	}

	boolean canInsertBuffer() {
		if (logs.isEmpty())
			return true;
			return currentSize < maxSize * bufferThreshold;

//		double a = (latest - earlist) * bufferThreshold;
//		double b = (a + earlist);
//		return b < logs.get(0).getStartTime();
	}

	boolean isEmpty() {
		return logs.isEmpty();
	}

	public boolean add(LogRecord r, int size) {
		if(skipList.contains(r.getOffset())){
			return true;
		} else if(processedOffsets.contains(r.getOffset())){
			return true;
		}
//		for(LogRecord rr : logs){//TODO: to be removed
//			if(rr.getId().equals(r.getId())){
//				System.out.println();
//			}
//		}
		// if(r.id.equals("93-274"))
		// System.out.println();

		r.setLogSize(size);
		if (((size + currentSize) >= maxSize))
			return false;

		if (isSorted && !logs.isEmpty() && r.getStartTime() < logs.getLast().getStartTime()) {
			isSorted = false;
		}

		logs.addLast(r);
		currentSize += size;

		return true;
	}

	public LogRecord getLogToProcess() {
		LogRecord r = getFirst();
		if (!logs.isEmpty() && logs == null) {
			System.out.println("Error: log is NULL while size=" + logs.size());
			System.exit(0);
		}
		return r;
	}

	public void serialize(DataOutputStream os) throws IOException {
		os.writeLong(minOffset);
		int size = processedOffsets.size();
		os.writeInt(size);
		for (int i = 0; i < size; i++) {
			long l = processedOffsets.get(i);
			os.writeLong(l);
		}
//		os.writeInt(skipList.size());
//		for(long offset : skipList){
//			os.writeLong(offset);			
//		}
	}

	public static Buffer deserialize(DataInputStream is, long size, float threshold) throws IOException {
		Buffer br = new Buffer(size, threshold);
		br.minOffset = is.readLong();
		br.offset = br.minOffset;
		int sizeP = is.readInt();

		for (int i = 0; i < sizeP; i++) {
			long l = is.readLong();
			br.processedOffsets.add(l);
		}
//		sizeP = is.readInt();
//
//		for (int i = 0; i < sizeP; i++) {
//			long l = is.readLong();
//			br.skipList.add(l);
//		}
		return br;
	}

	public int logsLength() {
		return logs.size();
	}

	public LogRecord getFirst() {
		if (logs.size() == 0)
			return null;
		sort();
//		if (iHaveOffset(13387)) {
//			if (!containOffset(13387)) {
//				System.out.println();
//			}
//		}
		LogRecord r = logs.removeFirst();
//		if (r.getOffset() == 13387)
//			System.out.println();
		int size = r.getLogSize();
		currentSize -= size;

		if (r.getOffset() == minOffset) {
			Collections.sort(processedOffsets);
			long i = minOffset + 1;
			if (processedOffsets.isEmpty()) {
				minOffset++;
			} else {
				while (!processedOffsets.isEmpty()) {
					if (processedOffsets.get(0) == i) {
						processedOffsets.remove(0);
						i++;
						minOffset = i;
					} else {
						minOffset = i;
						break;
					}
				}
			}
		} else {
			processedOffsets.add(r.getOffset());
		}
		skipList.remove(r.getOffset());


		return r;
	}

	public LogRecord get(int index) {
		if (index >= logs.size())
			return null;
		// sort(); sorting is wrong it will change the order

		LogRecord r = logs.remove(index);
		int size = r.getLogSize();
		currentSize -= size;

		return r;
	}

	public LogRecord viewLog(int index) {
		if (logs.size() == 0)
			return null;
		sort();
		LogRecord r = logs.get(index);

		return r;
	}

	public void sort() {
		if (logs.size() < 2) {
			isSorted = true;
			return;
		}

		if (!isSorted) {
			Collections.sort(logs);
			maxStartTime = logs.getFirst().getStartTime();
			minStartTime = logs.getLast().getStartTime();
			isSorted = true;
		}
	}

	public long size() {
		return currentSize;
	}

	public long maxSize() {
		return maxSize;
	}

	public float getBufferThreshold() {
		return bufferThreshold;
	}

	public void printBuffer() {
		System.out.println("offset: " + offset + ",, minOffset: " + minOffset);
		System.out.print("processedOffsets: " + processedOffsets);
		System.out.println();
		for (LogRecord r : logs) {
			System.out.println(r.getOffset() + ": " + r.toString());
		}
	}

	public void writeBuffer(String filePath) {
		try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), "utf-8"))) {
			out.write("offset: " + offset + ",, minOffset: " + minOffset);
			out.newLine();
			out.write("processedOffsets: " + processedOffsets);
			out.newLine();
			for (LogRecord r : logs) {
				out.write(r.getOffset() + ": " + r.toString());
				out.newLine();
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	private boolean containOffset(int i) {
		for (LogRecord r : logs) {
			if (r.getOffset() == i)
				return true;
		}
		return false;
	}

	boolean iHaveOffsetBoolean = false;
	private boolean iHaveOffset(int i) {
		for (LogRecord r : logs) {
			if (r.getOffset() == i)
				iHaveOffsetBoolean = true;
		}
		return iHaveOffsetBoolean;
	}

}
