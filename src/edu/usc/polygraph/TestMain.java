package edu.usc.polygraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TestMain {

	public static void main(String[] args) {
		//	public LogRecord(String id, String actionName, long startTime, long endTime, char type, Entity[] entities) {
 Long x= 5L;
 modify(x);
 System.out.println(x);
 System.exit(0);
		ArrayList<LogRecord> intervals= new ArrayList<LogRecord>();
		Entity[] entities1= new Entity[1];
		Property[] properties= new Property[1];
		properties[0]= new Property("name", "10", 'N');
		entities1[0]= new Entity("m1", "USR", properties);
		
		
		Entity[] entities2= new Entity[1];
		
		entities2[0]= new Entity("m2", "USR", properties);
		LogRecord r5= new LogRecord("w5","x",30,40,'U', entities1);
		LogRecord r1= new LogRecord("w1","x",10,20,'U', entities1);
		LogRecord r2= new LogRecord("w2","x",30,40,'U', entities1);
		LogRecord r3= new LogRecord("w3","x",5,10,'U', entities2);
		LogRecord r4= new LogRecord("w4","x",11,20,'U', entities2);
		
//		LogRecord r1= new LogRecord("w1","x",10,20,'U', entities1);
//		LogRecord r2= new LogRecord("w2","x",10,20,'U', entities1);
//		LogRecord r3= new LogRecord("w3","x",5,10,'U', entities2);
//		LogRecord r4= new LogRecord("w4","x",9,20,'U', entities2);
		
		intervals.add(r2);
		intervals.add(r1);
		
		intervals.add(r3);
		intervals.add(r4);
		intervals.add(r5);

		
		Collections.sort(intervals,LogRecord.Comparators.START);
		ScheduleList ss = computeGroupSerialScheduleTest(intervals);
		for (Schedule s:ss.schedules){
			System.out.println(s.getRecords());
		}

	}

	private static void modify(Long x) {
	x=7L;

		
	}

	private static void increase(int counter[]) {
		counter[0]=11;
		
	}
	private static ScheduleList computeGroupSerialScheduleTest(ArrayList<LogRecord> intervals) {
		ArrayList<Schedule> NSS = new ArrayList<Schedule>();
		// if (readLogsCount == 2110)
		// System.out.println();
		ScheduleList SS = new ScheduleList();
		for (LogRecord i : intervals) {
			NSS.clear();
			if (SS.isEmpty()) {
				Schedule temp = new Schedule();
				temp.add(i);
				SS.add(temp);
			} else {
				for (Schedule S : SS.schedules) {
					List<Integer> overL = new ArrayList<Integer>();
					int k = 0;
					int thisIndexAndUp = -1;
					int thisIndexAndBelow = Integer.MAX_VALUE;
					HashMap<String, Boolean> list;
					for (k = S.size() - 1; k >= 0; k--) {
					
						if (S.getLogRecord(k).overlap(i)&& S.getLogRecord(k).intersect(i)) {
							overL.add(k);
							overL.add(k + 1);
						} else
							break;
					}
					// if(thisIndexAndUp > thisIndexAndBelow || (read != null &&
					// read.getId().equals("80-243348"))){//TODO: To be removed
					// System.out.println("Read ID: " + read.getId() + " ,,,
					// Offset: " + read.getOffset());
					// System.out.println("Intervales: " + intervals);
					// bucketTest.print2(notAllowedList);
					// System.exit(0);
					// }
					assert thisIndexAndUp <= thisIndexAndBelow : "thisIndexAndUp(" + thisIndexAndUp
							+ ") is not less than thisIndexAndBelow (" + thisIndexAndBelow + ").";
					if (overL.isEmpty()) {
						S.add(i);
						NSS.add(S);
					} else {
						Collections.sort(overL);
						for (int m = overL.size() - 1; m > 0; m--) {
							if (overL.get(m) == overL.get(m - 1))
								overL.remove(m);
						}
						for (int m = overL.size() - 1; m >= 0; m--) {
							if (overL.get(m) > thisIndexAndBelow) {
								overL.remove(m);
								continue;
							}
							if (overL.get(m) < thisIndexAndUp) {
								overL.remove(m);
							}
						}
						for (int m = 0; m < overL.size(); m++) {
							Schedule newS = new Schedule();
							if (overL.get(m) == 0) {
								newS.add(i);
							}
							for (int p = 0; p < S.size(); p++) {
								newS.add(S.getLogRecord(p));
								if (p == (overL.get(m) - 1))
									newS.add(i);
							}
							NSS.add(newS);
						}
					}
				}
				SS.clearSchedules();
				SS.clearOverlaps();
				SS.addAll(NSS,false);
			}
		}
		
		return SS;
	}

}
