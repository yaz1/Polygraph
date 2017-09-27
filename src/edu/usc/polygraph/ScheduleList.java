package edu.usc.polygraph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;


public class ScheduleList {

	public ArrayList<Schedule> schedules;
	public ArrayList<LogRecord> overlapList;
	public long endTime;
//	public static long CET;
	public ArrayList<String> notShrink =null;
	
	public ArrayList<LogRecord> newLogs = null;
	public Set<String> participatingKeys =null;
	public ScheduleList() {
		this.schedules = new ArrayList<Schedule>();
		this.overlapList = new ArrayList<LogRecord>();
	}

	public ScheduleList(ArrayList<Schedule> schedules, ArrayList<LogRecord> overlapList) {
		this.schedules = schedules;
		this.overlapList = overlapList;
	}

	public boolean isEmpty() {
		return schedules.isEmpty();
	}

	public void add(Schedule s) {
		schedules.add(s);
	}

	public void clearSchedules() {
	
		for (Schedule s: schedules){
			s.getParents().clear();
		}
		schedules.clear();
		// overlapList.clear();
	}

	public void addAll(ArrayList<Schedule> SS,boolean insert) {
		for (Schedule s : SS) {
			if (insert ||!schedules.contains(s))
				schedules.add(s);
		}
	}
	public void addAll(HashSet<Schedule>   SS) {
	//	for(ArrayList<Schedule> arr:SS){
		for (Schedule s : SS) {
			if (!schedules.contains(s))
				schedules.add(s);
		}
	//	}
	}
	public void updateOverlapList(long et) {
		endTime = et;
		for (Schedule s : schedules) {
			ArrayList<LogRecord> over = s.getOverlap(et);

			for (LogRecord log : over) {
				if (!overlapList.contains(log))
					overlapList.add(log);
			}
		}
	}

	public boolean contain(Schedule s2) {
		for (Schedule s1 : schedules) {
			if (s1.same(s2))
				return true;
		}
		return false;
	}

	public void removeDuplicates(HashMap<String, LinkedList<DBState>> dbState,boolean isValidate, ArrayList<String> discardedSchIdList, int s1Size, int s2Size) {
		if (s1Size ==0|| s2Size==0)
			return;
		//YAZXX
	
		for (int i = 0; i < schedules.size() - 1; i=i+s2Size) {
			int jcount=0;
			for (int j = i + 1; j < schedules.size(); j++) {
				jcount++;
				if (jcount> s2Size-1)
					break;
					
				Schedule sL = schedules.get(j);
				if (schedules.get(i).sameRecords(sL)) {
				//YazXX	schedules.get(i).addAllParents(sL.getParents());
					schedules.get(i).getParents().clear();
					schedules.remove(j);
					if (!isValidate) {
						discardedSchIdList.add(sL.sid);
						sL.destructor(dbState);
					}
					j--;
				}
			}
		}
	}

	// public void updateEntities() {
	// for (Schedule s : schedules) {
	// s.updateEntities();
	// }
	// }

	public void removeNULLs() {
		for (int i = 0; i < schedules.size(); i++) {
			if (schedules.get(i) == null) {
				schedules.remove(i);
				i--;
			}
		}
	}

	public int indexOf(Schedule temp) {
		return schedules.indexOf(temp);
	}

	public Schedule get(int i) {
		return schedules.get(i);
	}

	public void clearOverlaps() {
		overlapList.clear();
	}

//	private void debugStates() {
//		Set<String> keys = Validator.dbState.keySet();
//		for (String key : keys) {
//			LinkedList<DBState> ll = Validator.dbState.get(key);
//			int sum = 0;
//			for (DBState st : ll) {
//				sum += st.getRefrenceCount();
//			}
//			if (sum > schedules.size()) {
//				System.out.println("Key:" + key + " has more states than the number of SS.");
//			}
//		}
//
//	}

	private int debugSearchImpactedEntities(String key) {
		int counter = 0;
		for (Schedule s : schedules) {
			if (s.getImpactedStates().containsKey(key)) {
				counter++;
			}
		}
		return counter;
	}

	// public Entity[] debugSearchCustomerImpactedEntities(String key) {
	// Entity[] es = new Entity[schedules.size()];
	// int i = 0;
	// for (Schedule s : schedules) {
	// if (s.getImpactedEntities().containsKey(key)) {
	// // es[i++] = s.getImpactedEntities().get(key);
	// }
	// }
	// return es;
	// }

	public void shrinkImpacted(HashMap<String, LinkedList<DBState>> dbState) {
		// if (ValidationMain.readLogsCount >= 400)
//		// System.out.println();
//		if (notShrink==null)
//			notShrink= new ArrayList<String>();
		notShrink .clear();
		if (schedules.size() == 0)
			return ;
		if (schedules.size() == 1) {
			schedules.get(0).clearImpacted(dbState);
			return ;
		} else {
			Set<String> allKeys = new HashSet<String>();
			for (Schedule s : schedules) {
				allKeys.addAll(s.getImpactedStates().keySet());
			}

			ArrayList<String> toShrink = new ArrayList<String>();
			for (String key : allKeys) {
				DBState first = schedules.get(0).getImpactedStates().get(key);
				boolean addToShrink = true;
				if (first == null)
					addToShrink = false;
				for (int i = 1; (i < schedules.size()) && addToShrink; i++) {
					Schedule s = schedules.get(i);
					DBState current = s.getImpactedStates().get(key);

					if (current == null) {
						addToShrink = false;
						break;
					}
					if (first != current) {
						addToShrink = false;
						String ename = key.substring(0, key.indexOf(ValidationParams.KEY_SEPERATOR));
						int eindex = -1;
						for (int p = 0; p < ValidationParams.ENTITY_NAMES.length; p++) {
							if (ValidationParams.ENTITY_NAMES[p].equals(ename)) {
								eindex = p;
								break;

							}
						}
						for (int p = 0; p < first.value.length; p++) {
							if (p < current.value.length) {
								if (!ValidationParams.hasInitState){
								if (first.value[p]==null || current.value[p]==null)
									continue;
								}
								

								if (!first.value[p].equals(current.value[p])) {
									notShrink.add(key + ValidationParams.KEY_SEPERATOR + ValidationParams.ENTITY_PROPERTIES[eindex][p]);

								}
							}
						}

						break;
					}
				}
				if (addToShrink) {
					toShrink.add(key);
				}
			}
			for (String key : toShrink) {
				int countDeleted = 0;
				for (Schedule s : schedules) {
					DBState current = s.getImpactedStates().get(key);
					if (current.getValue() == Validator.deletedArr)
						countDeleted++;
					current.decrement();
					s.getImpactedStates().remove(key);
				}
				assert schedules.size() == countDeleted || countDeleted == 0 : "No all of them point to the same State " + schedules.size() + " == " + countDeleted;
				if (countDeleted == 0) {
					// TODO: check with mr1
					assert dbState.get(key).size() == 1 : "There should be only ONE state in the LL:";
					assert dbState.get(key).getFirst().getRefrenceCount() == 0 : "RefrenceCount is not ZERO";
				}
			}
		
		}
	}
	public void initCurrentSS(){
		endTime=0;
		 newLogs = new ArrayList<LogRecord>();
		 notShrink = new ArrayList<String>();
		 participatingKeys=new HashSet<String>();
	}
	private boolean allEquals(DBState[] allStates) {
		DBState first = null;
		int start = 0;
		while (first != null)
			first = allStates[start++];
		for (int i = start; i < allStates.length; i++) {
			if (allStates[i] != null) {
				if (first != allStates[i])
					return false;
			}
		}
		return true;
	}

	public void validSchedules(HashMap<String, LinkedList<DBState>> dbState, HashSet<ArrayList<Schedule>> newSS, long startTime, ArrayList<String> discardedSchIdList) {
		HashSet<Schedule>   listOfSchedules= new HashSet<Schedule>();
		for (ArrayList<Schedule> arr:newSS){
			for (Schedule s:arr){
				listOfSchedules.add(s);
			}
			
		}
		for (Schedule s : schedules) {
			if (!listOfSchedules.contains(s)) {
				discardedSchIdList.add(s.sid);
				s.destructor(dbState);
			}
		}
		clearSchedules();
		clearOverlaps();
		addAll(listOfSchedules);
		removeNULLs();
		updateOverlapList(endTime);
	}

	public void addAndUpdateImpcated(HashMap<String, LinkedList<DBState>> dbState,Schedule s) {
		schedules.add(s);
		s.updateImpcated(dbState);
	}

	private void debugeConflict(HashMap<String, LinkedList<DBState>> dbState) {
		Set<String> allKeys = new HashSet<String>();
		for (Schedule s : schedules) {
			allKeys.addAll(s.getImpactedStates().keySet());
		}

		for (String key : allKeys) {

			boolean conflict = false;
			DBState first = null;
			int start = 0, numOfNulls = 0;
			while (first == null) {
				first = schedules.get(start++).getImpactedStates().get(key);
				if (first == null)
					numOfNulls++;
			}
			for (int i = start; i < schedules.size(); i++) {
				if (schedules.get(i).getImpactedStates().get(key) != null) {
					if (first != schedules.get(i).getImpactedStates().get(key) && !conflict) {
						if (key.contains("CUSTOMER")) {
							// String str1 = ((ChildDBState) first.getValue()[3]).key;
							// String str2 = ((ChildDBState) schedules.get(i).getImpactedStates().get(key).getValue()[3]).key;
							// if (str1.equals("ORDER-1-6-2819-383") || str2.equals("ORDER-1-6-2819-383")) {
							// System.out.println();
							// }
							// if (str1.equals("ORDER-1-6-2819-3548") || str2.equals("ORDER-1-6-2819-3548")) {
							// System.out.println();
							// }
							// if ((!str1.equals(str2)) && !conflict) {
							// System.out.println("Conflict on " + key);
							// conflict = true;
							// }
						} else {
							System.out.println("Conflict on " + key);
							conflict = true;
						}
					}
				} else {
					numOfNulls++;
				}
			}
			if (numOfNulls > 0 && conflict)
				System.out.printf("Number of NULLs for key %s = %d\n", key, numOfNulls);
		}

		boolean recordsConflict = false;
		if (schedules.size() > 1) {
			if (schedules.get(0).getRecords().size() != schedules.get(1).getRecords().size()) {
				recordsConflict = true;
			} else {
				for (int i = 0; i < schedules.get(0).getRecords().size(); i++) {
					if (!schedules.get(0).getRecords().get(i).getId().equals(schedules.get(1).getRecords().get(i).getId())) {
						recordsConflict = true;
					}
				}
			}
		}

		Set<String> allLL = dbState.keySet();
		int count = 0;
		for (String llKey : allLL) {
			LinkedList<DBState> ll = dbState.get(llKey);
			if (ll.size() > 1)
				count++;
		}
		System.out.printf("LLs with > 1 state = %d ... %s\n", count, (recordsConflict ? "record conflict in scheduals 0 and 1" : ""));
	}

	private DBState[] debug_checkEverySSImpacted(String key) {
		DBState[] result = new DBState[schedules.size()];
		int from = 0;
		int to = 0;
		DBState oldSt = schedules.get(from).getImpactedStates().get(key);
		for (int i = 1; i < schedules.size(); i++) {
			DBState st = schedules.get(i).getImpactedStates().get(key);
			result[i] = st;
			if (oldSt == st)
				to = i;
			else {
				System.out.printf("(%d-%d)%s; ", from, to, oldSt);
				from = to = i;
				oldSt = st;
			}
		}
		System.out.printf("(%d-%d)%s; ", from, to, oldSt);
		System.out.println();
		return result;
	}

	public int append(ScheduleList SS) {
		if (schedules.size() == 0)
			schedules.add(new Schedule());
		int startIndex = schedules.get(0).getRecords().size() + 1;

		if (SS.schedules.size() <= 1) {
			for (Schedule s1 : schedules) {
				for (Schedule s2 : SS.schedules) {
					s1.append(s2);
				}
			}
		} else {
			ArrayList<Schedule> newSSs = new ArrayList<Schedule>();
			for (Schedule s1 : schedules) {
				for (int i = 0; i < SS.schedules.size(); i++) {
					Schedule s2 = SS.schedules.get(i);
					if (i + 1 != SS.schedules.size()) {
						Schedule newS1 = new Schedule();
						newS1.append(s1);
						newS1.append(s2);
						newSSs.add(newS1);
					} else {
						s1.append(s2);
					}
				}
			}
			schedules.addAll(newSSs);
		}

		return startIndex;
	}


	public void append(ArrayList<LogRecord> currentIntervals) {
		if (schedules.size() == 0)
			schedules.add(new Schedule());
		for (Schedule s : schedules) {
			for (LogRecord r : currentIntervals) {
				s.add(r);
			}
		}
	}

	public void serialize(DataOutputStream os, HashMap<String, LinkedList<DBState>> db, HashSet<String> logs, DataOutputStream osLog) throws IOException {
		os.writeLong(endTime);
		int numS= schedules.size();
		os.writeInt(numS);
		
		for (int i=0; i< numS;i++){
			Schedule schedule = schedules.get(i);
			schedule.serialize(os,db,logs,osLog);
		}
		int numOver= this.overlapList.size();
		os.writeInt(numOver);
		for (int i=0; i< numOver;i++){
			LogRecord r= overlapList.get(i);
			r.serializeID(os);
			if (!logs.contains(r.getId()));
			{
				r.serialize(osLog);
				logs.add(r.getId());
			}
		}
		
		
		int numNotShrink= this.notShrink.size();
		os.writeInt(numNotShrink);
		for (String value:notShrink){
			byte[] b = value.getBytes();
			os.writeInt(b.length);
			os.write(b);
		}
		
		int numNewLogs= this.newLogs.size();
		os.writeInt(numNewLogs);
		for (int i=0; i< numNewLogs;i++){
			LogRecord r= newLogs.get(i);
			r.serializeID(os);
			if (!logs.contains(r.getId()));
			{
				r.serialize(osLog);
				logs.add(r.getId());
			}
		}
		int numPKeys= this.participatingKeys.size();
		os.writeInt(numPKeys);
		for (String key:participatingKeys){
			byte[] b = key.getBytes();
			os.writeInt(b.length);
			os.write(b);
		}
		
		
		
		
		
	}

	public static ScheduleList deserialize(DataInputStream is, HashMap<String, LinkedList<DBState>> db,
			HashMap<String, LogRecord> logs) throws IOException {
		ScheduleList ssl= new ScheduleList();
		ssl.endTime=is.readLong();
		int numS= is.readInt();
		
		
		for (int i=0; i< numS;i++){
			Schedule schedule = Schedule.deserialize(is,db,logs);
			ssl.add(schedule);
		}
		int numOver= is.readInt();
		
		for (int i=0; i< numOver;i++){
			int logIdSize=is.readInt();
			byte b[]= new byte[logIdSize];
			is.read(b, 0, logIdSize);
			String id= new String (b);
			LogRecord r= logs.get(id);
			assert r!=null: "log is null!!";
			ssl.overlapList.add(r);
		
		}
		
		
		int numNotShrink= is.readInt();
		ssl.notShrink= new ArrayList<String>();
		for (int i=0;i<numNotShrink;i++){
			int vlen= is.readInt();
			byte b[]= new byte[vlen];
			is.read(b,0,vlen);
			String value=new String(b);
			ssl.notShrink.add(value);
			
		}
		
		int numnewLogs= is.readInt();
		ssl.newLogs= new ArrayList<LogRecord>();
		for (int i=0; i< numnewLogs;i++){
			int logIdSize=is.readInt();
			byte b[]= new byte[logIdSize];
			is.read(b, 0, logIdSize);
			String id= new String (b);
			LogRecord r= logs.get(id);
			assert r!=null: "log is null!!";
			ssl.newLogs.add(r);
		
		}
		int pKeys= is.readInt();
		ssl.participatingKeys= new HashSet<String>();
		for (int i=0;i<pKeys;i++){
			int vlen= is.readInt();
			byte b[]= new byte[vlen];
			is.read(b,0,vlen);
			String value=new String(b);
			ssl.participatingKeys.add(value);
			
		}
		
		
		
		
		
		
		
		
		
		return ssl;
	}
}
