package edu.usc.polygraph.snapshot;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import edu.usc.polygraph.DBState;
import edu.usc.polygraph.Entity;
import edu.usc.polygraph.LogRecord;
import edu.usc.polygraph.Property;
import edu.usc.polygraph.Schedule;
import edu.usc.polygraph.ScheduleList;
import snapshot.bucketTest;

public class Test {

	public static void main(String[] args) {
		
		HashMap<String, LinkedList<DBState>> db = testDB();
		HashMap<String, ScheduleList> currentSS= new HashMap<String, ScheduleList>();
		ScheduleList sl= new ScheduleList();
		currentSS.put("sl1", sl);
		sl.endTime=1000;
		sl.notShrink= new ArrayList<String>();
		sl.notShrink.add("notShrink1");
		sl.participatingKeys= new HashSet<String>();
		sl.participatingKeys.add("pk1");
		Entity[] e= new Entity[1];
		Property[] properties= new Property[1];
		properties[0]= new Property("p1", "pv", 'i');
		e[0]= new Entity("ekey", "e1", properties);
		LogRecord newlog= new LogRecord("nlog","action",0,1,'F',e);
		newlog.setId("nlog");
		LogRecord r1= new LogRecord("nlog","action",0,1,'F',e);
		r1.setId("r1");
		LogRecord overlap= new LogRecord("nlog","action",0,1,'F',e);
		overlap.setId("over");
		sl.overlapList.add(overlap);
		sl.newLogs= new ArrayList<LogRecord>();
		sl.newLogs.add(newlog);
		Schedule sched= new Schedule("",null);
		sched.add(r1);
		sl.schedules.add(sched);
		sched.getImpactedStates().put("key1", db.get("key1").getFirst());
		String file="ss.bin";
		
		HashSet<String> logs= new HashSet<String>();
		String logFile="logs.bin";
		DataOutputStream osLog=null;
		try {
			osLog = bucketTest.serializeLogs();
		} catch (IOException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
		serializeSS(currentSS, file, db, logs, osLog);
		HashMap<String, LogRecord> logsMap = null;
		try {
			logsMap = bucketTest.deserializeLogs();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		currentSS= deserializeSS(file, db, logsMap);
		
		for (String key:currentSS.keySet()){
			System.out.println("SSL key:"+key);
			
			sl=currentSS.get(key);
			System.out.println("endTime:"+sl.endTime);
			System.out.println("over:"+sl.overlapList);
			System.out.println("newLogs:"+sl.newLogs);
			System.out.println("notShrink:"+sl.notShrink);
		
			System.out.println("pk:"+sl.participatingKeys);
			
			
			for (Schedule s:sl.schedules){
				System.out.println("Records:"+s.getRecords());
				System.out.println("Impacted:");
				for(String k:s.getImpactedStates().keySet()){
					System.out.println("key:"+k); 
					DBState st = s.getImpactedStates().get(k);
					System.out.println(Arrays.toString(st.getValue()));
					
					
				}
				
			}


			
		}
		
			}
	
	private static  HashMap<String, LinkedList<DBState>> testDB() {
		HashMap<String, LinkedList<DBState>> dbState= new HashMap<String, LinkedList<DBState>>();
		LinkedList<DBState> ll = new LinkedList<DBState>();
		DBState s1= new DBState(0,null);
		ll.add(s1);
		dbState.put("key1", ll);
		
		
		
		LinkedList<DBState> lll = new LinkedList<DBState>();
		DBState s2= new DBState(1,null);
		DBState s3= new DBState(2,null);
		lll.add(s2);
		lll.add(s3);
		dbState.put("key2", lll);
		
		
		
		
		String file = "db.bin";
		serializeDB(dbState , file);
		HashMap<String, LinkedList<DBState>> db = deserializeDB(file);
		for (String key:db.keySet()){
			System.out.println("Key:"+key);
			LinkedList<DBState> ll2 = db.get(key);
			for (DBState state:ll2){
				System.out.println("DBSTATE:"+state.getRefrenceCount()+","+Arrays.toString(state.getValue()));
				
			}
			
		}

		return dbState;
		
	}

	public static HashMap<String, ScheduleList>  deserializeSS(String file,HashMap<String, LinkedList<DBState>> db,HashMap<String,LogRecord> logs)
	{
		HashMap<String, ScheduleList> currentSS= new HashMap<String, ScheduleList>();
		
		try {
			DataInputStream is= new DataInputStream(new FileInputStream("./snapshots/"+file));
			
			int numKeys=is.readInt();
			for (int i=0; i<numKeys;i++){
				int keysize=is.readInt();
				byte[] b = new byte[keysize]; 
				is.read(b, 0, keysize);
				String key= new String(b);
				ScheduleList ss =ScheduleList.deserialize(is,db,logs);
				currentSS.put(key, ss);
		
				
			}
			is.close();
			
			
			
		} catch (FileNotFoundException e) {
		
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		return currentSS;
		
	}
	
	public static void  serializeSS(HashMap<String, ScheduleList> currentSS, String file,HashMap<String, LinkedList<DBState>> db,HashSet<String> logs,DataOutputStream osLog)
	{
		
		try {
			DataOutputStream os= new DataOutputStream(new FileOutputStream("./snapshots/"+file));
			int numKeys=currentSS.size();
			os.writeInt(numKeys);
			for (String key: currentSS.keySet()){
				byte[] b = key.getBytes();
				os.writeInt(b.length);
				os.write(b);
				
				ScheduleList ss = currentSS.get(key);
				ss.serialize(os,db,logs,osLog);
		
				
			}
			os.close();
			
		} catch (FileNotFoundException e) {
		
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		
	}

	public static void  serializeDB(HashMap<String, LinkedList<DBState>> dbState , String file){
		
		try {
			DataOutputStream os= new DataOutputStream(new FileOutputStream("./snapshots/"+file));
			int numKeys=dbState.size();
			os.writeInt(numKeys);
			for (String key: dbState.keySet()){
				byte[] b = key.getBytes();
				os.writeInt(b.length);
				os.write(b);
				LinkedList<DBState> ll = dbState.get(key);
				os.writeInt(ll.size());
				for (DBState state:ll){
					os.writeInt(state.getRefrenceCount());
					os.writeInt(state.size());
					for (String v:state.getValue())
					{
						byte[] bb= v.getBytes();
						os.writeInt(bb.length);
						os.write(bb);
					
						
					}
					
				}
				
			}
			os.close();
			
		} catch (FileNotFoundException e) {
		
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		
	}
	
	public static HashMap<String, LinkedList<DBState>>  deserializeDB(String file){
		HashMap<String, LinkedList<DBState>> dbState= new HashMap<String, LinkedList<DBState>>();
		try {
			DataInputStream is= new DataInputStream(new FileInputStream("./snapshots/"+file));
			int numKeys= is.readInt();
	
			for (int i=0; i<numKeys;i++){
				int keylength=is.readInt();
				byte[] b= new byte[keylength];
				is.read(b, 0, keylength);
				String key= new String(b);
				LinkedList<DBState> ll = dbState.get(key);
				assert ll==null:"ll not null!";
					ll= new LinkedList<DBState>();
				
				int llsize=is.readInt();
				for (int l=0;l<llsize;l++){
					
					int refCount=is.readInt();
					int stateSize=is.readInt();
					String values[]= new String[stateSize];
					for (int v=0;v<values.length;v++)
					{
						int vlen=is.readInt();
				
						byte[] vb= new byte[vlen];
						is.read(vb, 0, vlen);
						 values[v]= new String(vb);
					}
					DBState state= new DBState(refCount, values);
					ll.add(state);
					
				}
				dbState.put(key, ll);
				
			}
			is.close();
			
		} catch (FileNotFoundException e) {
		
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	return dbState;	
	}

}


