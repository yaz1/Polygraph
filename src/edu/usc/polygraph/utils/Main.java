package edu.usc.polygraph.utils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import edu.usc.polygraph.Entity;
import edu.usc.polygraph.LogRecord;
import edu.usc.polygraph.Property;
import edu.usc.polygraph.ValidationParams;

public class Main {

	public static int startTimeIndex = 4;
	public static int countX=0;
	public static void main(String[] args) {
		String cmp=args[0];
		String[] newargs= new String[args.length-1];
		newargs=Arrays.copyOfRange(args, 1, args.length);
		if (cmp.equals("sort")){
		sort(newargs[0],newargs[1]);
		}
		else if(cmp.equals("stale"))
		compareStaleFiles(newargs);
		else{
			HashSet<String> init= readFile(args[1]);
			HashSet<String> noinit= readFile(args[2]);
			HashSet< String> execludeL= new HashSet<String>();
			//R,GetPendingFriends,78,8,605993560367370,605993560522662,MEMBER;8;PENDING_CNT:0:R

			//execludeL.add("MEMBER-21-PENDING_CNT");
//			execludeL.add("MEMBER-0-FRIEND_CNT");
//			execludeL.add("MEMBER-8-FRIEND_CNT");
			//execludeL.add("MEMBER-132-FRIEND_CNT");

//			MEMBER-21-PENDING_CNT
//			MEMBER;295;FRIEND_CNT
//			MEMBER;132;FRIEND_CNT
			
			HashSet<String> initOnly;
			initOnly= existFirstOnly(init,noinit);
			System.out.println("Exist in Init only:");
			System.out.println("Count:"+initOnly.size()+":"+initOnly);
			noinit= existFirstOnly(noinit,init);
			System.out.println("Exist in noInit only:");
			System.out.println("Count:"+noinit.size()+":"+noinit);
			
			exclude(noinit, execludeL, args[3],init);
			noinit= existFirstOnly(noinit,init);
			System.out.println("Exist in noInit only after excluding:");
			System.out.println("Count:"+noinit.size()+":"+noinit);
			HashMap<String, ArrayList<String>> logs= new HashMap<String, ArrayList<String>>();
			for (String item: noinit){
				String line=getLine(item, args[3]);
				LogRecord r = LogRecord.createLogRecord(line);
				String key= r.getEntities()[0].getEntityKey();
				if (logs.containsKey(key)){
					logs.get(key).add(line);
				}
				else{
					ArrayList<String> recs= new ArrayList<String>();
					recs.add(line);
					logs.put(key, recs);
				}
			}
			for (String key: logs.keySet()){
				System.out.println(key);
				for (String line: logs.get(key)){
					System.out.println(line);
				}
			}
			execludeFromInitOnly(initOnly,execludeL,args[3]);
			
			
			System.out.println("Exist in Init only after excluding:");
			System.out.println("Count:"+initOnly.size()+":"+initOnly);
			System.out.println("Execlude list");
		System.out.println("Count:"+execludeL.size()+":"+execludeL);	
			
		}
	}
	private static void execludeFromInitOnly(HashSet<String> items, HashSet<String> execludeL,String filesDir) {
		Iterator<String> it = items.iterator();
		while(it.hasNext()){
			String item= it.next();
			//R,GetPendingFriends,78,8,605993560367370,605993560522662,MEMBER;8;PENDING_CNT:0:R
			String line=getLine(item,filesDir);
			LogRecord r = LogRecord.createLogRecord(line);
//			if (r.getId().equals("72-25"))
//				System.out.println();
			for (Entity e: r.getEntities()){
					for (Property p: e.getProperties()){
						String pkey=Property.getProprtyKey(e, p);
						if (execludeL.contains(pkey)){
							it.remove();
						}
					}
			}
		}
		
	
	
	}
	private static void compareStaleFiles(String[] args) {
		HashSet<String> ids1= readFile(args[0]);
		HashSet<String> ids2= readFile(args[1]);
		HashSet<String> oneOnly= existFirstOnly(ids1,ids2);
		HashSet<String> twoOnly= existFirstOnly(ids2,ids1);
		
		


		
		System.out.println("Count:"+oneOnly.size()+":"+oneOnly);
		System.out.println("Count:"+twoOnly.size()+":"+twoOnly);

	}
	private static void exclude(HashSet<String> items, HashSet<String> execludeList, String filesDir, HashSet<String> init){
		Iterator<String> it = items.iterator();
		while(it.hasNext()){
			String item= it.next();
			//R,GetPendingFriends,78,8,605993560367370,605993560522662,MEMBER;8;PENDING_CNT:0:R
			String line=getLine(item,filesDir);
			LogRecord r = LogRecord.createLogRecord(line);
//			if (r.getId().equals("72-25"))
//				System.out.println();
			for (Entity e: r.getEntities()){
					for (Property p: e.getProperties()){
						String pkey=Property.getProprtyKey(e, p);
						if (execludeList.contains(pkey)){
							it.remove();
						}
						else{
							// verify
							boolean remove= verfiy(r, execludeList,init);
							if (remove)
								it.remove();

								
						}
						
					}
			}
	
			
		}
		
	}
	private static String getLine(String item, String filesDir) {
		String line = null;
		String result=null;
		FileInputStream fstreams;
		try {
			String fileId= item.split("-")[0];
			String recordId=item.split("-")[1];
			fstreams = new FileInputStream(filesDir+"\\"+"read0-"+fileId+".txt");
			DataInputStream dataInStreams = new DataInputStream(fstreams);
			BufferedReader bReaders = new BufferedReader(new InputStreamReader(dataInStreams));

		
			while ((line = bReaders.readLine()) != null) {
				if (line.split(",")[3].equals(recordId)){
				
					result=line;
				
				break;
				}
			
			}
			bReaders.close();
			dataInStreams.close();
			fstreams.close();

		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.out.println("Log file not found " + e.getMessage());
		}

		return result;
	}
	private static HashSet<String> existFirstOnly(HashSet<String> ids1, HashSet<String> ids2) {
		HashSet<String> ids = new HashSet<String>();
		for (String id: ids1){
			
			if (!ids2.contains(id)){
				ids.add(id);
			}
		}
		return ids;
	}
	private static boolean exist(ArrayList<String> ids2, String id) {
		
		for (String i:ids2){
			if (i.equals(id))
				return true;
		}
		return false;
	}
	private static HashSet<String> readFile(String file) {
		String src = file;
		
		HashSet<String> ids = new HashSet<String>();
	
		FileInputStream fstreams;
		try {
			fstreams = new FileInputStream(src);
			DataInputStream dataInStreams = new DataInputStream(fstreams);
			BufferedReader bReaders = new BufferedReader(new InputStreamReader(dataInStreams));

			String line;
			while ((line = bReaders.readLine()) != null) {
				if (line.contains("ID")){
				String id = line.substring(line.indexOf(':')+1);
				ids.add(id);
				}
			
			}
			bReaders.close();
			dataInStreams.close();
			fstreams.close();

		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.out.println("Log file not found " + e.getMessage());
		}

		return ids;
	}
	private static void sort(String src, String dist) {
//		String src = args[0];
//		String dist = args[1];

		ArrayList<MyObject> objects = new ArrayList<MyObject>();
		FileInputStream fstreams;
		try {
			fstreams = new FileInputStream(src);
			DataInputStream dataInStreams = new DataInputStream(fstreams);
			BufferedReader bReaders = new BufferedReader(new InputStreamReader(dataInStreams));

			String line;
			while ((line = bReaders.readLine()) != null) {
				String[] tokens = line.split("[,]");
				long startTime = Long.parseLong(tokens[startTimeIndex]);
				objects.add(new MyObject(startTime, line));
			}
			bReaders.close();
			dataInStreams.close();
			fstreams.close();

		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.out.println("Log file not found " + e.getMessage());
		}

	//	System.out.println("Completed Reading the file.");

		Collections.sort(objects);
	//	System.out.println("Completed Sorting the file.");

		File distFile = new File(dist);
		FileWriter ufstream;
		try {
			ufstream = new FileWriter(distFile);
			BufferedWriter bWriter = new BufferedWriter(ufstream);

			for (int i = 0; i < objects.size(); i++) {
				bWriter.write(objects.get(i).line + "\n");
			}
			bWriter.close();
			ufstream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Completed Writing sorted file.");

	


		
	}
	

private static boolean verfiy(LogRecord record, HashSet<String> execludeList, HashSet<String> init) {
	if (record.getEntities()[0].getProperties().length>1)
		return false;
//	if (record.getId().equals("91-1488"))
//		System.out.println();
	String src="C:\\Users\\yaz\\y.txt";
	String dist="C:\\Users\\yaz\\ysort.txt";
	//C:/cygwin64/bin/
	String traceFolder=" /cygdrive/c/Users/yaz/Documents/linuxShare/10K-100fpm-100Threads-1Min-1log-TwemcachedNoIQ";
	String searchStr=record.getEntities()[0].getName()+";"+record.getEntities()[0].getKey()+";";
	String cmd="C:/cygwin64/bin/grep -rh \""+searchStr+"\" "+traceFolder+" > "+src;
	executeRuntime(cmd, true,src);

	sort(src,dist);
	
	FileInputStream fstreams;
	try {
		fstreams = new FileInputStream(dist);
		DataInputStream dataInStreams = new DataInputStream(fstreams);
		BufferedReader bReaders = new BufferedReader(new InputStreamReader(dataInStreams));

		String line;
		LogRecord firstRead=null;
		LogRecord lastWrite=null;
		LogRecord possibleStaleRead=null;
		boolean done=false;
		while ((line = bReaders.readLine()) != null) {
			LogRecord r= LogRecord.createLogRecord(line);
			if (r.getType()==ValidationParams.READ_RECORD){
				
				for (Entity e: r.getEntities()){

				Entity erecord= record.getEntities()[0];
				if (e.getEntityKey().equals(erecord.getEntityKey())){
					Property per= e.getProperties()[0];
					Property perec= erecord.getProperties()[0];
					if (per.getName().equals(perec.getName())){
						if (firstRead==null){
							firstRead=r;
							//if (firstRead.getId().equals(record.getId())){
								if (init.contains(firstRead.getId())){
									execludeList.add(Property.getProprtyKey(record.getEntities()[0], record.getEntities()[0].getProperties()[0]));
									bReaders.close();
									dataInStreams.close();
									fstreams.close();
									countX++;
									return true;
								}
							
							break;
						}
						else{
							// check if establish read and stale
							if(lastWrite!=null && r.getStartTime()>lastWrite.getEndTime()){
								possibleStaleRead=r;
								done=true;
								break;
							}
						}
					}
				}

				} // end for
				if (done)
					break;
			}
			else{
				// write
				Entity erecord= record.getEntities()[0];

				for (Entity e: r.getEntities()){
			
				if (e.getEntityKey().equals(erecord.getEntityKey())){
					Property per= e.getProperties()[0];
					Property perec= erecord.getProperties()[0];
					if (per.getName().equals(perec.getName())){
						if (lastWrite==null || lastWrite.getEndTime()< r.getEndTime()){
							lastWrite=r;
						}
			}
			}
				}
		
						
		
			}
		}// while
		bReaders.close();
		dataInStreams.close();
		fstreams.close();
		if (possibleStaleRead !=null){
			if (init.contains(possibleStaleRead.getId())){
				execludeList.add(Property.getProprtyKey(record.getEntities()[0], record.getEntities()[0].getProperties()[0]));
			return true;
			}
		}
	} catch (IOException e) {
		e.printStackTrace(System.out);
		System.out.println("Log file not found " + e.getMessage());
	}

	return false;
}

public static void executeRuntime(String cmd, boolean wait, String dist) {
		Process p;
		File ufile = new File( dist);
		FileWriter ufstream = null;
		try {
			ufstream = new FileWriter(ufile);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		StringBuilder sb= new StringBuilder();
	BufferedWriter file = new BufferedWriter(ufstream);
	try {

			p = Runtime.getRuntime().exec(cmd);
			if (wait) {
				InputStream stdout = p.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line+"\n");
				}
				p.waitFor();
			} else
				Thread.sleep(5000);
		} catch (Exception e2) {
			e2.printStackTrace(System.out);
		}
		try {
			sb.deleteCharAt(sb.length()-1);
			file.write(sb.toString());
			file.flush();
			file.close();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		

	}

}
