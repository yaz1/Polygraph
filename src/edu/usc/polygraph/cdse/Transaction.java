package edu.usc.polygraph.cdse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import edu.usc.polygraph.LogRecord;

//public class Transaction{
//	public static final int NO_CONTAINMENT = -1;
//	public static final int EQUAL = 0;
//	public static final int CONTAIN_A = 1;
//	public static final int CONTAIN_B = 2;
//	public static HashMap<String,Boolean>overlapMap= new HashMap<String,Boolean>();
//	public static HashMap <String,HashSet<String>>commonDI= new HashMap<String,HashSet<String>>();
//	String name;
//	HashSet<DataItem>deltaDI=new HashSet<DataItem>();
//	ArrayList<DataItem> dataItems= new ArrayList<DataItem>();
//	long startTime;
//	long commitTime;
//	static long maxStart=-1;
//	HashSet<String> dataItemNames= new HashSet<String>();
//	public static long getMaxStart(){
//		return maxStart;
//	}
//	Transaction(String n){
//		name=n;
//	}
//	Transaction(String n, long st,long et){
//		name=n;
//		startTime=st;
//		commitTime=et;
//		maxStart=Math.max(startTime, maxStart);
//	}
//	boolean isOverlap(Transaction t){
//		String key1=this.name+"-"+t.name;
//		String key2=t.name+"-"+this.name;
//
//		if (overlapMap.containsKey(key1))
//			return overlapMap.get(key1);
//		if (!overlapMap.containsKey(key2))
//			System.out.println();
//		assert overlapMap.containsKey(key2);
//		
//		return overlapMap.get(key2);
////		
////		if (t.dataItems.size()<= this.dataItems.size()){
////		HashSet<String> dMap = this.getDataItemsMap();
////		for (DataItem d:t.dataItems){
////			//Main.treesCounter++;
////			if (dMap.contains(d.name))
////				return true;
////		}
////		}
////		
////		else{
////			HashSet<String> dMap = t.getDataItemsMap();
////			for (DataItem d:this.dataItems){
////				//Main.treesCounter++;
////				if (dMap.contains(d.name))
////					return true;
////			}
////		}
////		return false;
//	}
//	
//	boolean isOverlapGraph(Transaction t){
//		if (t.dataItems.size()<= this.dataItems.size()){
//			HashSet<String> dMap = this.getDataItemsMap();
//			for (DataItem d:t.dataItems){
//				CDSE.graphCounter++;
//				if (dMap.contains(d.name)){
//					overlapMap.put(this.name+"-"+t.name, true);
//					return true;
//				}
//			}
//			}
//			
//			else{
//				HashSet<String> dMap = t.getDataItemsMap();
//				for (DataItem d:this.dataItems){
//					CDSE.graphCounter++;
//					if (dMap.contains(d.name)){
//						overlapMap.put(this.name+"-"+t.name, true);
//
//						return true;
//					}
//				}
//			}
//		overlapMap.put(this.name+"-"+t.name, false);
//
//			return false;
//	}
//	
//	int containment(Transaction t){
//		if (dataItems.size()==t.dataItems.size()){
//			HashSet<String> dMap = this.getDataItemsMap();
//			for (DataItem d:t.dataItems){
//				CDSE.graphCounter++;
//				if (!dMap.contains(d.name))
//					return NO_CONTAINMENT;
//			}
//			return EQUAL;
//		}
//		else if (dataItems.size()>t.dataItems.size()){
//			HashSet<String> dMap = this.getDataItemsMap();
//			for (DataItem d:t.dataItems){
//				CDSE.graphCounter++;
//				if (!dMap.contains(d.name))
//					return NO_CONTAINMENT;
//			}
//			return CONTAIN_A;
//		}
//		
//		else{ // t is larger
//			HashSet<String> dMap = t.getDataItemsMap();
//			for (DataItem d:this.dataItems){
//				CDSE.graphCounter++;
//				if (!dMap.contains(d.name))
//					return NO_CONTAINMENT;
//			}
//			return CONTAIN_B;
//			
//		}
//		
//		
//	}
//	public HashSet<String> getDataItemsMap() {
////		HashSet<String> dataMap= new HashSet<String>();
////		
////			for (DataItem d:dataItems){
////				dataMap.add(d.name);
////			
////		}
//		return dataItemNames;
//	}
//
//
//	public void addDataItem(DataItem a){
//		if (CDSE.CHECK_DELTA){
//			if (a.isDelta){
//				deltaDI.add(a);
//			}
//		}
//		dataItems.add(a);
//		dataItemNames.add(a.name);
//	}
//	
//
//	public static ArrayList<Transaction> duplicateSchedule(ArrayList<Transaction> s) {
//		ArrayList<Transaction> schedule= new ArrayList<Transaction>();
//		for (Transaction t:s)
//			schedule.add(t);
//		return schedule;
//	}
//	public String toString(){
//		String items="";
//		for (DataItem d: dataItems){
//			items+=d.name+"="+d.value+",";
//		}
//		items=items.substring(0,items.length()-1);
//		return name+"("+items+")";
//	}
//	public static void addOtherTrans(ArrayList<LogRecord> s, ArrayList<LogRecord> transactions, int skipIndex) {
//		for (int i=0;i<transactions.size();i++){
//			
//			if (i!=skipIndex){
//				s.add(transactions.get(i));
//			}
//		}
//		
//	}
//	public boolean isOverlapNodes(Node n) {
//		
//		for (String d:this.dataItemNames){
//			CDSE.treesCounter++;
//			if (!n.remainDataItems.contains(d)){
//				return true;
//			}
//			
//		}
//		return false;
////	while (n!=null){
////		if(this.isOverlap(n.transactions.get(0))){
////			return true;
////		}
////		n=n.parent;
////	}
////	return false;
//	}
//	public static class Comparators {
//
//		public static Comparator<Transaction> COMMIT= new Comparator<Transaction>() {
//
//			@Override
//			public int compare(Transaction o1, Transaction o2) {
//				return (o1.commitTime > o2.commitTime ? 1 : (o1.commitTime < o2.commitTime ? -1 : 0));
//			}
//		};
//		public static Comparator<Transaction> COMMIT_REV= new Comparator<Transaction>() {
//
//			@Override
//			public int compare(Transaction o1, Transaction o2) {
//				return (o1.commitTime < o2.commitTime ? 1 : (o1.commitTime > o2.commitTime ? -1 : 0));
//			}
//		};
//		
//	}
//
////	public Transaction findFirstOverlapping(ArrayList<Node> nodes) {
////		for (Node n: nodes){
////			LogRecord trans=n.transactions.get(0);
////			if (this.isOverlap(trans))
////				return trans;
////			}
////			
////		
////		return null;
////	}
//}
//class DataItem{
//	public String name;
//	public String value;
//	public boolean isDelta=false;
//	DataItem(String n, String v){
//		name=n;
//		value=v;
//	}
