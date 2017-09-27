package edu.usc.polygraph.cdse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import edu.usc.polygraph.LogRecord;


public class Node {
	String name="";
	Node parent;
	String edge=null;
	long commit=-1;
	public long getCommit() {
		return commit;
	}
	public void setCommit(long commit) {
		this.commit = commit;
	}
	ArrayList<LogRecord> transactions= new ArrayList<LogRecord>();
	ArrayList<Node> children= new ArrayList<Node>();
	HashSet<String>  candidateExc= null;
	HashSet<String>  remainDataItems= null;
	HashSet<LogRecord>  remainTrans= null;

	HashSet<String>  notAllowed= null;
	HashSet<String>  excluded= null;

	public void addCandidateExc(String transName){
		if(candidateExc==null)
			candidateExc= new HashSet<String>();
		candidateExc.add(transName); 
	}
	public void addExcluded(String transName){
		if(excluded==null)
			excluded= new HashSet<String>();
		excluded.add(transName); 
	}
	public void copyNotallowed(HashSet<String> n){
		if (notAllowed==null){
			notAllowed= new HashSet<String>();
		}
		notAllowed.addAll(n);
	}
	
	Node(){
		
	}
	Node(Node p){
		parent=p;
	}
	public void setEdge(String e){
		edge=e;
	}
	Node(LogRecord t){
		addTrans(t);
	}
//	Node(Transaction t, Node p){
//		parent=p;
//		addTrans(t);
//	}
	public void addTrans(LogRecord t){
		transactions.add(t);
		if (!name.isEmpty())
	     	name+="-"+t.getId();
		else
			name+=t.getId();
	}
	public String getName(){
		return name;
	}
	public void addChild(Node n){
		
		children.add(n);
		
		
	}
	
	public void addChild(Node n, String e){
		children.add(n);
		n.parent=this;
		n.edge=e;
		
		
		
	}
//	public boolean sameDataItems(Node child) {
//	HashSet<String> dataMap= getDataItemsMap();
//	int childDataSize=0;
//	for (LogRecord t:child.transactions){
//		childDataSize+=t.dataItems.size();
//	}
//		if (childDataSize!=dataMap.size())
//		return false;
//		for (Transaction t:child.transactions){
//			for (DataItem d:t.dataItems){
//				if (!dataMap.contains(d.name))
//					return false;
//			}
//		}
//		return true;
//	}
//	public HashSet<String> getDataItemsMap() {
//		HashSet<String> dataMap= new HashSet<String>();
//		for (LogRecord t:transactions){
//			for (DataItem d:t.dataItems){
//				dataMap.add(d.name);
//			}
//		}
//		return dataMap;
//	}
	public void addTrans(ArrayList<LogRecord> transactions2) {
		for(LogRecord t:transactions2){
		addTrans(t);	
		}
		
	}
	public void copyCandidateExcluded(HashSet<String> candidateExclude) {
		if(candidateExc==null)
			candidateExc= new HashSet<String>();
		for (String trans: candidateExclude){
			//CDSE.treesCounter++;
			candidateExc.add(trans); 
		}
		
	}
	public void copyRemainTrans(HashSet<LogRecord> trans) {
		if(remainTrans==null)
			remainTrans= new HashSet<LogRecord>();
		for (LogRecord t: trans){
		//	Main.treesCounter++;
			remainTrans.add(t); 
		}
		
	}
	public void copyRemainDataItems(HashSet<String> refItems) {
		if(remainDataItems==null)
			remainDataItems= new HashSet<String>();
		for (String item: refItems){
		//	CDSE.treesCounter++;
			remainDataItems.add(item); 
		}
		
	}
	public void updateRemainTrans(LogRecord t){
		remainTrans.remove(t.getId());
	}
	public String updateRemainDataItems(HashSet<String> dataItemNames) {
		String added=",";
		if(remainDataItems==null)
			remainDataItems= new HashSet<String>();
		if (dataItemNames.size()<=remainDataItems.size()){
		for ( String item: dataItemNames){
			//	CDSE.treesCounter++;
			if (remainDataItems.contains(item)){
				added+=item+",";
			remainDataItems.remove(item); 
			}
		}
		}
		
		else{
			Iterator<String> it = remainDataItems.iterator();
			while( it.hasNext()){
				String item = it.next();
				//CDSE.treesCounter++;
			if (dataItemNames.contains(item)){
				added+=item+",";
			it.remove(); 
			}
		}
		}
		return added;
		
	}
	
	public String toString(){
		return name;
	}
	public void setRemainDI(HashSet<String> items) {
		if (remainDataItems==null)
			remainDataItems= new HashSet<String>();
		for (String d: items){
			//CDSE.treesCounter++;
			remainDataItems.add(d);
		}
		
	}
	public void setRemainTrans(ArrayList<LogRecord> input, LogRecord trans) {
		if(remainTrans==null)
			remainTrans= new HashSet<LogRecord>();
		for (LogRecord t:input){
			if(t!=trans){
				remainTrans.add(t);
			}
			
		}
		
	}
	public void updateNotAllowed(HashMap<String, HashMap<String, Boolean>> notAllowedV) {
		if (notAllowed==null){
			notAllowed= new HashSet<String>();
		}
		HashMap<String, Boolean> listB = notAllowedV.get(this.name+"After");
		if (listB==null)
			return;
		for (String rid: listB.keySet()){
			if (listB.get(rid)!=null &&listB.get(rid)==true){
				notAllowed.add(rid);
			}
		}
		
	}
}
