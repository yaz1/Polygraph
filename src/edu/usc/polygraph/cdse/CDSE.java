package edu.usc.polygraph.cdse;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.usc.polygraph.Entity;
import edu.usc.polygraph.EntitySpec;
import edu.usc.polygraph.LogRecord;
import edu.usc.polygraph.Property;
import edu.usc.polygraph.Schedule;
import edu.usc.polygraph.ScheduleList;
import edu.usc.polygraph.ValidationParams;
import edu.usc.polygraph.Validator;
import edu.usc.polygraph.cdse.graph.Edge;
import edu.usc.polygraph.cdse.graph.Graph;
import edu.usc.polygraph.cdse.graph.Vertex;



public class CDSE {
	public static final int NO_CONTAINMENT = -1;
	public static final int EQUAL = 0;
	public static final int CONTAIN_A = 1;
	public static final int CONTAIN_B = 2;
	private static final String SEPERATOR = ",";
//	public static long graphCounter=0;
//	public static long treesCounter=0;
//	public static long pathsCounter=0;
//	public static long permCounter=0;
//	public static long testCounter=0;
//	public static long valuesPermCounter=0;
//	public static long valuesAlgCounter=0;
	public final static boolean CHECK_DELTA=false;
	public final static boolean CHECK_TIME=true;
	public final static boolean REMOVE_EDGES_TIME=true;
	public final static boolean CHECK_NOT_ALLOWED=true;




	public static void main(String[] args) {
		
		String resultFile="C:\\Users\\yaz\\Documents\\Permutation\\result.txt";
		File file = new File(resultFile);
		try {
			 Files.deleteIfExists(file.toPath());
		} catch (IOException e) {
			
			e.printStackTrace();
		} 
		 File dir = new File(args[0]);
		  File[] directoryListing = dir.listFiles();
		  Arrays.sort(directoryListing);
		  if (directoryListing != null) {
		    for (File child : directoryListing) {
		    	String fileName=child.getAbsolutePath();
		    	
				run(fileName, resultFile);
				//System.out.println("Test Counter="+testCounter);
		    }
		

	}
	}
	private static void run(String fileName, String resultFile) {}


	static HashSet<String> getCommonDI(Validator v,LogRecord transaction1,
			LogRecord transaction2, HashMap<LogRecord, HashSet<String>> transItems) {
		HashSet<String> common=v.commonDI.get(transaction1.getId()+"-"+transaction2.getId());
		if (common!=null){
			return common;
		}
				common=v.commonDI.get(transaction2.getId()+"-"+transaction1.getId());
				if (common!=null){
					return common;
				}
		common= new HashSet<>();
		for (String d1:transItems.get(transaction1)){
			//CDSE.treesCounter++;
			if (transItems.get(transaction2).contains(d1))
				common.add(d1);
		}
		v.commonDI.put(transaction1.getId()+"-"+transaction2.getId(), common);
		return common;
	}
	private static void resetWithoutFact(Validator v) {
		v.commonDI.clear();
		v.overlapMap.clear();
//		graphCounter=0;
//		treesCounter=0;
//		pathsCounter=0;
//		testCounter=0;
//		valuesAlgCounter=0;
		
	}

	


	

	private static ArrayList<HashMap<String, EntitySpec>>  getPathsValues(ArrayList<ArrayList<LogRecord>> paths, HashMap<String, EntitySpec>  skipped,int dataItemsCount) {
		ArrayList<HashMap<String, EntitySpec>>  values= new ArrayList<HashMap<String, EntitySpec>>();
		LogRecord dummy=createLogfromEntitySpec(skipped);
		if (dummy!=null){
		dummy.setActionName("Dummy");
		dummy.setId("Dummy");
//		if (dummy.getEntities()==null)
//			System.out.println();
	}
		for (ArrayList<LogRecord> input: paths){
			if (dummy!=null){
			input.add(dummy);
			}
//		ArrayList<String> schedules = new ArrayList<String>();
//		schedules.add(getSchedule(input));
		values.add(getValues(input,dataItemsCount,true));//schedules);
		
		}
		
		return values;
	}

	private static LogRecord createLogfromEntitySpec(HashMap<String, EntitySpec> espMap) {
		if (espMap.isEmpty())
			return null;
		LogRecord dummy=new LogRecord();
		Entity[] entities = new Entity[espMap.size()];
		int i=0;
		for (EntitySpec esp: espMap.values()){
		entities[i]= esp;
		Property[] props = entities[i].getProperties();
		props = new Property[esp.getPropertiesArrayLis().size()];
		props = esp.getPropertiesArrayLis().toArray(props);
		entities[i].setProperties(props);
		esp.getPropertiesArrayLis().clear();
		esp.setPropertiesArrayLis(null);

		i++;
		}
		
					
		dummy.setEntities(entities);
					
		return dummy;
	}
	private static boolean isRefByOthersProps(ArrayList<LogRecord> input, String pkey, int skip, HashMap<LogRecord, HashSet<String>> transItems) {
		for (int i=0;i<input.size();i++){
			if (i==skip)
				continue;
			LogRecord t= input.get(i);
			if (transItems.get(t).contains(pkey))
				return true;
		}
		return false;
	}
	
	private static int removeOneValueProps(HashMap<String,EntitySpec> skipped,ArrayList<LogRecord> input, HashMap<LogRecord, HashSet<String>> transItems, HashSet<String> items) {
		int removedCount=0;
		HashSet<String> checked=new HashSet<String>();
		for (int i=0;i<input.size();i++){
			LogRecord t= input.get(i);
			Entity[] entities = t.getEntities();
			for (Entity e: entities){
				Property[] props = e.getProperties();
				for (int j=0; j< props.length;j++){
					Property p=props[j];
					String pkey=p.getProprtyKey(e);
				if (checked.contains(pkey))
					continue;
				checked.add(pkey);
				if (!isRefByOthersProps(input,pkey,i,transItems)){
					String ekey=e.getEntityKey();
					EntitySpec entitySp = skipped.get(ekey);
					if (entitySp==null){
						entitySp= new EntitySpec(e.getKey(), e.getName(),null);
						skipped.put(ekey,entitySp );
					}
					ArrayList<Property> parry = entitySp.getPropertiesArrayLis();
					if (parry==null){
						parry= new ArrayList<Property>();
						entitySp.setPropertiesArrayLis(parry);
					}
					parry.add(p);
					
					items.remove(pkey);
					props[j]=null;
					removedCount++;
					transItems.get(t).remove(pkey);
				}
			}
			}
		}
		return removedCount;
		
	}

	

	private static ArrayList<Tree> constructTrees(Validator validator,ArrayList<LogRecord> input, Graph<LogRecord> g, HashSet<String> items, long maxStart,HashMap<LogRecord, HashSet<String>> transItems, HashMap<String, HashMap<String, Boolean>> notAllowedList) {
		ArrayList<Tree> treeList= new ArrayList<Tree>();
	
		
		for (LogRecord trans: input){
			
			Node root= new Node(trans);
			root.setRemainDI(items);
			
			if (CHECK_TIME){
				// if r.commit < max start of all transactions, we discard the tree.
				root.setRemainTrans(input,trans);
				if (trans.getEndTime()< maxStart)
					continue;
			}
			boolean validTree=true;
			if(CHECK_NOT_ALLOWED){
				root.updateNotAllowed(notAllowedList);
				for (LogRecord t:root.remainTrans){
					if (root.notAllowed.contains(t.getId())){
						validTree=false;
						break;
					}
					
				}
			}
			if(!validTree)
				continue;
			Tree tree=new Tree(root);
			treeList.add(tree);
			
			Vertex<LogRecord> v = g.findVertexByName(trans.getId());
			root.updateRemainDataItems(transItems.get(trans));
		
			root.copyCandidateExcluded(v.getCandidateExclude());
			root.setCommit(trans.getEndTime());
			buildTree(validator, tree,g,root, v,transItems,notAllowedList);
				
				
				
		
		}

		return treeList;
	}

	private static void buildTree(Validator validator,Tree tree, Graph<LogRecord> g, Node n, Vertex<LogRecord> v, HashMap<LogRecord, HashSet<String>> transItems, HashMap<String, HashMap<String, Boolean>> notAllowedList) {
		if(n.remainDataItems.size()==0){
		//	treesCounter++;
			return;
		}
		
		//Vertex<Transaction> v = g.findVertexByName(n.name);
		
		for(int o=0; o< v.getOutgoingEdgeCount();o++){
			Edge<LogRecord> out = v.getOutgoingEdge(o);
			Vertex<LogRecord> toV = out.getTo();
			LogRecord toTrans = toV.getData();
			if (CHECK_NOT_ALLOWED&& n.notAllowed.contains(toTrans.getId()))
				continue;
			if(CHECK_TIME){
			//we check C of parent < start time of transaction of M. If check succeeds, we do not add it as a child.
				if (n.getCommit()< toTrans.getStartTime())
					continue;
			}
			if (n.candidateExc.contains(toTrans.getId())){
				if (!isOverlapNodes(toTrans,n,transItems)){ //toTrans.isOverlap(n.transactions.get(0))
					continue;
				}
			}
			
			Node child= new Node(toTrans);
			child.copyRemainDataItems(n.remainDataItems);
			
			String edge=child.updateRemainDataItems(transItems.get(toTrans));
			if (edge.equals(","))
				continue;
			if (CHECK_TIME){
				if (CHECK_NOT_ALLOWED){
					child.copyNotallowed(n.notAllowed);
					child.updateNotAllowed(notAllowedList);
					
				}
				if (!checkValidChild(child,n.remainTrans, toTrans, edge,transItems))
					continue;
				child.copyRemainTrans(n.remainTrans);
				child.updateRemainTrans(toTrans);
			}
			if (n!= tree.root&&v.getBiDirectVertices().containsKey(child.name)){
				HashSet<String>common=getCommonDI(validator,n.transactions.get(0),child.transactions.get(0),transItems);
				
				if (!edgeRefCommon(common,n)){
					if (n.parent.excluded !=null && n.parent.excluded.contains(child.name) )
						continue;
					n.parent.addExcluded(n.name);
				
				}
				
			}
			
			child.copyCandidateExcluded(toV.getCandidateExclude());
			child.copyCandidateExcluded(n.candidateExc);
			child.setCommit(Math.min(n.getCommit(), toTrans.getEndTime()));
			n.addChild(child, edge);
			tree.noNodes++;
			buildTree(validator,tree,g,child,toV,transItems,notAllowedList);
			
			
			
		}
		
		
	}

	private static boolean isOverlapNodes(LogRecord toTrans, Node n, HashMap<LogRecord, HashSet<String>> transItems) {

		
		for (String d:transItems.get(toTrans)){
		//	CDSE.treesCounter++;
			if (!n.remainDataItems.contains(d)){
				return true;
			}
			
		}
		return false;
//	while (n!=null){
//		if(this.isOverlap(n.transactions.get(0))){
//			return true;
//		}
//		n=n.parent;
//	}
//	return false;
	
	}
	private static boolean checkValidChild(Node child,HashSet<LogRecord> remainTrans, LogRecord toTrans, String edge, HashMap<LogRecord, HashSet<String>> transItems) {
		//we  check if a remaining trans overlap with a data item established by child child and start after it commits, do not add the child
		String []edgeData= edge.split(",");
		for (String data: edgeData){
			for (LogRecord t: remainTrans){
				if (t!=toTrans){
					if (transItems.get(t).contains(data)){
						if (t.getStartTime()> toTrans.getEndTime())
							return false;
						if (CHECK_NOT_ALLOWED){
							if (child.notAllowed.contains(t.getId()))
								return false;
							
						}
					}
					
				}
				
			}
		}
		return true;
	}
	private static boolean edgeRefCommon(HashSet<String> common, Node n) {
		
		String []edgeData= n.edge.split(",");
		 //references a data items(s) belonging to the list n minus c data items:
		for (String data: edgeData){
		//	CDSE.treesCounter++;
			if (common.contains(data))
				return true;
		}
		return false;
	}

	
	

	
	
	public static boolean isValidSchedule(ArrayList<LogRecord> result) {
		long maxStart=0;
		for (int i=0;i<result.size()-1;i++){
			LogRecord trana = result.get(i);
			LogRecord tranb = result.get(i+1);
			if (trana.getStartTime()>maxStart){
				maxStart=trana.getStartTime();
			}
			if (maxStart> tranb.getEndTime())
				return false;
		}
		return true;
	}
	static boolean isOverlapGraph(Validator validator,LogRecord t1,LogRecord t2, HashMap<LogRecord, HashSet<String>> transItems){
		if (transItems.get(t2).size()<= transItems.get(t1).size()){
			HashSet<String> dMap = transItems.get(t1);
			for (String d:transItems.get(t2)){
				//CDSE.graphCounter++;
				if (dMap.contains(d)){
					validator.overlapMap.put(t1.getId()+"-"+t2.getId(), true);
					return true;
				}
			}
			}
			
			else{
				HashSet<String> dMap = transItems.get(t2);
				for (String d:transItems.get(t1)){
				//	CDSE.graphCounter++;
					if (dMap.contains(d)){
						validator.overlapMap.put(t1.getId()+"-"+t2.getId(), true);
						validator.overlapMap.put(t2.getId()+"-"+t1.getId(), true);


						return true;
					}
				}
			}
		validator.overlapMap.put(t1.getId()+"-"+t2.getId(), false);
		validator.overlapMap.put(t2.getId()+"-"+t1.getId(), false);


			return false;
	}
	
	static int containment(LogRecord t1,LogRecord t2, HashMap<LogRecord, HashSet<String>> transItems){
		if (transItems.get(t1).size()==transItems.get(t2).size()){
			HashSet<String> dMap = transItems.get(t1);
			for (String d:transItems.get(t2)){
			//	CDSE.graphCounter++;
				if (!dMap.contains(d))
					return NO_CONTAINMENT;
			}
			return EQUAL;
		}
		else if (transItems.get(t1).size()>transItems.get(t2).size()){
			HashSet<String> dMap = transItems.get(t1);
			for (String d:transItems.get(t2)){
			//	CDSE.graphCounter++;
				if (!dMap.contains(d))
					return NO_CONTAINMENT;
			}
			return CONTAIN_A;
		}
		
		else{ // t is larger
			HashSet<String> dMap = transItems.get(t2);
			for (String d:transItems.get(t1)){
				//CDSE.graphCounter++;
				if (!dMap.contains(d))
					return NO_CONTAINMENT;
			}
			return CONTAIN_B;
			
		}
		
		
	}

private static Graph<LogRecord> generateGraphLogs(Validator validator,ArrayList<LogRecord> input, HashMap<LogRecord, HashSet<String>> transItems) {
	Graph<LogRecord> g= new Graph<LogRecord>();
	for(int i=0; i<input.size();i++){
		LogRecord t1= input.get(i);
		if (!g.getVerticiesNames().contains(t1.getId())){
			Vertex<LogRecord> v= new Vertex<LogRecord>(t1.getId(),t1);
			g.addVertex(v);
		}
		for (int j=i+1; j< input.size();j++){
			//graphCounter++;
			LogRecord t2= input.get(j);
			if (!g.getVerticiesNames().contains(t2.getId())){
				Vertex<LogRecord> v= new Vertex<LogRecord>(t2.getId(),t2);
				g.addVertex(v);
			}
			if (CDSE.isOverlapGraph(validator, t1,t2,transItems) ){
				int result=CDSE.containment(t1,t2, transItems);
				if (result==EQUAL){
					
					continue;
				}
			
				if (result==CONTAIN_A){
					 //Create an edge from G.nodes(j)  to G.nodes(i)
					if (CHECK_TIME && REMOVE_EDGES_TIME){
						if (t1.getStartTime()>t2.getEndTime() )
							continue;
					}
					Vertex<LogRecord> v1 = g.findVertexByName(t1.getId());
					Vertex<LogRecord> v2 = g.findVertexByName(t2.getId());
					g.addEdge(v2, v1, 0);
					
			
				}
				else if (result==CONTAIN_B){
			     //Create an edge from G.nodes(i)  to G.nodes(j)
					if (CHECK_TIME && REMOVE_EDGES_TIME){
						if (t2.getStartTime()>t1.getEndTime() )
							continue;
					}
					Vertex<LogRecord> v1 = g.findVertexByName(t1.getId());
					Vertex<LogRecord> v2 = g.findVertexByName(t2.getId());
					g.addEdge(v1, v2, 0);
				}
				else{
			  // - Create an edge from G.nodes(i)  to G.nodes(j) and an edge from  G.nodes(j)  to G.nodes(i) 
					Vertex<LogRecord> v1 = g.findVertexByName(t1.getId());
					Vertex<LogRecord> v2 = g.findVertexByName(t2.getId());
					if (CHECK_TIME && REMOVE_EDGES_TIME){
						if (t1.getStartTime()>t2.getEndTime() ){
							g.addEdge(v1, v2, 0);
						}
						else if (t2.getStartTime()>t1.getEndTime() ){
							g.addEdge(v2, v1, 0);
						}
						else{
							g.addEdge(v1, v2, 0);
							g.addEdge(v2, v1, 0);
							v1.addBiDirect(v2);
							v2.addBiDirect(v1);
						}
					}
					else{
					g.addEdge(v1, v2, 0);
					g.addEdge(v2, v1, 0);
					v1.addBiDirect(v2);
					v2.addBiDirect(v1);
					}
				}
				}
			else{ // not overlap
				//- Create an edge from G.nodes(i)  to G.nodes(j)
				Vertex<LogRecord> v1 = g.findVertexByName(t1.getId());
				Vertex<LogRecord> v2 = g.findVertexByName(t2.getId());
				if (CHECK_TIME && t2.getEndTime() >  t1.getEndTime() ){
					//we choose the node with max commit time to be the source of edge between two overlapping nodes

					g.addEdge(v2, v1, 0);
					v1.addCandidateExc(t2.getId());
				}
				else{
				g.addEdge(v1, v2, 0);
				v2.addCandidateExc(t1.getId());
				}
			}
			
			                                
		}
		
		
	}
	return g;
}

	

//	private static ArrayList<ArrayList<LogRecord>> getPossibleSchedules(ArrayList<Node> path) {
//		ArrayList<ArrayList<LogRecord>> scheduleList= new ArrayList<ArrayList<LogRecord>>();
//		ArrayList<LogRecord> schedule= new ArrayList<LogRecord>();
//		scheduleList.add(schedule);
//		for (int j=0;j<path.size();j++){
//			Node n=path.get(j);
//			ArrayList<ArrayList<Transaction>>dupList= new ArrayList<ArrayList<Transaction>>();
//			HashSet<String> nodeDataItems= new HashSet<String>();
//			boolean check=false;
//			boolean remainingHasAll=false;
//			if (n.transactions.size()>1){
//				check=true;
//				nodeDataItems= n.getDataItemsMap();
//			}
//			boolean doneWithAllTrans=false;
//			for (int i=0; i<n.transactions.size();i++){
//				if (doneWithAllTrans)
//					break;
//				LogRecord trans = n.transactions.get(i);
//				for (int schIndex=0;schIndex<scheduleList.size();schIndex++){
//					ArrayList<Transaction> s = scheduleList.get(schIndex);
//					if (i==n.transactions.size()-1){
//						// last trans
//						s.add(trans);
//						if (n.transactions.size()!=1){
//							Transaction.addOtherTrans(s,n.transactions,i);
//						}
//					}
//					else{ // first trans if has more than trans
//						if (check){
//							remainingHasAll=remaingHasAllData(path,j+1,nodeDataItems);
//						}
//						check=false;
//						if (remainingHasAll){ 
//							s.add(trans);
//							assert n.transactions.size()!=1:"Expecting more than one trans";
//								Transaction.addOtherTrans(s,n.transactions,i);
//							
//								doneWithAllTrans=true;
//								
//							
//							continue;
//						} // done remaining has all
//						
//						ArrayList<Transaction> dupSchedule= Transaction.duplicateSchedule(s);
//						dupList.add(dupSchedule);
//						dupSchedule.add(trans);
//						if (n.transactions.size()!=1){
//							Transaction.addOtherTrans(dupSchedule,n.transactions,i);
//						}
//
//					}
//				} // done with schedules
//			} // done with all trans
//			// add duplicate schedules
//			scheduleList.addAll(dupList);
//		}
//
//		return scheduleList;
//	}



	

	private static String getPathStr(ArrayList<Node> path) {
		String pathStr="";
		for (Node n: path){
			pathStr+=n.getName()+SEPERATOR;
		}
		pathStr=pathStr.substring(0, pathStr.length()-1);
		return pathStr;
	}

	



	private static boolean isNumeric(String string) {
		try{
		Long.parseLong(string);
		return true;

		}

		catch(Exception ex){
			return false;
		}
	}


	





	

	public static HashMap<String, EntitySpec> getValues(ArrayList<LogRecord> input, int dataItemsCount, boolean alg) {

		//SortedMap <String,String>values= new TreeMap<String,String>();
		HashMap<String, EntitySpec> espmap= new HashMap<String, EntitySpec>();
		int numProps=0;
		for (int i= input.size()-1; i>=0; i--){
			LogRecord t = input.get(i);
//			if (t.getEntities()==null)
//				System.out.println();
			for (Entity e:t.getEntities()){
			for (Property p:e.getProperties()){
				if (p==null || p.getType()!=ValidationParams.NEW_VALUE_UPDATE)
					continue;
				numProps++;
				String ekey=e.getEntityKey();
				EntitySpec entitySp = espmap.get(ekey);
				if (entitySp==null){
					entitySp= new EntitySpec(e.getKey(), e.getName(),null);
					espmap.put(ekey,entitySp );
				}
				ArrayList<Property> parry = entitySp.getPropertiesArrayLis();
				if (parry==null){
					parry= new ArrayList<Property>();
					entitySp.setPropertiesArrayLis(parry);
				}
				parry.add(p);
				
			}
			if (numProps==dataItemsCount)
				break;
		}
		}
	
		return espmap;
	}
	public static ArrayList<LogRecord> runAlg(Validator validator, LogRecord read, HashMap<String, HashMap<String, Boolean>> notAllowedList,
			ArrayList<LogRecord> intervals) {
//		if (read.getId().equals("89-19991")|| read.getId().equals("90-14526"))
//			System.out.println();
		
		if (checkDelta(intervals)){
			return null;
		}
		ArrayList<LogRecord> myLogs= new ArrayList<LogRecord>();
		myLogs.addAll(intervals);
		getNonOverlappings(read,myLogs);
		if (intervals.size()-myLogs.size()>2){
			validator.factUsed=true;
		}
		if (myLogs.size()<4){
			
			return null;
		}
//		if (myLogs.size()<4)
//			System.out.println();
	//	debugEntities(intervals,read);
		validator.cdseUsed=true;
		intervals.removeAll(myLogs);
		ArrayList<LogRecord> values=runLogs(myLogs, validator,read,notAllowedList);
		//ArrayList<LogRecord> values= new ArrayList<LogRecord>();
		return values;
		
	}
	private static void debugEntities(ArrayList<LogRecord> intervals, LogRecord r) {
		for (LogRecord log: intervals){
			for (Entity e: log.getEntities()){
				if (e.getEntityKey().equals("CUS-32-6-1530"))
					System.out.println("Read:"+r.getId());
				
			}
			
		}
		
	}
	private static boolean checkDelta(ArrayList<LogRecord> intervals) {
		for (LogRecord log:intervals){
			if (log.getActionName().equalsIgnoreCase("DE"))
				return true;
		}
		return false;
	}
	private static ArrayList<LogRecord> runLogs(ArrayList<LogRecord> input, Validator validator, LogRecord read, HashMap<String, HashMap<String, Boolean>> notAllowedList) {
		Graph<LogRecord> g=null;
		short group=input.get(0).getGroupNum();
		HashSet<String> items = new HashSet<String>();
		HashMap<LogRecord, HashSet<String>> transItems= new HashMap<LogRecord, HashSet<String>>();
		long maxStart=generateDTs(input,items,transItems);
		int orgDICount=items.size();
		HashMap<String,EntitySpec> skipped= new HashMap<String,EntitySpec> ();

		 removeOneValueProps(skipped,input,transItems, items);
//		 if (skipped.size()>0)
//			 System.out.println();
		ArrayList<HashMap<String, EntitySpec>> treesValues=null;
		
		
	
		
	
		
		resetWithoutFact(validator);
		g= generateGraphLogs(validator,input,transItems);
		
		
		ArrayList<Tree> treeList= constructTrees(validator,input,g,items,maxStart,transItems,notAllowedList);
	
		ArrayList<ArrayList<LogRecord>> paths = new ArrayList<ArrayList<LogRecord>>();
		
		for (Tree t1:treeList){
//			if (read.getId().equals("0-655"))
//			t1.visualize();
			t1.findPathsDFS(input.size(), paths,input);
			
			
		}
			treesValues = getPathsValues(paths,skipped,orgDICount);
			ArrayList<LogRecord> records= getRecordsFromValues(treesValues,validator,read,group);
		
		return records;
	}
	
	private static ArrayList<LogRecord> getRecordsFromValues(ArrayList<HashMap<String, EntitySpec>> treesValues,
			Validator validator, LogRecord read, short group) {
		ArrayList<LogRecord> records= new ArrayList<LogRecord>();
		for (HashMap<String, EntitySpec> espMap: treesValues){
			LogRecord log = createLogfromEntitySpec(espMap);
			log.setGroupNum(group);
			log.setActionName("CDSE");
			log.setId("CDSE-"+validator.csdseLogCounter++);
			log.setStartTime(read.getStartTime()-20);
			log.setEndTime(read.getStartTime()-10);
			records.add(log);
		}
		
		return records;
	}
	private static long generateDTs(ArrayList<LogRecord> input, HashSet<String> items, HashMap<LogRecord, HashSet<String>> transItems) {
		long maxStart=0;
		for (LogRecord r:input){
			if (r.getStartTime()>maxStart)
				maxStart=r.getStartTime();
			HashSet<String> tItems = new HashSet<String>();
			transItems.put(r, tItems);
			for (Entity e:r.getEntities()){
				for (Property p:e.getProperties()){
					if (p==null || p.getType()!=ValidationParams.NEW_VALUE_UPDATE){
						continue;
					}
					String pkey= p.getProprtyKey(e);
					items.add(pkey);
					tItems.add(pkey);
					
				}
				
			}
			
		}
		return maxStart;
		
	}
	private static void getNonOverlappings(LogRecord read, ArrayList<LogRecord> myLogs) {
		ArrayList<LogRecord> overLapping= new ArrayList<LogRecord>();
		overLapping.add(read);
	//	Iterator<LogRecord> itOver = overLapping.iterator();
		for (int i=0; i< overLapping.size();i++){
			LogRecord over = overLapping.get(i);
			Iterator<LogRecord> itmy = myLogs.iterator();
			while(itmy.hasNext()){
				LogRecord my = itmy.next();
				if (my.equals("2-13734"))
					System.out.println();
				if (my.getEndTime()> read.getStartTime()||(my.overlap(over) && my.intersect(over))){
					itmy.remove();
					overLapping.add(my);
				}
			}
			
		}
		
	}
	public static void combineSchedule(ArrayList<LogRecord> cdseLogs, ScheduleList ss) {
		
		if (ss.schedules.isEmpty()){
			for(int i=0; i< cdseLogs.size();i++){
				LogRecord log = cdseLogs.get(i);
				Schedule schedule = new Schedule();
				schedule.add(log);
				ss.add(schedule);
			}
		}
		else{
			ArrayList<Schedule>myNewSS= new ArrayList<Schedule>();
		for(int j=0; j<ss.schedules.size();j++){
			Schedule schedule = ss.schedules.get(j);
			for(int i=0; i< cdseLogs.size();i++){
				LogRecord log = cdseLogs.get(i);
				if (i==cdseLogs.size()-1){
					// no duplicate
					schedule.getRecords().add(0, log);
				}
				else{
					Schedule dup= new Schedule();
					dup.add(log);
					dup.getRecords().addAll(schedule.getRecords());
					//ss.add(dup);
					myNewSS.add(dup);
				}
				
			}
			
		}
		ss.addAll(myNewSS,true);
		
		
	}
	}

}

