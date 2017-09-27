package edu.usc.polygraph.codegenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.usc.polygraph.Entity;
import edu.usc.polygraph.Property;
import edu.usc.polygraph.ValidationParams;

public class CodeGeneratorFunctions {


	public static HashMap<Transaction, HashSet<String>>  buildHashmap(ArrayList<Transaction> trans) throws Exception{

		HashMap<Transaction, HashSet<String>>  transHash= new HashMap<Transaction,HashSet<String>>();
		for (Transaction t: trans){
			HashSet <String>info= new HashSet<String>();
			transHash.put(t, info);
			for (Entity e:t.entities){


				for (Property p:e.getProperties()){
					char type='*';
					switch (p.getType()) {
					case ValidationParams.NEW_VALUE_UPDATE:
					case ValidationParams.DECREMENT_UPDATE_INTERFACE:
					case ValidationParams.VALUE_DELETED:
					case ValidationParams.INCREMENT_UPDATE:
						type = ValidationParams.UPDATE_RECORD;
						break;
					case ValidationParams.VALUE_READ:
					case ValidationParams.VALUE_NA:
						type = ValidationParams.READ_RECORD;
						break;
					}
					if (type=='*'){
						throw new Exception("Error:Unrecognized property type");
					}
					String key= e.getName()+ValidationParams.KEY_SEPERATOR+p.getName()+ValidationParams.KEY_SEPERATOR+type;
					info.add(key);

				}
			}
		}

		return transHash;

	}

	public static void getTopics(ArrayList<Transaction>  trans,String topic){
		HashSet<Transaction> visited= new HashSet<Transaction>();
		int count=1;
		for (Transaction t:trans){
			if(t.topic==null){
				t.topic=topic+"-"+count;
				count++;

			}
			search(t,visited);
		}

	}
	public static String readFile(String filename) {
		String result = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				line = br.readLine();
			}
			result = sb.toString();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	private static void search(Transaction t, HashSet<Transaction> visited) {
		if (!visited.contains(t)){
			visited.add(t);
			if (t.conflicted==null){

				return;
			}
			for (ConflictedTransaction nextTrans: t.conflicted){
				if (!visited.contains(nextTrans.transaction)){

					nextTrans.transaction.topic=t.topic;
					search(nextTrans.transaction, visited);
				}
			}
		}
	}

	public static  ArrayList<Transaction> parseTrans(ArrayList<String> jsonStrings){
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();

		for (String jsonString:jsonStrings)
		{ 



			JSONObject obj = new JSONObject(readFile(jsonString));
			JSONArray elements = obj.getJSONArray("Elements");
			String tname=obj.getString("Name");
			Transaction trans= new Transaction(tname);
			transactions.add(trans);
			trans.entities= new ArrayList<Entity>();
			trans.relationships= new ArrayList<String>();
			trans.propsVars= new HashMap<String,String>();
			for (int i = 0; i < elements.length(); i++) {
				JSONObject element = (JSONObject) elements.get(i);
				if (element.getString("elementType").equals("Entity")){
					int eid= element.getInt("eid");
					JSONArray sets = element.getJSONArray("sets");

					for (int j = 0; j < sets.length(); j++) {


						JSONObject set = (JSONObject) sets.get(j);
						//String setType= set.getString("type");
						JSONArray pks = set.getJSONArray("pks");
						for (int p = 0; p < pks.length(); p++) {
							JSONObject pk = (JSONObject) pks.get(p);
							int pid=pk.getInt("pid");
							String varName=pk.getString("variable");
							trans.propsVars.put("E-"+j+"-"+eid+"-"+pid, varName);
						}

						JSONArray propsArr = set.getJSONArray("properties");
						Property[] properties= new Property[propsArr.length()];
						for (int p = 0; p < propsArr.length(); p++) {
							JSONObject prop = (JSONObject) propsArr.get(p);
							int pid=prop.getInt("pid");
							String varName=prop.getString("variable");
							trans.propsVars.put("E-"+j+"-"+eid+"-"+pid, varName);
							char propertyType=prop.getString("type").charAt(0);
							properties[p]= new Property(String.valueOf(pid), varName, propertyType);
						}
						Entity e= new Entity(null, String.valueOf(eid), properties);
						trans.entities.add(e);

					} // end set loop

				} 
				else  if (element.getString("elementType").equals("Relationship")){
					String rid= String.valueOf(element.getInt("eid"));
					trans.relationships.add(rid);

				}
			}// elements loop

		} //json file loop
		return transactions;

	}

	public static void parseEr(String jsonString, HashMap<String, Entity> entities,HashMap<String, Property> props ){
		JSONObject obj = new JSONObject(readFile(jsonString));
		JSONArray elements = obj.getJSONArray("Entities");

		int entityCount=0;
		//int relationCount=0;
		for (int i = 0; i < elements.length(); i++) {
			JSONObject element = (JSONObject) elements.get(i);
			if (element.getString("type").equals("Entity")){

				String eid="E-"+entityCount;
				entityCount++;
				String ename=element.getString("name");
				String ekey="";
				JSONArray propsArr = element.getJSONArray("properties");
				for (int j = 0; j < propsArr.length(); j++) {
					JSONObject prop = (JSONObject) propsArr.get(j);
					boolean pk=prop.getBoolean("pk");
					int pid=j;
					String pname=prop.getString("name");
					String ptype=prop.getString("type");


					if(pk){
						ekey=ekey+pid+"-";
					}
					Property p=new Property(pname,ptype , 'A');
					props.put(eid+"-"+pid, p);

				}
				ekey=ekey.substring(0, ekey.length()-1); // remove last char
				Entity e= new Entity(ekey, ename, null);
				entities.put(String.valueOf(eid), e);

			}
		}



	}
	public static void removeUnconflictProps(ArrayList<Transaction> trans,HashMap<Transaction, HashSet<String>> hashTrans ) throws Exception{

		for (int tIndex=trans.size()-1; tIndex >=0; tIndex--){
			Transaction t= trans.get(tIndex);

			for (int eIndex=t.entities.size()-1; eIndex>=0;eIndex--)
			{
				Entity e=t.entities.get(eIndex);

				Property[] propsArray = e.getProperties();
				if (propsArray.length==0){
					t.entities.remove(eIndex);

				}
				for (int pIndex=propsArray.length-1; pIndex>=0;pIndex--)
				{
					Property p= e.getProperties()[pIndex];
					char type='*';
					switch (p.getType()) {
					case ValidationParams.NEW_VALUE_UPDATE:
					case ValidationParams.DECREMENT_UPDATE_INTERFACE:
					case ValidationParams.VALUE_DELETED:
					case ValidationParams.INCREMENT_UPDATE:
						type = ValidationParams.UPDATE_RECORD;
						break;
					case ValidationParams.VALUE_READ:
					case ValidationParams.VALUE_NA:
						type = ValidationParams.READ_RECORD;
						break;
					}
					if (type=='*'){
						throw new Exception("Error:Unrecognized property type");
					}

					String key= e.getName()+ValidationParams.KEY_SEPERATOR+p.getName();
					boolean r= buildConflict(t,hashTrans, key,type);
					if (!r){
						propsArray[pIndex]=propsArray[propsArray.length-1];
						e.setProperties(Arrays.copyOfRange(propsArray, 0, propsArray.length-1));
						propsArray=e.getProperties();
					}
					if (propsArray.length==0){
						//t.entities.remove(eIndex);
						break;
					}
				} // property loop

				if (t.entities.size()==0){
					trans.remove(tIndex);
					break;
				}
			}// entity loop


		}// trans loop

	}



	private static boolean buildConflict(Transaction t,HashMap<Transaction, HashSet<String>> hashTrans, String key, char type) {
		boolean result=false;
		if (type==ValidationParams.READ_RECORD)
			type= ValidationParams.UPDATE_RECORD;
		else
			type=ValidationParams.READ_RECORD;

		String searchKey=key+ValidationParams.KEY_SEPERATOR+type;

		for ( Transaction trans:hashTrans.keySet()){
			HashSet<String> h=hashTrans.get(trans);
			if (h.contains(searchKey)){
				result=true;
				if (t.conflicted==null)
					t.conflicted= new ArrayList<ConflictedTransaction>();

				//if (!t.name.equals(trans.name)){
				t.conflicted.add(new ConflictedTransaction(trans, key));
				//}
			}
		}

		return result;
	}


	public static void main(String[] args) {


		HashMap<String, Entity> entities= new HashMap<String,Entity>();
		HashMap<String, Property> props= new HashMap<String,Property>();


		try {

			String application="TPCC";
			String erFile="/home/mr1/tpccApp/TPC-C.txt";
			ArrayList<String> transFiles= new ArrayList<String>();
			transFiles.add("/home/mr1/tpccApp/TPC-C_Delivery.txt");
			transFiles.add("/home/mr1/tpccApp/TPC-C_Payment.txt");
			transFiles.add("/home/mr1/tpccApp/TPC-C_OrderStatus.txt");
			

			ArrayList<Transaction> transactions2=prepareDTs(erFile,transFiles,entities,props,application);
			for (Transaction t:transactions2){
				System.out.println(t);
				String entityArrName="transEntities";
				String entityCode=generateEntitiesCode(entityArrName,t,entities,props);
				int numPartitions=1;
				String logRecordCode=generateLogRecordCode(entityArrName,t,numPartitions);
				String startCode=generateStartLogRecordCode(entityArrName, t, numPartitions);
				System.out.println("Entity Code:");
				System.out.println(entityCode);
				System.out.println("Start Code:");
				System.out.println(startCode);
				System.out.println("Log Code:");
				System.out.println(logRecordCode);
				System.out.println("======================================");
				
			}

			System.exit(0);


		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}







		//		Transaction t1= new Transaction("t1");
		//
		//		t1.entities= new ArrayList<Entity>();
		//		Property[] properties= new Property[2];
		//		properties[0]= new Property("p1", null, ValidationConstants.NEW_VALUE_UPDATE);
		//		properties[1]= new Property("p1", null, ValidationConstants.VALUE_READ);
		//
		//		Entity member= new Entity(null, "MEMBER", properties);
		//		t1.entities.add(member);
		//		
		//		
		//		Transaction t2= new Transaction("t2");
		//
		//		
		//		t2.entities= new ArrayList<Entity>();
		//		Property[] properties2= new Property[2];
		//		properties2[0]= new Property("p1", null, ValidationConstants.NEW_VALUE_UPDATE);
		//		properties2[1]= new Property("p2", null, ValidationConstants.VALUE_READ);
		//
		//		Entity member2= new Entity(null, "MEMBER", properties2);
		//		t2.entities.add(member2);
		//		

		ArrayList<Transaction> transactions= new ArrayList<Transaction>();


		Transaction a= new Transaction("ViewProfile");
		a.entities= new ArrayList<Entity>();
		Property[] aproperties= new Property[1];
		aproperties[0]= new Property("fcount", null, ValidationParams.VALUE_READ);
		//	aproperties[1]= new Property("pcount", null, ValidationConstants.VALUE_READ);

		Entity ae1= new Entity(null, "member", aproperties);
		//Entity ae2= new Entity(null, "member", aproperties);

		a.entities.add(ae1);

		Transaction b= new Transaction("acceptFriend");
		b.entities= new ArrayList<Entity>();
		Property[] bproperties= new Property[1];
		bproperties[0]= new Property("fcount", null, ValidationParams.INCREMENT_UPDATE);
		//bproperties[1]= new Property("pcount", null, ValidationConstants.INCREMENT_UPDATE);

		Entity be1= new Entity(null, "member", bproperties);
		//Property[] bproperties2= new Property[1];
		//bproperties2[0]= new Property("fcount", null, ValidationConstants.INCREMENT_UPDATE);
		//Entity be2= new Entity(null, "member2", bproperties);
		b.entities.add(be1);
		//	b.entities.add(be2);
		//		Property[] bproperties2= new Property[1];
		//		bproperties2[0]= new Property("e2", null, ValidationConstants.VALUE_READ);
		//		Entity be2= new Entity(null, "e2", bproperties2);
		//		b.entities.add(be2);

		Transaction c= new Transaction("acceptFriend2");
		c.entities= new ArrayList<Entity>();
		Property[] cproperties= new Property[2];
		cproperties[0]= new Property("pcount", null, ValidationParams.INCREMENT_UPDATE);
		cproperties[1]= new Property("pcount", null, ValidationParams.VALUE_READ);

		Entity ce1= new Entity(null, "member", cproperties);

		c.entities.add(ce1);

		Transaction d= new Transaction("acceptFriend3");
		d.entities= new ArrayList<Entity>();
		Property[] dproperties= new Property[2];
		dproperties[0]= new Property("pcount", null, ValidationParams.INCREMENT_UPDATE);
		dproperties[1]= new Property("fcount", null, ValidationParams.VALUE_READ);

		Entity de1= new Entity(null, "member", dproperties);

		d.entities.add(de1);

		//		Transaction c= new Transaction("c");
		//		c.entities= new ArrayList<Entity>();
		//		Property[] cproperties= new Property[1];
		//		cproperties[0]= new Property("e3", null, ValidationConstants.VALUE_READ);
		//		Entity ce3= new Entity(null, "e3", cproperties);
		//		c.entities.add(ce3);
		//		Property[] cproperties2= new Property[1];
		//		cproperties2[0]= new Property("e4", null, ValidationConstants.NEW_VALUE_UPDATE);
		//		Entity ce4= new Entity(null, "e4", cproperties2);
		//		c.entities.add(ce4);
		//		
		//		Transaction d= new Transaction("d");
		//		d.entities= new ArrayList<Entity>();
		//		Property[] dproperties= new Property[1];
		//		dproperties[0]= new Property("e4", null, ValidationConstants.VALUE_READ);
		//		Entity de4= new Entity(null, "e4", dproperties);
		//		d.entities.add(de4);
		//		Property[] dproperties2= new Property[1];
		//		dproperties2[0]= new Property("e5", null, ValidationConstants.NEW_VALUE_UPDATE);
		//		Entity de5= new Entity(null, "e5", dproperties2);
		//		d.entities.add(de5);
		//		
		//		
		//		Transaction e= new Transaction("e");
		//		e.entities= new ArrayList<Entity>();
		//		Property[] eproperties= new Property[1];
		//		eproperties[0]= new Property("e5", null, ValidationConstants.NEW_VALUE_UPDATE);
		//		Entity ee5= new Entity(null, "e5", eproperties);
		//		e.entities.add(ee5);


		transactions.add(a);
		transactions.add(b);
		transactions.add(c);
		transactions.add(d);
		//		transactions.add(e);


		HashMap<Transaction, HashSet<String>> transactionsMap=null;
		try {
			transactionsMap = buildHashmap(transactions);
			removeUnconflictProps(transactions,transactionsMap);
			getTopics(transactions,"MEMBER");
			Graph<String> g = Graph.buildGraph("/home/mr1/app1.txt");
			//	setKeys(transactions,g);
			for (Transaction t:transactions){
				System.out.println(t);
				System.out.println("======================================");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}


	}

	private static String generateLogRecordCode(String entityArrName, Transaction t,  int numPartitions) {
		int count=1;
		String nl=System.getProperty("line.separator");
		StringBuilder sb= new StringBuilder();

		sb.append("String logRecord=getLogRecordString('"+t.type+"', \""+t.name+"\", recordKey, startTime, endTime, "+entityArrName+");"+nl);
		//producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), value));
		//		int partition = (key % numPartitions) + numPartitions;
		//		if (t.type != ValidationConstants.READ_RECORD)
		//			partition = (key % numPartitions) + (3 * numPartitions);
		sb.append("int numPartitionsE="+numPartitions+";"+nl);
		if (t.type == ValidationParams.READ_RECORD){
			sb.append("int partitionEnd=("+t.key+"%numPartitionsE)+numPartitionsE;"+nl);
		}
		else if(t.type != ValidationParams.READ_RECORD){
			sb.append("int partitionEnd=("+t.key+"%numPartitionsE)+(3*numPartitionsE);"+nl);


		}
		sb.append("producer.send(new ProducerRecord<String, String>(\""+t.topic+"\",partitionEnd, recordKey, logRecord));"+nl);

		return sb.toString();
	}
	private static String generateStartLogRecordCode(String entityArrName, Transaction t, int numPartitions) {
		//int count=1;
		String nl=System.getProperty("line.separator");
		StringBuilder sb= new StringBuilder();
		sb.append("long startTime=System.nanoTime();"+nl);

		//		sb.append("String logRecord=getLogRecordString("+t.type+", "+t.name+", recordKey, startTime, endTime, "+entityArrName+");"+nl);
		//producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), value));
		//		int partition = (key % numPartitions) + numPartitions;
		//		if (t.type != ValidationConstants.READ_RECORD)
		//			partition = (key % numPartitions) + (2 * numPartitions);
		sb.append("int numPartitionsS="+numPartitions+";"+nl);
		if (t.type == ValidationParams.READ_RECORD){
			sb.append("int partitionStart=("+t.key+"%numPartitionsS);"+nl);
		}
		else if(t.type != ValidationParams.READ_RECORD){
			sb.append("int partitionStart=("+t.key+"%+numPartitionsS)+(2*numPartitionsS);"+nl);


		}
		sb.append("producer.send(new ProducerRecord<String, String>(\""+t.topic+"\",partitionStart, recordKey, Long.toString(startTime));"+nl);

		return sb.toString();
	}

	private static String generateEntitiesCode(String entitiesArrName,Transaction t, HashMap<String, Entity> entities,
			HashMap<String, Property> props) {

		int count=1;
		String nl=System.getProperty("line.separator");
		StringBuilder sb= new StringBuilder();
		ArrayList<Entity> transEntities= new ArrayList<Entity>();

		sb.append("ArrayList<Entity> "+entitiesArrName+"= new ArrayList<Entity>();"+nl);
		int setsCount=0;
		String prevEid="-1";
		for (Entity e: t.entities){
			if(prevEid.equals(e.getName())){
				setsCount++;
			}
			else
				setsCount=0;
			prevEid=e.getName();
			Entity tempE=entities.get("E-"+e.getName());
			String pkeys[]=tempE.getKey().split("-");
			String entityKey="";
			for (String pkey:pkeys){
				String pVar=t.propsVars.get("E-"+setsCount+"-"+e.getName()+"-"+pkey);
				entityKey=entityKey+pVar+"\""+ValidationParams.KEY_SEPERATOR+"\"";
			}

			entityKey=entityKey.substring(0,entityKey.length()-3);
			String entityKeyName=tempE.getName()+"_key_"+(count++);
			sb.append("String "+entityKeyName+"="+entityKey+";"+nl);
			int l=e.getProperties().length;
			Property[] properties= new Property[l];
			String propsArrName="props_"+tempE.getName()+"_"+(count++);
			sb.append("Property[] "+propsArrName+"=new Property["+l+"];"+nl);
			for (int j=0; j<e.getProperties().length;j++){
				Property p= e.getProperties()[j];
				Property propTemp=props.get("E-"+e.getName()+"-"+p.getName());
				String pname=propTemp.getName();
				String value=t.propsVars.get("E-"+setsCount+"-"+e.getName()+"-"+p.getName());
				char pType=p.getType();
				if (pType==ValidationParams.DECREMENT_UPDATE_INTERFACE){
					pType=ValidationParams.INCREMENT_UPDATE;
					value=value+"*-1";
				}
				properties[j]= new Property(pname, value, pType);
				sb.append(propsArrName+"["+j+"]= new Property(\""+pname+"\",String.valueOf("+value+"), '"+pType+"');"+nl);

			} // prop loop
			Entity mye= new Entity(entityKey, tempE.getName(), properties);
			String entityName="e_"+tempE.getName()+"_"+(count++);
			sb.append(" Entity " +entityName+"= new Entity("+entityKeyName+",\""+ tempE.getName()+"\","+propsArrName+");"+nl);
			transEntities.add(mye);
			sb.append(entitiesArrName+".add("+entityName+");"+nl);

		} // entity loop
		return sb.toString();
	}

	private static ArrayList<Transaction> prepareDTs(String erFile,ArrayList<String> jsonStrings,HashMap<String, Entity> entities,
			HashMap<String, Property> props,String application) {
		parseEr(erFile, entities, props);
		//		ArrayList<String> jsonStrings= new ArrayList<String>();
		//		jsonStrings.add("/home/mr1/t.txt");
		ArrayList<Transaction> transactions = parseTrans(jsonStrings);
		HashMap<Transaction, HashSet<String>> transactionsMap=null;

		try {
			transactionsMap = buildHashmap(transactions);
			removeUnconflictProps(transactions,transactionsMap);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		getTopics(transactions,application);
		Graph<String> g = Graph.buildGraph(erFile);
		setKeys(transactions,g,entities,props);

		for (Transaction t:transactions)
			t.setType();
		return transactions;
	}

	private static void setKeys(ArrayList<Transaction> transactions,Graph <String>g, HashMap<String, Entity> entities, HashMap<String, Property> props) {
		HashMap<String, ArrayList<Transaction>> groups= new HashMap<String, ArrayList<Transaction>>();
		HashSet<String> entitiesMap = null;
		for (Transaction transaction: transactions){
			if (!groups.containsKey(transaction.topic)){

				ArrayList<Transaction> trans= new ArrayList<Transaction>();
				groups.put(transaction.topic, trans);
			}
			groups.get(transaction.topic).add(transaction);

		}

		for (String group: groups.keySet()){
			ArrayList<Transaction> groupTrans= groups.get(group);
			HashSet<String> allowed= new HashSet<String>(); 
			HashSet<String> referencedEnities= new HashSet<String>();
			getRefEnt_AllowedEdg(groupTrans,referencedEnities,allowed);
			HashSet<String> res = g.getPartitioningKeys(referencedEnities, allowed);
			setKeysForGroup(groupTrans,res,entities,props);


		} // end group

	}

	private static void setKeysForGroup(ArrayList<Transaction> groupTrans, HashSet<String> res, HashMap<String, Entity> entities, HashMap<String, Property> props) {
		String key=null;
		HashSet<String> validKeys=new HashSet<String>();
		if(res.isEmpty()){
			key="1";
		}
		else{
			int count=0;
			boolean firstCkey=true;
			for (String ckey: res){
				boolean validKey=true;
				for (Transaction t:groupTrans){

					Entity e= entities.get("E-"+ckey);
					
					String[] pks=e.getKey().split("-");
					if (pks.length!=1){
						validKey=false;
						break;
					}
					else{
						String pid= pks[0];
						Property propTemp=props.get("E-"+ckey+"-"+pid);
						if (propTemp.getValue().equals("Double")|| propTemp.getValue().equals("Integer")){
							if (t.propsVars.get("E-1-"+ckey+"-"+pid)!=null){
								validKey=false;
								break;
							}

						}
						else{
							validKey=false;
							break;
						}

					}
				} // trans loop






				if (validKey){
					validKeys.add(ckey);
				}

			} // cKey loop
			key=null;
			if (validKeys.isEmpty())
				key="1";
			for (String ckey: validKeys){
				boolean validKey=true;

				count++;


				if(count==validKeys.size()&& key==null){
					// last key dont delete
					key=ckey;
					break;
				}

				for (Transaction transaction:groupTrans){

					for (int i=transaction.entities.size()-1;i>=0;i++){
						Entity e=transaction.entities.get(i);
						if (ckey.equals(e.getName())){
							if (e.getProperties().length==0){
								validKey=false;
								transaction.entities.remove(i);
							}
						}

						if(firstCkey){ // remove any empty entity that is not among the candidate keys. We only need to do it once
							if ( e.getProperties().length==0 && !validKeys.contains(e.getName())){
								transaction.entities.remove(i);
							}
						}
					}
				}
				if (validKey){
					key=ckey;
				}

				firstCkey=false;
			} // vKey loop

		}

		if (!key.equals("1")){ // get variable 

			for (Transaction t:groupTrans){

				Entity e= entities.get("E-"+key);
				String[] pks=e.getKey().split("-");

				String pid= pks[0];
				t.key=t.propsVars.get("E-0-"+key+"-"+pid);



			} // trans loop
		}
		if (key.equals("1")){ //key="1"
			for (Transaction transaction:groupTrans){
				transaction.key=key;
			}
		}




	}

	private static void getRefEnt_AllowedEdg(ArrayList<Transaction> groupTrans, HashSet<String> referencedEnities,
			HashSet<String> allowed) {
		for (Transaction trans:groupTrans){
			allowed.addAll(trans.relationships);
			for (Entity e: trans.entities){
				referencedEnities.add(e.getName());
			}
		}

	}

	private static HashSet<String> getUniqueEntitiesMap(Transaction trans) {
		HashSet<String> entitiesHash= new HashSet<String>();
		HashSet<String> notAllowedEntities= new HashSet<String>();

		for (Entity e: trans.entities){
			if (entitiesHash.contains(e.getName())){
				entitiesHash.remove(e.getName());
				notAllowedEntities.add(e.getName());
			}
			if (!notAllowedEntities.contains(e.getName())){
				entitiesHash.add(e.getName());
			}
		}
		return entitiesHash;

	}



}
