package edu.usc.polygraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import static com.mongodb.client.model.Filters.*;
//import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class Database {
	public final static int PORT=27017;
	public final static String HOST="localhost";
	 public  MongoClient mongoClient;
	 public MongoDatabase db;
	 public  static String databaseName="test";
	 public static final String SCHED_ID_FIELD_NAME="sid";
	 public static final String PRIMARY_KEY_FIELD_NAME="pkey";

	public void deleteSchedules(ArrayList<String> discardedSchIdList) {
		
		for (String ename:ValidationParams.ENTITY_NAMES){
			MongoCollection<Document> coll = db.getCollection(ename);
			coll.deleteMany(in(SCHED_ID_FIELD_NAME, discardedSchIdList));

		}
        // System.out.println(deleteResult.getDeletedCount());
		
	}

	public void processDBStatesCheckRefCount(ArrayList<CandidateValue> newDBStateList,  ValidatorData vd) {
		HashSet<String> hasBeenDeleted= new HashSet<>();
		HashMap<String,List<WriteModel<Document>>> writesmap = new HashMap<String,List<WriteModel<Document>> >();
		BulkWriteOptions wopt= new BulkWriteOptions();
		wopt.bypassDocumentValidation(true);
		wopt.ordered(false);
		HashMap<String, ArrayList<Document>> docList= new HashMap<String, ArrayList<Document>>();
		for (CandidateValue v: newDBStateList){
			// check v is still referenced
			if (v.dbState.refrenceCount==0 ){
				// check if it is not the first continue
				String entityKey= Utilities.concat(v.eName, ValidationParams.KEY_SEPERATOR, v.eKey);
				LinkedList<DBState> ll = vd.dbState.get(entityKey);
				if (ll==null || ll.isEmpty() ||v.dbState!=vd.dbState.get(entityKey).getFirst()){
					continue;
				}
			}
			

			assert !v.updateDocument:"An update document while expecting insert only!";
			String entityKey= Utilities.concat(v.eName, ValidationParams.KEY_SEPERATOR, v.eKey);
			if (!hasBeenDeleted.contains(entityKey)){
				hasBeenDeleted.add(entityKey);
				MongoCollection<Document> coll = db.getCollection(v.eName);
				List<WriteModel<Document>> writes =writesmap.get(v.eName);
				if (writes==null){
					writes = new ArrayList<WriteModel<Document>>();
				
					writesmap.put(v.eName, writes);

				}

				int primaryKey=Integer.parseInt(v.eKey);
				writes.add(new DeleteManyModel<Document>(eq(PRIMARY_KEY_FIELD_NAME, primaryKey)));

			//	coll.deleteMany(eq(PRIMARY_KEY_FIELD_NAME, primaryKey));
			}
			Document doc= createDocument(v.eKey, v.sid, v.eName, v.dbState);
			ArrayList<Document> arr = docList.get(v.eName);
			if (arr==null){
				arr=new ArrayList<Document>();
				docList.put(v.eName, arr);
			}
			arr.add(doc);
			//}
		 } // end looping new entities
		for (String k:writesmap.keySet()){
			MongoCollection<Document> coll = db.getCollection(k);
			BulkWriteResult bulkWriteResult = coll.bulkWrite(writesmap.get(k),wopt);
			
			//System.out.println(bulkWriteResult.getDeletedCount());
			
		}
		insertDocList(docList);
	
	}

	private void updateDocuments(String ekey, String ename, DBState dbState) {
		MongoCollection<Document> coll = db.getCollection(ename);
		Document search= new Document();
		if (ValidationParams.INTEGER_KEY){
		int primarykey= Integer.parseInt(ekey);
		search.put(PRIMARY_KEY_FIELD_NAME, primarykey);
		}
		else{
			search.put(PRIMARY_KEY_FIELD_NAME, ekey);

		}
		Document updateDoc = new Document();
	    // fill doc
		int index=-1;
		for (int j=0; j<ValidationParams.ENTITY_NAMES.length;j++) {
				if (ename.equals(ValidationParams.ENTITY_NAMES[j])) {
					index = j;
					break;
				}
			}
			
		String propNames[]= ValidationParams.ENTITY_PROPERTIES[index];
		String[] values = dbState.getValue();
		for (int i=0; i< propNames.length;i++){
			if (values[i]!=null)
			updateDoc.put(propNames[i], values[i]);
			
		}
		Document updateSet= new Document();
		updateSet.put("$set", updateDoc);
		coll.updateMany(search, updateSet);
		
	}
	private Document createDocument(String key, String sid, String ename, DBState state){
		Document doc= new Document();
		if (ValidationParams.INTEGER_KEY){
			int intKey= Integer.parseInt(key);
			doc.put(PRIMARY_KEY_FIELD_NAME, intKey);
		}
		else{
			doc.put(PRIMARY_KEY_FIELD_NAME, key);

		}
		doc.put(SCHED_ID_FIELD_NAME, sid);
		fillDocument(doc,ename,state);
		return doc;
	}
	private void insertDocument(String key, String sid, String ename, DBState state) {
		Document doc= createDocument(key, sid, ename, state);
		MongoCollection<Document> coll = db.getCollection(ename);
		coll.insertOne(doc);
		
	}

	private void fillDocument(Document doc, String ename,DBState state) {
		int index=-1;
		for (int j=0; j<ValidationParams.ENTITY_NAMES.length;j++) {
				if (ename.equals(ValidationParams.ENTITY_NAMES[j])) {
					index = j;
					break;
				}
			}
			
		String propNames[]= ValidationParams.ENTITY_PROPERTIES[index];
		if (state.getValue()==Validator.deletedArr){
			for (int i=0; i< propNames.length;i++){
				doc.put(propNames[i], ValidationParams.DELETED_STRING);
				
			}
		}
		else{
		String[] values = state.getValue();
		for (int i=0; i< propNames.length;i++){
			doc.put(propNames[i], values[i]);
			
		}
		}
		
	}

	
	
public void init() {
		
	 mongoClient = new MongoClient( HOST ,PORT  );
	 mongoClient.dropDatabase(databaseName);
	  db = mongoClient.getDatabase( databaseName );
	  
		for ( String name:ValidationParams.ENTITY_NAMES){
			db.createCollection(name);
			MongoCollection<Document> coll = db.getCollection(name);
			BasicDBObject index = new BasicDBObject(PRIMARY_KEY_FIELD_NAME,1);
			coll.createIndex(index);
		}
	}
public  void close(){
	MongoIterable<String> collections = db.listCollectionNames();
	String names="";
	for ( String name:collections){
		names+=name+",";
	}
	System.out.println("Collection names:"+names);
	db.drop();
	mongoClient.close();
}
	
	public static void main(String[] args) {
		System.out.println("Testing mongo");
		 try{   
				Database dbobj= new Database();
	         dbobj.init();
				
	         // Now connect to your databases
	          
	         System.out.println("Connect to database successfully");
				
	            
	         MongoCollection<Document> coll = dbobj.db.getCollection("mycol");
	         System.out.println("Collection mycol selected successfully");
				
	         Document doc = new Document("title", "MongoDB").
	            append("description", 1).
	            append("likes", "1").
	            append("url", "http://www.tutorialspoint.com/mongodb/").
	            append("by", "tutorials point");
	         Document doc2 = new Document("title", "MongoDB").
	 	            append("description", 2).
	 	            append("likes", "1911").
	 	            append("url", "http://www.tutorialspoint.com/mongodb/").
	 	            append("by", null);
					
	         coll.insertOne(doc);
	         coll.insertOne(doc2);
	         BasicDBObject getQuery = new BasicDBObject();
	         getQuery.put("likes", new BasicDBObject("$gt", "18").append("$lt", "20"));
	         getQuery.put("title", new BasicDBObject("$ne", "MongoD"));
	        FindIterable<Document> qdoc = coll.find(getQuery);
	        
			
			for (Document q:qdoc){
				if(q.getString("by")==null){
					System.out.println("Yes nulll");
				}
	        	System.out.println(q);
	        }
	        System.exit(0);
	 		
	         
	         //update
	         Document search= new Document();
	 		search.put("title", "MongoDB");
	 		Document updateDoc = new Document();
	 	    // fill doc
	 		
	 		updateDoc.put("by", null);	
	 		
	 		Document updateSet= new Document();
	 		
	 		updateSet.put("$set", updateDoc);
	 		coll.updateMany(search, updateSet);
	 		FindIterable<Document> result = coll.find();
	 		for (Document doc1: result){
	 			Object x = doc1.get("by");
	 			System.out.println(x);
	 			System.out.println(doc1);
	 		}
	         dbobj.close();

	      }catch(Exception e){
	         e.printStackTrace(System.out);
	      }
	   }

	public void updateDatabase(ScheduleList finalSS) {
		HashMap<String,List<WriteModel<Document>>> writesmap = new HashMap<String,List<WriteModel<Document>> >();
		BulkWriteOptions wopt= new BulkWriteOptions();
		wopt.bypassDocumentValidation(true);
		wopt.ordered(false);
		HashSet<String> hasBeenDeleted= new HashSet<String>();
		HashMap <String, ArrayList<Document>> docList= new HashMap<String, ArrayList<Document>>();
		for (Schedule schedule:finalSS.schedules){
			schedule.newSchedule=false;
		getImpacted(docList, schedule,hasBeenDeleted,writesmap);
	} //schedule loop
		for (String k:writesmap.keySet()){
			MongoCollection<Document> coll = db.getCollection(k);
			BulkWriteResult bulkWriteResult = coll.bulkWrite(writesmap.get(k),wopt);
			
			//System.out.println(bulkWriteResult.getDeletedCount());
			
		}
		insertDocList(docList);
	
	}
	
	private HashMap<String, ArrayList<Document>>  getNotRefImpacted(Schedule schedule, HashSet<String> refEntities, HashMap<String, List<WriteModel<Document>>> writesmap) {
		HashMap<String, ArrayList<Document>> docList= new HashMap<String, ArrayList<Document>> ();
		
		for (String key: schedule.getImpactedStates().keySet()){
			
			String ename=key.substring(0,key.indexOf(ValidationParams.KEY_SEPERATOR));
			String primaryKey=key.substring(key.indexOf(ValidationParams.KEY_SEPERATOR)+1, key.length());
			DBState state = schedule.getImpactedStates().get(key);
			
			if (refEntities.contains(key)){
				continue;
			}
				
				MongoCollection<Document> coll = db.getCollection(ename);
				int intprimaryKey= Integer.parseInt(primaryKey);
				List<WriteModel<Document>> writes =writesmap.get(ename);
				if (writes==null){
					writes = new ArrayList<WriteModel<Document>>();
				
					writesmap.put(ename, writes);

				}
				writes.add(new DeleteManyModel<Document>(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey)));

			//	coll.deleteMany(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey));
			
			Document doc = createDocument(primaryKey, schedule.sid, ename, state);
		ArrayList<Document> myList = docList.get(ename);
		if (myList==null){
			myList= new ArrayList<Document>();
			docList.put(ename, myList);
		}
		myList.add(doc);
	} // impacted loop
		
	
		return docList;
	}

	private void getImpacted(HashMap<String, ArrayList<Document>> docList, Schedule schedule, HashSet<String> hasBeenDeleted, HashMap<String, List<WriteModel<Document>>> writesmap) {
		
		
		for (String key: schedule.getImpactedStates().keySet()){
			
			String ename=key.substring(0,key.indexOf(ValidationParams.KEY_SEPERATOR));
			String primaryKey=key.substring(key.indexOf(ValidationParams.KEY_SEPERATOR)+1, key.length());
			DBState state = schedule.getImpactedStates().get(key);
			
			if (!hasBeenDeleted.contains(key)){
				
				hasBeenDeleted.add(key);
				MongoCollection<Document> coll = db.getCollection(ename);
				List<WriteModel<Document>> writes =writesmap.get(ename);
				if (writes==null){
					writes = new ArrayList<WriteModel<Document>>();
				
					writesmap.put(ename, writes);

				}

				if (ValidationParams.INTEGER_KEY){
				int intprimaryKey= Integer.parseInt(primaryKey);
				writes.add(new DeleteManyModel<Document>(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey)));
			//	coll.deleteMany(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey));
				}
				else{
					writes.add(new DeleteManyModel<Document>(eq(PRIMARY_KEY_FIELD_NAME, primaryKey)));

				//	coll.deleteMany(eq(PRIMARY_KEY_FIELD_NAME, primaryKey));

					
				}
			}
			Document doc = createDocument(primaryKey, schedule.sid, ename, state);
		ArrayList<Document> myList = docList.get(ename);
		if (myList==null){
			myList= new ArrayList<Document>();
			docList.put(ename, myList);
		}
		myList.add(doc);
	} // impacted loop

	
		
	}

	private void insertDocList(HashMap<String, ArrayList<Document>> docList) {
		for (String name:docList.keySet()){
			MongoCollection<Document> coll = db.getCollection(name);
			ArrayList<Document> list = docList.get(name);
			coll.insertMany(list);
		}
		
	}

	public void processDBStates(ArrayList<CandidateValue> newDBStateList) {
		for (CandidateValue v: newDBStateList){
			
			if (v.updateDocument){
				updateDocuments(v.eKey, v.eName, v.dbState);
			}
			else{
				insertDocument(v.eKey, v.sid, v.eName, v.dbState);
			}
		
	}
	}
	
public boolean processScan(HashMap<String, LinkedList<DBState>> dbState,LogRecord r, boolean generateRecords, HashMap<String, String> expectedValues, ArrayList<EntitySpec> generatedEntities, Schedule s, boolean[] hasGeneratedState, long[] readMatchUpdateTime) {
		hasGeneratedState[0]=false;
//	if (r.getId().equals("1-88"))
//			System.out.println("found it");
		HashSet<String> scanProps= new HashSet<String>();
		Entity oneEntity = r.getEntities()[0];
		for (Property p: oneEntity.getProperties()){
			scanProps.add(p.getName());
		}
		
		HashMap<String, String> computedProperties = new HashMap<String, String>();
		HashMap<String, Boolean> computedPropertiesStatus = new HashMap<String, Boolean>();
		

		boolean startCompute = false;
		for (int i = s.getRecords().size() - 1; i >= 0; i--) {
			if (startCompute) {
				
				for (Entity e : s.getRecords().get(i).getEntities()) {
					if (!e.getName().equals(oneEntity.getName()))
							continue;
					for (Property p : e.getProperties()) {
						if(ValidationParams.SHRINK_BUCKET && p==null)
							continue;
						String pKey = Property.getProprtyKey(e, p);
						// is this property being read?
						if (scanProps.contains(p.getName())) {
							// is this property NOT updated with new value later?
							if (computedPropertiesStatus.get(pKey)==null ||!computedPropertiesStatus.get(pKey)) {
								if (p.getType() == ValidationParams.NEW_VALUE_UPDATE) {
									computedPropertiesStatus.put(pKey, true);
								
									String value = computedProperties.get(pKey);
									if (value==null) {
										computedProperties.put(pKey, p.getValue());
									} else {
										double v = Double.parseDouble(value);
										v += Double.parseDouble(p.getValue());
										String vString = ValidationParams.DECIMAL_FORMAT.format(v);
										computedProperties.put(pKey, vString);
									}
								} else if (p.getType() == ValidationParams.INCREMENT_UPDATE) {
									String value = computedProperties.get(pKey);
									if (value==null)
										value="0";
									value = ValidationParams.DECIMAL_FORMAT.format(Double.parseDouble(value) + Double.parseDouble(p.getValue()));
									computedProperties.put(pKey, value);
								} else if (p.getType() == ValidationParams.VALUE_DELETED) {
									computedProperties.put(pKey, ValidationParams.DELETED_STRING);
									computedPropertiesStatus.put(pKey, true);
									
								}
							}
						}
					}
				}
				// if(records.get(i).)
			} else {
				if (s.getRecords().get(i).getId().equals(r.getId())) {
					startCompute = true;
				}
			}
		} // end processing all records in SS
		// prepare docs
		HashSet<String> refEntitiyKeys= new HashSet<String>();
		HashMap <String, ArrayList<Document>> delDocList= new HashMap<String, ArrayList<Document>>();
		HashMap<String,List<WriteModel<Document>>> writesmap = new HashMap<String,List<WriteModel<Document>> >();
		BulkWriteOptions wopt= new BulkWriteOptions();
		wopt.bypassDocumentValidation(true);
		wopt.ordered(false);
		HashMap<String, ArrayList<Document>> docToInsertNotImpacted = getDocuments(delDocList,computedProperties, computedPropertiesStatus, dbState,s,refEntitiyKeys,writesmap);
		HashMap<String, ArrayList<Document>> docToInsertImpacted=getNotRefImpacted( s, refEntitiyKeys,writesmap);
		for (String k:writesmap.keySet()){
			MongoCollection<Document> coll = db.getCollection(k);
			coll.bulkWrite(writesmap.get(k),wopt);
						
		}
		for (String key:docToInsertNotImpacted.keySet()){
			if (docToInsertImpacted.containsKey(key)){
				ArrayList<Document> doc1 = docToInsertNotImpacted.get(key);
				ArrayList<Document> doc2 = docToInsertImpacted.remove(key);
				doc1.addAll(doc2);
			}
		}
		insertDocList(docToInsertNotImpacted);
		insertDocList(docToInsertImpacted);

		// issue query
		FindIterable<Document> qResult = issueQuery(r);
		boolean result=validateQuery(qResult, r,generateRecords, expectedValues,  generatedEntities,hasGeneratedState);
		if(ValidationParams.hasInitState){
			assert generatedEntities.isEmpty():"Polygraph has initial state no entity state should be generated";
		}
		///////////////////////////////////
		deleteInsertedDoc(s.sid);
		insertDocList(delDocList);
		
		return result;
	
	}

private boolean validateQuery(FindIterable<Document> qResult, LogRecord r, boolean generateRecords,
		HashMap<String, String> expectedValues, ArrayList<EntitySpec> generatedEntities, boolean[] hasGeneratedState) {
	HashMap<String, Document> qDocs= new HashMap<String,Document>();
	HashMap<String, Document> deletedDocs= new HashMap<String,Document>();

//	System.out.println("Query params="+r.getQueryParams());
	for (Document doc:qResult){
		String pkey=String.valueOf(doc.get(PRIMARY_KEY_FIELD_NAME));
//		if (pkey.equals("96"))
//			System.out.println("found it");
		//System.out.println(String.valueOf(pkey));
		String propValue=doc.getString(r.getEntities()[0].getProperties()[0].getName());
		if (propValue.equals(ValidationParams.DELETED_STRING))
			deletedDocs.put(pkey, doc);

		else
		qDocs.put(pkey, doc);
	}
	if (ValidationParams.hasInitState){
	if (qDocs.size()!=r.getEntities().length){ // with init state
//		System.out.println("Read:"+r.getEntities().length);
//		System.out.println("Database:"+qDocs.size());
//		print(r.getEntities(),qDocs);
	return false;
	}
	}
	else{
		// no init state
		if (qDocs.size()> r.getEntities().length){
//			System.out.println("Read:"+r.getEntities().length);
//			System.out.println("Database:"+qDocs.size());
//			print(r.getEntities(),qDocs);
//			print(null, deletedDocs);
			return false;
		}
	}
	for (Entity e: r.getEntities()){
		Document document=qDocs.get(e.key);
		if (document==null){
			if (ValidationParams.hasInitState || deletedDocs.containsKey(e.key))
			return false;
			else{
				
				if (generateRecords){
				// establish entity
				establishEntity(e, generatedEntities);
				hasGeneratedState[0]=true;
				continue;
				}
				else{
					return false;
				}
			}
		}
		boolean result=compareEntityDoc(e,document,expectedValues,generatedEntities,generateRecords,hasGeneratedState);
		if (result==false)
			return false;
	}
	
	
	return true;
}

private void establishEntity(Entity e, ArrayList<EntitySpec> generatedEntities) {
	
	
		ArrayList<Property> newProps = new ArrayList<Property>();
		EntitySpec eg = new EntitySpec(e.key, e.name, null);
		eg.setPropertiesArrayLis(newProps);
		generatedEntities.add(eg);
		for (Property p: e.getProperties()){
			Property p1= new Property(p.getName(), p.getValue(), ValidationParams.NEW_VALUE_UPDATE);
			newProps.add(p1);
			
		}
			
	
}

private void print(Entity[] entities, HashMap<String, Document> qDocs) {
	ArrayList<Integer> readResult= new ArrayList<Integer>();
	ArrayList<Integer> dbResult= new ArrayList<Integer>();
	if (entities!=null){
	for (Entity e:entities){
		readResult.add(Integer.parseInt(e.getKey()));
	}
	}
	for (Document doc:qDocs.values()){
		dbResult.add(Integer.parseInt(String.valueOf(doc.get(PRIMARY_KEY_FIELD_NAME))));
	}
	if (entities!=null){
	System.out.println("Read:"+readResult.size());
	Collections.sort(readResult);
	System.out.println(readResult);
	}
	System.out.println("DB:"+dbResult.size());
	Collections.sort(dbResult);
	System.out.println(dbResult);


}

private boolean compareEntityDoc(Entity e, Document document, HashMap<String, String> expectedValues, ArrayList<EntitySpec> generatedEntities, boolean generateRecords, boolean[] hasGeneratedState) {
	
	for(Property p:e.getProperties()){
		
		String docValue=document.getString(p.getName());
		if (!ValidationParams.hasInitState && docValue==null && generateRecords){
			establishProperty(e,p,generatedEntities);
			hasGeneratedState[0]=true;
			continue;
		}
		if(docValue==null){
			System.out.println("Doc value is null");
			System.out.println("Read value:"+ p.getValue());
			return false;
		}
		if (!Schedule.compareValues(docValue, p.getValue())) {
//			System.out.println("Read value for "+ p.getProprtyKey(e)+"="+p.getValue());
//			System.out.println("expected value "+"="+docValue);

			if(expectedValues != null){
				expectedValues.put(e.getName() + ValidationParams.KEY_SEPERATOR + e.getKey() + ValidationParams.KEY_SEPERATOR + p.getName(), docValue);
			}
			return false;
		}
	}
	return true;
}

private void establishProperty(Entity e, Property p, ArrayList<EntitySpec> generatedEntities) {
	ArrayList<Property> newProps = new ArrayList<Property>();
	EntitySpec eg = new EntitySpec(e.key, e.name, null);
	eg.setPropertiesArrayLis(newProps);
	generatedEntities.add(eg);
	Property p1= new Property(p.getName(), p.getValue(), ValidationParams.NEW_VALUE_UPDATE);
	newProps.add(p1);
		
	
	
}

private FindIterable<Document> issueQuery(LogRecord r) {
	Entity oneEntity = r.getEntities()[0];
MongoCollection<Document> coll = db.getCollection(oneEntity.getName());
String[] tokens = r.getQueryParams().split(":");
String upperBound=tokens[0];
String lowerBound=tokens[1];
BasicDBObject getQuery = new BasicDBObject();
getQuery.put(PRIMARY_KEY_FIELD_NAME, new BasicDBObject("$gt", Integer.parseInt(lowerBound)).append("$lt", Integer.parseInt(upperBound)));
//getQuery.put(oneEntity.getProperties()[0].getName(), new BasicDBObject("$ne", ValidationConstants.DELETED_STRING));
FindIterable<Document> docList= coll.find(getQuery);
//for (Document doc: docList){
//	System.out.println(doc);
//}
return docList;


}

private void deleteInsertedDoc(String sid) {
	for (String name:ValidationParams.ENTITY_NAMES){
		MongoCollection<Document> coll = db.getCollection(name);
		coll.deleteMany(eq(SCHED_ID_FIELD_NAME, sid));

	}
	
}

private HashMap <String, ArrayList<Document>> getDocuments(HashMap <String, ArrayList<Document>> delDocList,HashMap<String, String> computedProperties, HashMap<String, Boolean> computedPropertiesStatus, HashMap<String, LinkedList<DBState>> dbState, Schedule s, HashSet<String> refEntitiyKeys, HashMap<String, List<WriteModel<Document>>> writesmap) {
HashMap <String, ArrayList<Document>> insertDocList= new HashMap<String, ArrayList<Document>>();


for (String key : computedProperties.keySet()) {

	String entityName = Schedule.getEntityName(key);
	String entityKey = Schedule.getEntityKey(key);
	String primaryKey=key.substring(entityKey.indexOf(ValidationParams.KEY_SEPERATOR)+1, entityKey.length());


	DBState newState = null;
	if (!refEntitiyKeys.contains(entityKey)) {
		refEntitiyKeys.add(entityKey);
		if (computedProperties.get(key).equals(ValidationParams.DELETED_STRING)) {
			newState = new DBState(1, Validator.deletedArr);
		
		} else {
			// not delete
			newState = s.getNewState(dbState,entityKey, entityName, computedProperties, computedPropertiesStatus,false,null);
			if (!ValidationParams.hasInitState){
			if (newState==null)
				continue;
			}
			

		}
		
		// create doc
		assert newState!=null: "State shouldn't be null";
		Document doc= createDocument(primaryKey, s.sid, entityName, newState);
		
		MongoCollection<Document> coll = db.getCollection(entityName);
		List<WriteModel<Document>> writes =writesmap.get(entityName);
		if (writes==null){
			writes = new ArrayList<WriteModel<Document>>();
		
			writesmap.put(entityName, writes);

		}
		int intprimaryKey= Integer.parseInt(primaryKey);
		Document delDoc =null;// coll.find(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey)).first();//coll.findOneAndDelete(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey));
		writes.add(new DeleteManyModel<Document>(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey)));
	
//		DeleteResult r = coll.deleteMany(eq(PRIMARY_KEY_FIELD_NAME, intprimaryKey));
//		if (r.getDeletedCount()>0)
//		System.out.println("Delete many count="+r.getDeletedCount());
		if(delDoc!=null){
			ArrayList<Document> dellist = delDocList.get(entityName);
			if (dellist==null){
				dellist= new ArrayList<Document>();
				delDocList.put(entityName, dellist);
			}
			dellist.add(delDoc);
		}
	
		ArrayList<Document> list = insertDocList.get(entityName);
		if (list==null){
			list= new ArrayList<Document>();
			insertDocList.put(entityName, list);
		}
		list.add(doc);
		
	} 

}

return insertDocList;
}

public void initializeState(HashMap<String, LinkedList<DBState>> dbState) {
	HashMap<String, ArrayList<Document>> docList= new HashMap<String, ArrayList<Document>>();
	for (String key: dbState.keySet()){
		String ename=key.substring(0,key.indexOf(ValidationParams.KEY_SEPERATOR));
		String ekey=key.substring(key.indexOf(ValidationParams.KEY_SEPERATOR)+1, key.length());
		
		DBState state=dbState.get(key).getFirst();
		Document doc = createDocument(ekey, ValidationParams.SID_INIT, ename, state);
		ArrayList<Document> myList = docList.get(ename);
		if (myList==null){
			myList= new ArrayList<Document>();
			docList.put(ename, myList);
		}
		myList.add(doc);
	}
	insertDocList(docList);
	
	
}
	
}
