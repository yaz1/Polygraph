package edu.usc.polygraph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;


public class Schedule {
	
	private ArrayList<LogRecord> records;
	private ArrayList<Schedule> parent;
	private HashMap<String, DBState> impactedStates;
	String sid=null;
	boolean newSchedule=true;
	ArrayList<LogRecord> shrinked= null; //YAZXX
//	public static ArrayList<EntitySpec> generatedEntities= new ArrayList<EntitySpec>();
	// private short index1stOverlaping = -1;

	public Schedule(String readId, int[] counter) {
		if (counter!=null){
		counter[0]++;
		this.sid=readId+"-"+counter[0];
		}
		else{
			this.sid=readId;
		}
		
		this.records = new ArrayList<LogRecord>();
		this.impactedStates = new HashMap<String, DBState>();
		this.parent = new ArrayList<Schedule>();
		// index1stOverlaping = -1;
	}
	
	public HashMap<String, DBState> getImpactedStates() {
		return impactedStates;
	}

	public Schedule(String readId, int[]counter,Schedule x1, Schedule x2, boolean setSID) {
		this(readId,counter);
		for (LogRecord log : x1.records) {
			records.add(log);
		}

		boolean addThis;
		for (LogRecord log : x2.records) {
			addThis = true;
			for (LogRecord r : x1.records) {
				if (r.getId().equals(log.getId())) {
					addThis = false;
					break;
				}
			}
			if (addThis)
				records.add(log);
		}

		impactedStates = x1.getImpactedStates();

		for (String key : x2.getImpactedStates().keySet()) {
			if (!impactedStates.containsKey(key)) {
				impactedStates.put(key, x2.impactedStates.get(key));
			}
		}
	}

	public Schedule(String readId, int []counter,HashMap<String, LinkedList<DBState>> dbState, Schedule x1, Schedule x2, ArrayList<LogRecord> overlapList, boolean[] isOverlapShrinked, int s1Size, boolean isValidation, boolean firstOne, long et) {
		this(readId,counter);

		s1Size--;
		if (firstOne || isValidation) {// TODO make it last one
			impactedStates = x1.getImpactedStates();
		} else {
			for (String key : x1.getImpactedStates().keySet()) {
				DBState st = x1.getImpactedStates().get(key);
				if (st != null) {

					
						assert st.getRefrenceCount() >= 0 : "Incorrect value: it should be 0 or more. st.getRefrenceCount() = " + st.getRefrenceCount();
					increment(st, key);
					impactedStates.put(key, st);
				}
			}
		}

		boolean addThis;
		for (int j = 0; j < x2.records.size(); j++) {
			LogRecord log = x2.records.get(j);
			addThis = true;
			for (int i = 0; i < overlapList.size(); i++) {
				LogRecord r = overlapList.get(i);
				if (r.getId().equals(log.getId())) {
					// if (isOverlapShrinked[i]) {
					// addThis = false;
					// }
					addThis = !isOverlapShrinked[i];
					break;
				}
			}
			if (addThis) {
				if (/* j < x2.index1stOverlaping && */ !isValidation)
					increment(dbState, log);
				records.add(log);
			}
			else{ // TODO: check with mr1
				if (!isValidation)
					increment(dbState, log);
			}
			
		}
	}

	public Schedule() {
		
		this("NA",null);
	}

	private void increment(HashMap<String, LinkedList<DBState>> dbState, LogRecord log) {
		for (Entity e : log.getEntities()) {
			String key = e.getEntityKey();
			LinkedList<DBState> ll;
			if ((ll = dbState.get(key)) == null)
				continue;

			// TODO part of 1================
			// if (ll.isEmpty()) {
			// ValidationMain.dbState.remove(key);
			// continue;
			// }
			// ==============================

			assert ll.isEmpty() == false : "Empty linkedlist with key = " + key;
			if (!impactedStates.containsKey(key)) {
				DBState st = ll.getFirst();
				impactedStates.put(key, st);
				increment(st, key);
			}
		}
	}

	private void increment(DBState st, String key) {
		
			assert st.getRefrenceCount() >= 0 : "Incorrect value: it should be 0 or more. st.getRefrenceCount() = " + st.getRefrenceCount();
		st.increment();
	}

	public void add(LogRecord e) {
		records.add(e);
	}

	public int size() {
		return records.size();
	}

	public LogRecord getLogRecord(int i) {
		return records.get(i);
	}

	public ArrayList<LogRecord> getOverlap(long et) {
		ArrayList<LogRecord> result = new ArrayList<LogRecord>();
		boolean found = false;
		for (int i = records.size() - 1; i >= 0; i--) {
			LogRecord l = records.get(i);
			if (!found && l.getEndTime() <= et) {
				// index1stOverlaping = (short) (i + 1);
				found = true;
			}
//			if (l.intersect(et) && (!result.contains(l)))
			if (l.getEndTime() > et && (!result.contains(l)))
				result.add(l);
		}
		return result;
	}

	public void duplicate(Schedule s) {
		for (LogRecord log : s.records)
			records.add(log);

		impactedStates = s.impactedStates;
	}

	public void duplicateNoRecords(Schedule s) {
		impactedStates = s.impactedStates;

	}

	public void remove(int i) {
		records.remove(i);
	}
	
	public boolean same(Schedule s) {
		// return sameRecords(s) ? sameImpacted(s) : false;
		return sameImpacted(s) ? sameRecords(s) : false;

		// compare impacted;

	}

	private boolean sameImpacted(Schedule s) {
		if (impactedStates.size() != s.impactedStates.size()) {
			return false;
		}

		Set<String> keys = impactedStates.keySet();
		for (String key : keys) {
			DBState s2 = s.impactedStates.get(key);
			if (s2 == null)
				return false;
			else {
				if (!impactedStates.get(key).equals(s2)) {
					return false;
				}
			}
		}
		return true;
	}

	public boolean sameRecords(Schedule s) {
		if (records.size() != s.records.size())
			return false;
		for (int i = 0; i < records.size(); i++) {
			if (!records.get(i).equals(s.records.get(i))) // TODO: check this if
				// if (!records.get(i).getId().equals(s.records.get(i).getId()))
				return false;
		}
		return true;
	}
	
	public void setParent(ArrayList<Schedule> p) {
	
	parent=p;
	}

	public ArrayList<Schedule> getParents() {
		return parent;
	}

	public void addParent(Schedule p) {
		if (!this.parent.contains(p))
			this.parent.add(p);
		
		assert this.getParents().size()<=1:"Has "+this.getParents().size()+" parents!";

	}

	public boolean isReadMatch(HashMap<String, LinkedList<DBState>> dbState,LogRecord r, boolean generateRecords, HashMap<String, String> expectedValues, ArrayList<EntitySpec> generatedEntities, boolean[] hasGeneratedState, long[] updateTime) {
		int trueCount = 0, propertiesCount = 0;
		hasGeneratedState[0]=false;
		HashMap<String, String> computedProperties = new HashMap<String, String>();
		HashMap<String, Boolean> computedPropertiesStatus = new HashMap<String, Boolean>();
		for (Entity e : r.getEntities()) {
			for (Property p : e.getProperties()) {
				if (p.getType() == ValidationParams.VALUE_READ || p.getType() == ValidationParams.VALUE_NA) {
					computedProperties.put(e.getName() + ValidationParams.KEY_SEPERATOR + e.getKey() + ValidationParams.KEY_SEPERATOR + p.getName(), null);
					computedPropertiesStatus.put(e.getName() + ValidationParams.KEY_SEPERATOR + e.getKey() + ValidationParams.KEY_SEPERATOR + p.getName(), false);
					propertiesCount++;
				}
			}

		}

		boolean startCompute = false;
		for (int i = records.size() - 1; i >= 0; i--) {
			long updateT=0;
			LogRecord record = records.get(i);
			if (!record.isCreatedFromRead() && record.getType()!=ValidationParams.READ_RECORD)
				updateT=record.getOriginalEndTime();
			if (startCompute) {
				if (trueCount >= propertiesCount)
					break;
				for (Entity e : records.get(i).getEntities()) {
					for (Property p : e.getProperties()) {
						if(ValidationParams.SHRINK_BUCKET && p==null)
							continue;
						String pKey = Property.getProprtyKey(e, p);
						// is this property being read?
						if (computedProperties.containsKey(pKey)) {
							// is this property NOT updated with new value later?
							if (!computedPropertiesStatus.get(pKey)) {
								if (p.getType() == ValidationParams.NEW_VALUE_UPDATE) {								
									computedPropertiesStatus.put(pKey, true);
									trueCount++;
									String value = computedProperties.get(pKey);
									if (value==null) {
										updateTime[0]=Math.max(updateTime[0], updateT);
										computedProperties.put(pKey, p.getValue());
									} else {
										double v = Double.parseDouble(value);
										v += Double.parseDouble(p.getValue());
										String vString = ValidationParams.DECIMAL_FORMAT.format(v);
										computedProperties.put(pKey, vString);
									}
								} else if (p.getType() == ValidationParams.INCREMENT_UPDATE) {
									String value = computedProperties.get(pKey);
									if (value==null){
										value="0";
										updateTime[0]=Math.max(updateTime[0], updateT);

									}
									value = ValidationParams.DECIMAL_FORMAT.format(Double.parseDouble(value) + Double.parseDouble(p.getValue()));
									computedProperties.put(pKey, value);
								} else if (p.getType() == ValidationParams.VALUE_DELETED) {
									computedProperties.put(pKey, ValidationParams.DELETED_STRING);
									computedPropertiesStatus.put(pKey, true);
									updateTime[0]=Math.max(updateTime[0], updateT);

									trueCount++;
								}
							}
						}
					}
				}
				// if(records.get(i).)
			} else {
				if (records.get(i).getId().equals(r.getId())) {
					startCompute = true;
				}
			}
		} // end processing all records in SS
		
	//assert updateTime[0]==0;
		for (Entity e : r.getEntities()) {
			for (Property p : e.getProperties()) {
				if (p.getType() == ValidationParams.VALUE_READ || p.getType() == ValidationParams.VALUE_NA) {
					String pKey = Property.getProprtyKey(e, p);
					String value = computedProperties.get(pKey);
					boolean status = computedPropertiesStatus.get(pKey);
					if (!status) {
						DBState st = null;
						String entityKey = getEntityKey(pKey);
						if ((st = impactedStates.get(entityKey)) == null) {
							LinkedList<DBState> ll = dbState.get(entityKey);
							if ((ll == null) || ((st = ll.getFirst()) == null)) {

								if (p.getType() != ValidationParams.VALUE_NA) {// TODO debug point
									// create a new DB state, this is new 
									if (!ValidationParams.hasInitState){ //TODO: add && generateRecords
//									int entityindex=-1;
//									for(int i=0; i<ValidationConstants.ENTITY_NAMES.length; i++){
//										if (e.name.equals(ValidationConstants.ENTITY_NAMES[i])){
//											entityindex=i;
//											break;
//										}
//									}
//									String props[]= new String[ValidationConstants.ENTITY_PROPERTIES[entityindex].length];
//									for (int i=0; i<props.length;i++){
//										if (p.getName().equals(ValidationConstants.ENTITY_PROPERTIES[entityindex][i])){
//											props[i]=p.getValue();
//										}
//										else{
//											props[i]= null;
//										}
//										
//									}
									
									Property p1= new Property(p.getName(), p.getValue(), ValidationParams.NEW_VALUE_UPDATE);
									// check entity exist in generated
									EntitySpec eg=null;
									boolean exist=false;
									for (EntitySpec eGen:  generatedEntities){
										
										if (eGen.getKey().equals(e.key) &&eGen.getName().equals(e.name) ){
											eg=eGen;
											for (Property pg: eg.getPropertiesArrayLis()){
												if (p.getName().equals(pg.getName())){
													exist=true;
													break;
												}
											}
											break;
										}
									}
									if (eg==null){
										eg= new EntitySpec(e.key, e.name, null);
										generatedEntities.add(eg);
										eg.setPropertiesArrayLis(new ArrayList<Property>());
									}
									
									if (!exist)
									eg.getPropertiesArrayLis().add(p1);
								
									if (generateRecords){
										hasGeneratedState[0]=true;
									continue;
									}
									}
										return false;
								} else
									continue;
							}
						}
						

						String entityName = e.getName();
						int index = -1;
						for (int i = 0; i < ValidationParams.ENTITY_NAMES.length; i++) {
							if (entityName.equals(ValidationParams.ENTITY_NAMES[i])) {
								index = i;
								break;
							}
						}

						String pValue = null;
						int propIndex=-1;

						for (int i = 0; i < ValidationParams.ENTITY_PROPERTIES[index].length; i++) {
							if (ValidationParams.ENTITY_PROPERTIES[index][i].equals(p.getName())) {
								if (i>=st.getValue().length && st.getValue()[0].equals(ValidationParams.NULL_STRING)){
									i=0;
								}
								String stCurrentValue = st.getValue()[i];
								pValue = stCurrentValue;
								propIndex=i;
								break;
							}
						}
						if (computedProperties.get(pKey)==null){
							 //TODO: is it correct to only update time if it is not set yet?
							if (ValidationParams.COMPUTE_FRESHNESS)
							updateTime[0]= Math.max(updateTime[0],st.updateEndTimes[propIndex]);
							}
							

						
						if (pValue==null){
						// this is new value
							if(!ValidationParams.hasInitState ){
							Property p1= new Property(p.getName(), p.getValue(), ValidationParams.NEW_VALUE_UPDATE);
							// check entity exist in generated
							EntitySpec eg=null;
							boolean exist=false;
							for (EntitySpec eGen:  generatedEntities){
								
								if (eGen.getKey().equals(e.key) &&eGen.getName().equals(e.name) ){
									eg=eGen;
									for (Property pg: eg.getPropertiesArrayLis()){
										if (p.getName().equals(pg.getName())){
											exist=true;
											break;
										}
									}
									break;
								}
							}
							if (eg==null){
								eg= new EntitySpec(e.key, e.name, null);
								generatedEntities.add(eg);
								eg.setPropertiesArrayLis(new ArrayList<Property>());
							}
						if(!exist)	
							eg.getPropertiesArrayLis().add(p1);
						value=p.getValue();
						if (generateRecords){
							value=p.getValue();
							hasGeneratedState[0]=true;
						}
							else
								return false;
//						
						}
						}
						else{
						if (value==null) {
							// not exist in computed
							value = pValue;
							
						} else {
							value = Utilities.applyIncrements(value, pValue);
						}
						}
						// }
						// }
					} // end ! status

//					if (ValidationMain.readLogsCount == 5976) {
//						if (!compareValues(value, p.getValue())) {
//							System.out.printf("From the validator (%s == %s) Expected \n", value, p.getValue());
//						}
//					}
//					if(expectedValues != null){
//						expectedValues.put(e.getName() + ValidationConstants.KEY_SEPERATOR + e.getKey() + ValidationConstants.KEY_SEPERATOR + p.getName(), value);
//					}
					if (value.equals(ValidationParams.DELETED_STRING)) {
						if (p.getType() != ValidationParams.VALUE_NA) {
							if(expectedValues != null){
								expectedValues.put(e.getName() + ValidationParams.KEY_SEPERATOR + e.getKey() + ValidationParams.KEY_SEPERATOR + p.getName(), value);
							}
							return false;
						}
					} else if (!compareValues(value, p.getValue())) {// TODO debug point
						
						if(expectedValues != null){
							expectedValues.put(e.getName() + ValidationParams.KEY_SEPERATOR + e.getKey() + ValidationParams.KEY_SEPERATOR + p.getName(), value);
						}
						return false;
					}
				}
			}
		}
		return true;
	}

	static boolean compareValues(String value1, String value2) {
		if (isNumeric(value1) && isNumeric(value2)) {
			
			double a = Double.parseDouble(value1);
			double b = Double.parseDouble(value2);
			double c = Math.abs(a - b);
			if (Double.isNaN(c) || Double.isInfinite(c) )
				return value1.equals(value2);
			return ValidationParams.ERROR_MARGIN > c;

		} else {
			return value1.equals(value2);
		}
	}

	public static boolean isNumeric(String value) {
		try {
			if (ValidationParams.ALL_STRING)
			return false;
			Double.parseDouble(value);
			return true;
		} catch (Exception e) {
			return false;
		}

	}

	public void addAllParents(ArrayList<Schedule> parents) {
		for (Schedule s : parents) {
			if (!this.parent.contains(s)) {
				s.parent.clear();
				this.parent.add(s);
			}
		}
		//assert this.getParents().size()<=1:"Has "+this.getParents().size()+" parents!";

	}

	public void shrink(long endTime, ValidatorData vd, ArrayList<CandidateValue> documentList) {
		
		 
		HashMap<String, String> computedProperties = new HashMap<String, String>();
		HashMap<String, Long> computedPropertiesEndTime = new HashMap<String, Long>();

		HashMap<String, Boolean> computedPropertiesStatus = new HashMap<String, Boolean>();

		Stage1_ShrinkRecordsIntoStates_v2(vd.collapsedIntervals,computedProperties, computedPropertiesStatus, endTime,computedPropertiesEndTime);
		Stage2_ApplyStatesToDBStateAndImpactedEntites(vd.dbState,computedProperties, computedPropertiesStatus,documentList,computedPropertiesEndTime);
	
	}

	private void Stage1_ShrinkRecordsIntoStates_v2( Set<LogRecord> collapsedIntervals,HashMap<String, String> computedProperties, HashMap<String, Boolean> computedPropertiesStatus, long et, HashMap<String, Long> computedPropertiesEndTime) {
		
		boolean startComputing = false;
		int currentGroup = -1;
		
		for (int i = records.size() - 1; i >= 0; i--) {
			long updateTime=0;
			LogRecord record = records.get(i);
			if (!record.isCreatedFromRead())
				updateTime=record.getOriginalEndTime();
			// TODO review later
			if (currentGroup == -1) {
				currentGroup = records.get(i).getGroupNum();
			} else if (currentGroup != records.get(i).getGroupNum()) {
				currentGroup = records.get(i).getGroupNum();
				startComputing = false;
			}
			if ((!startComputing) && records.get(i).getEndTime() /* >= */ > et) {
				continue;
			}
			startComputing = true;
			
			for (Entity e : records.get(i).getEntities()) {
				
				for (Property p : e.getProperties()) {
					if (p == null)
						continue;
					String pKey = Property.getProprtyKey(e, p);
					// is this property being read?
					if (!computedProperties.containsKey(pKey)) {
						computedProperties.put(pKey, "0");
						computedPropertiesStatus.put(pKey, false);
					}
					// is this property NOT updated with new value later
					if (!computedPropertiesStatus.get(pKey)) {
						if (p.getType() == ValidationParams.NEW_VALUE_UPDATE) {
							computedPropertiesStatus.put(pKey, true);
							String value = computedProperties.get(pKey);
							if (value.equals("0")) {
								computedProperties.put(pKey, p.getValue());
								computedPropertiesEndTime.put(pKey, updateTime);
							} else { // there is an increment value
								String vString = Utilities.applyIncrements(value, p.getValue());
								computedProperties.put(pKey, vString);
							}
						} else if (p.getType() == ValidationParams.INCREMENT_UPDATE) {
							String value = computedProperties.get(pKey);
							if (value.equals("0")){
								computedPropertiesEndTime.put(pKey, updateTime);

							}
							value = Utilities.applyIncrements(value, p.getValue());
							computedProperties.put(pKey, value);
						} else if (p.getType() == ValidationParams.VALUE_DELETED) {
							computedPropertiesStatus.put(pKey, true);
							computedPropertiesEndTime.put(pKey, updateTime);
							computedProperties.put(pKey, ValidationParams.DELETED_STRING);

						}
					}

				} // end property
			} // done looping all entities in current log record
//			if(records.get(i).getId().equals("2-13734"))
//				System.out.println();
			collapsedIntervals.add(records.get(i));
			if (shrinked==null)
				shrinked=new ArrayList<LogRecord>();
			shrinked.add(records.get(i)); // inserted in reverse order
			records.remove(i);

		} // done looping all records			
	}

	public HashMap<String,Character> countDiscardedWrites(HashMap<String, LinkedList<DBState>> dbState,long et) {
		HashMap<String, Character> discardedRecs= new HashMap<String, Character> ();
		HashSet <String> propertiesWithState= new HashSet<String>();
		boolean startComputing = false;
		int currentGroup = -1;
		for (int i =0; i<records.size() ; i++) {
			boolean partial=false;
			// TODO review later
			if (currentGroup == -1) {
				currentGroup = records.get(i).getGroupNum();
			} else if (currentGroup != records.get(i).getGroupNum()) {
				currentGroup = records.get(i).getGroupNum();
				startComputing = false;
			}
			if ((!startComputing) && records.get(i).getEndTime() /* >= */ > et) {
				continue;
			}
			startComputing = true;
			int allPropsCount=0;
			int discardedPropsCount=0;
			for (Entity e : records.get(i).getEntities()) {
				allPropsCount=0;
				discardedPropsCount=0;
				for (Property p : e.getProperties()) {
					if (p == null)
						continue;
					allPropsCount++;
					String pKey = Property.getProprtyKey(e, p);
					if (propertiesWithState.contains(pKey))
						continue;
					if(p.getType()==ValidationParams.INCREMENT_UPDATE){
						//check impacted
						String ekey= e.getEntityKey();
						DBState eState= impactedStates.get(ekey);
						if (eState==null){
							LinkedList<DBState> ll = dbState.get(ekey);
							if(ll!=null){
								eState=ll.getFirst();
							}
							
						}
						if (eState==null){
							discardedPropsCount++;
						}
						else{
							int pIndex=e.getPropertyIndex(p.getName());
							if (eState.getValue()[pIndex]==null){
								discardedPropsCount++;

							}
						}
						
					}
					else{
						// NVU
						if(p.getType()==ValidationParams.NEW_VALUE_UPDATE){
						propertiesWithState.add(pKey);
						}
					}

				} // end property
				
				// after we are done with an entity, we add log record as discarded if it has at least 1 prop
				// if we are adding as full, we shouldn't overwrite previous entry as it may be P. If it is P we can add always.
				
				if(discardedPropsCount>0){
					
					if (discardedPropsCount<allPropsCount)
						discardedRecs.put(records.get(i).getId(),'P');
					else{
						if (!discardedRecs.containsKey(records.get(i).getId()))
						discardedRecs.put(records.get(i).getId(),'F');
					}

					}
				else{
					// we are not discarding this entity
					if (discardedRecs.containsKey(records.get(i).getId()))
						// we convert it to P
						discardedRecs.put(records.get(i).getId(),'P');
				}
				
			} // done looping all entities in current log record
			
		

		} // done looping all records
		return discardedRecs;
	}

	private void Stage1_ShrinkRecordsIntoStates(HashMap<String, String> computedProperties, HashMap<String, Boolean> computedPropertiesStatus, long et) {
		boolean startComputing = false;
		for (int i = records.size() - 1; i >= 0; i--) {
			// TODO review later
			if ((!startComputing) && records.get(i).getEndTime() /* >= */ > et) {
				continue;
			}
			startComputing = true;
			for (Entity e : records.get(i).getEntities()) {
				for (Property p : e.getProperties()) {
					String pKey = Property.getProprtyKey(e, p);
					// is this property being read?
					if (!computedProperties.containsKey(pKey)) {
						computedProperties.put(pKey, "0");
						computedPropertiesStatus.put(pKey, false);
					}
					// is this property NOT updated with new value later
					if (!computedPropertiesStatus.get(pKey)) {
						if (p.getType() == ValidationParams.NEW_VALUE_UPDATE) {
							computedPropertiesStatus.put(pKey, true);

							String value = computedProperties.get(pKey);
							if (value.equals("0")) {
								computedProperties.put(pKey, p.getValue());
							} else { // there is an increment value
								String vString = Utilities.applyIncrements(value, p.getValue());
								computedProperties.put(pKey, vString);
							}
						} else if (p.getType() == ValidationParams.INCREMENT_UPDATE) {
							String value = computedProperties.get(pKey);
							value = Utilities.applyIncrements(value, p.getValue());
							computedProperties.put(pKey, value);
						}
					}

				}
			} // done looping all entities in current log record
//			ValidationMain.NotAllowedList.remove(records.get(i).getId()+"Before"); TODO
//			ValidationMain.NotAllowedList.remove(records.get(i).getId()+"After"); TODO
			records.remove(i);

		} // done looping all records
	}

	private void Stage2_ApplyStatesToDBStateAndImpactedEntites(	HashMap<String, LinkedList<DBState>> dbState,HashMap<String, String> computedProperties, HashMap<String, Boolean> computedPropertiesStatus, ArrayList<CandidateValue> documentList, HashMap<String, Long> computedPropertiesEndTime) {
		HashMap<String, DBState> entitiesKeys = new HashMap<String, DBState>();
		
		for (String key : computedProperties.keySet()) {
			// TODO debug point
			String entityName = getEntityName(key);
			String entityKey = getEntityKey(key);
			String pKey= entityKey.substring(entityKey.indexOf(ValidationParams.KEY_SEPERATOR)+1);
			// shrinkState(entityKey, newValue);

			DBState newState = null;
			if (!entitiesKeys.containsKey(entityKey)) {
				entitiesKeys.put(entityKey, null);
				if (computedProperties.get(key).equals(ValidationParams.DELETED_STRING)) {
					
					
					newState = new DBState(1, computedPropertiesEndTime.get(key),Validator.deletedArr);
					

		
				} else {
					// not delete
					newState = getNewState(dbState,entityKey, entityName, computedProperties, computedPropertiesStatus,true,computedPropertiesEndTime);
					if (!ValidationParams.hasInitState){
					if (newState==null)
						continue;
					}
					assert newState.updateEnd>=0;

					

				}
			} else { // key already exist
				if (entitiesKeys.get(entityKey) == null)
					continue;
				//This code is not reachable
//				newState = entitiesKeys.get(entityKey);
//				entitiesKeys.put(entityKey, null);
			}

			LinkedList<DBState> ll = dbState.get(entityKey);

			DBState state = impactedStates.get(entityKey);

			if (state == null) {// not in my impacted hashmap
				// add to document list
				
				if (ll == null) { // This is a new entity (insert), it doesn't have an old state
					ll = new LinkedList<DBState>();
					dbState.put(entityKey, ll);
				}
				boolean found = false;
				for (DBState s : ll) {// loop existing properties, id similar found, point to it
					if (s.same(newState)) {
						documentList.add(new CandidateValue(entityName, pKey, this.sid, s));
						found = true;
						if (ValidationParams.COMPUTE_FRESHNESS)
						{
						s.updateEnd=newState.updateEnd;
						s.updateEndTimes=newState.updateEndTimes;
						}
						impactedStates.put(entityKey, s);
						assert s.getRefrenceCount() >= 0 : "Incorrect value: it should be 0 or more. s.getRefrenceCount() = " + s.getRefrenceCount();
						s.increment();
						break;
					}
				}
				if (!found) {// no similar found
					ll.add(newState);
					impactedStates.put(entityKey, newState);
					documentList.add(new CandidateValue(entityName, pKey, this.sid, newState));
				}
			} else {
				// state already exists in impacted
				if (!state.same(newState)) {
					// add to document list
					

					// impactedEntities.remove(key);
					
					assert state.getRefrenceCount() >= 1 : "Incorrect value: it should be 1 or more. state.getRefrenceCount() = " + state.getRefrenceCount();
					// TODO part of 1================
					destructor_rec2(dbState,entityKey, state);
					// if (state.getRefrenceCount() == 0) {
					// ll.remove(state);
					// assert ll.isEmpty() == false : "ERROR: linkedlist is empty for key = " + entityKey;
					// }

					boolean found = false;
					for (DBState s : ll) {
						if (s.same(newState)) {
							// TODO part of 2================
							// removeFromLinkedList(entityKey, newState);
							found = true;
							if (ValidationParams.COMPUTE_FRESHNESS)
							{
							s.updateEnd=newState.updateEnd;
							s.updateEndTimes=newState.updateEndTimes;
							}
							documentList.add(new CandidateValue(entityName, pKey, this.sid, s));
							impactedStates.put(entityKey, s);
							assert s.getRefrenceCount() >= 0 : "Incorrect value: it should be 0 or more. s.getRefrenceCount() = " + s.getRefrenceCount();
							s.increment();
							break;
						}
					}
					if (!found) {
						documentList.add(new CandidateValue(entityName, pKey, this.sid, newState));
						ll.add(newState);
						dbState.put(entityKey, ll);
						impactedStates.put(entityKey, newState);
					}
				}
				else{
					//state same as impacted
					if (ValidationParams.COMPUTE_FRESHNESS)
					{
					state.updateEnd=newState.updateEnd;
					state.updateEndTimes=newState.updateEndTimes;
					}

				}
				
			}
		}

	}

	private void destructor_rec2(HashMap<String, LinkedList<DBState>> dbState,String key, DBState st) {
		st.decrement();
		if (st.getRefrenceCount() == 0) {
			LinkedList<DBState> ll = dbState.get(key);
			ll.remove(st);
		}
	}

	DBState getNewState(HashMap<String, LinkedList<DBState>> dbState,String entityKey, String entityName, HashMap<String, String> computedProperties, HashMap<String, Boolean> computedPropertiesStatus, boolean checkHasOld, HashMap<String, Long> computedPropertiesEndTime) {
		
		int index = -1;
		for (int i = 0; i < ValidationParams.ENTITY_NAMES.length; i++) {
			if (entityName.equals(ValidationParams.ENTITY_NAMES[i])) {
				index = i;
				break;
			}
		}

		LinkedList<DBState> ll = dbState.get(entityKey);
		DBState oldState = impactedStates.get(entityKey);
		if (oldState == null && ll != null)
			oldState = ll.getFirst();

		String[] values = new String[ValidationParams.ENTITY_PROPERTIES[index].length];
		long[] updateTimes = new long[ValidationParams.ENTITY_PROPERTIES[index].length];

		long updateTime=0;
		for (int i = 0; i < ValidationParams.ENTITY_PROPERTIES[index].length; i++) {
			String pKey = entityKey + ValidationParams.KEY_SEPERATOR + ValidationParams.ENTITY_PROPERTIES[index][i];
			if (computedPropertiesStatus.containsKey(pKey)) {
				long update=0;
				if (computedPropertiesEndTime!=null){
					update=computedPropertiesEndTime.get(pKey);
				}
					updateTime=Math.max(updateTime,update);
				if (computedPropertiesStatus.get(pKey)) {
					
					if (computedProperties.get(pKey).equals(ValidationParams.DELETED_STRING)){
						long deleteTime=0;
						if (computedPropertiesEndTime!=null){
							deleteTime=computedPropertiesEndTime.get(pKey);
						}
						return new DBState(1, deleteTime,Validator.deletedArr);
					}
					values[i] = computedProperties.get(pKey);
					updateTimes[i]=update;
				} else {
	

					if ((oldState!=null &&oldState.getValue()[i]!=null) || ValidationParams.hasInitState ){
						assert oldState != null : "An increment with no old state. EntityKey = " + entityKey + " .. index = " + i;
						values[i] = Utilities.applyIncrements(oldState.getValue()[i], computedProperties.get(pKey));
						updateTimes[i]=update;
					}
				}
			}
			// }
			if (values[i] == null) {
				if ((oldState!=null ||ValidationParams.hasInitState) && checkHasOld ){
				assert oldState != null : "An update with no old state. EntityKey = " + entityKey + " .. index = " + pKey;

				values[i] = oldState.getValue()[i];
				if (ValidationParams.COMPUTE_FRESHNESS)
				updateTimes[i]=oldState.updateEndTimes[i];
				}
			}
		}
		if(!ValidationParams.hasInitState){
		boolean allnull= true;
		for (String v: values){
			if(v!=null){
				allnull=false;
				break;
			}
		}
		if (allnull)
		return null;
		}
		return new DBState(1,updateTimes, values);
	}

	public void insertAt(LogRecord r, int index) {
		records.add(index, r);
	}

	public int getRecordIndex(String id) {
		for (int i = 0; i < records.size(); i++) {
			if (records.get(i).getId().equals(id))
				return i;
		}
		return -1;
	}

	public ArrayList<LogRecord> getRecords() {
		return records;
	}

	public LogRecord search(String id) {
		for (LogRecord r : records) {
			if (r.getId().equals(id))
				return r;
		}
		return null;
	}

	public void clearImpacted(HashMap<String, LinkedList<DBState>> dbState) {
		for (String key : impactedStates.keySet()) {
			DBState st = impactedStates.get(key);
			st.decrement();
			
//				if (ValidationMain.dbState.get(key).size() != 1)
//					System.out.println();
//				 if (ValidationMain.dbState.get(key).size() != 1)
//					 System.out.println();
				assert dbState.get(key).size() == 1 : "MORE THAN ONE STATE (" + dbState.get(key).size() + ")";
				assert st.getRefrenceCount() == 0 : "IT'S NOT ZERO. key = " + key + "Ref" + st.getRefrenceCount();
			
		}
		impactedStates.clear();
	}

	private void decrement(DBState st, String key) {
		st.decrement();
		assert st.getRefrenceCount() >= 0 : "IT'S LESS THAN ZERO";
	}

	public static String getEntityName(String key) {
		return key.substring(0, key.indexOf(ValidationParams.KEY_SEPERATOR));
	}

	public static String getEntityKey_WithSeperator(String key) {
		return key.substring(0, key.lastIndexOf(ValidationParams.KEY_SEPERATOR) + 1);
	}

	public static String getEntityKey(String key) {
		return key.substring(0, key.lastIndexOf(ValidationParams.KEY_SEPERATOR));
	}

	public void destructor(HashMap<String, LinkedList<DBState>> dbState) {
		for (String key : impactedStates.keySet()) {
			DBState st = impactedStates.get(key);
			destructor_rec(dbState,key, st);
		}
		impactedStates.clear();
	}

	private void destructor_rec(HashMap<String, LinkedList<DBState>> dbState,String key, DBState st) {
		st.decrement();
		if (st.getRefrenceCount() == 0) {
			LinkedList<DBState> ll = dbState.get(key);
//			if (ll == null && st == Validator.Deleted) {

		//	} else {
				ll.remove(st);
				if (ll.isEmpty()) {
					// TODO part of 1================
					dbState.remove(key);
				}
		//	}
		}
	}

	public void updateImpcated(HashMap<String, LinkedList<DBState>> dbState) {// nothing is shrieked here
		for (int j = 0; j < records.size(); j++) {
			LogRecord log = records.get(j);
			increment(dbState,log);
		}
	}

	private int debug_containLogRecord(String ID) {
		int index = -1;
		for (int i = 0; i < records.size(); i++) {
			if (records.get(i).getId().equals(ID))
				return i;
		}
		return index;
	}

	public void append(Schedule s) {
		for (int i = 0; i < s.records.size(); i++) {
			records.add(s.records.get(i));
		}
	}

	public void serialize(DataOutputStream os, HashMap<String, LinkedList<DBState>> dbState, HashSet<String> logs, DataOutputStream osLog) throws IOException {
		
	
		byte[] bID = sid.getBytes();
		os.writeInt(bID.length);
		os.write(bID);
		int numRecs= this.size();
		os.writeInt(numRecs);
		for (int i=0;i<numRecs;i++){
			LogRecord r= this.getLogRecord(i);
			r.serializeID(os);
			if (!logs.contains(r.getId()));
			{
				r.serialize(osLog);
				logs.add(r.getId());
			}
		}
		int numKeys= impactedStates.size();
		os.writeInt(numKeys);
		for (String key:impactedStates.keySet()){
			byte[] b = key.getBytes();
			os.writeInt(b.length);
			os.write(b);
			LinkedList<DBState> ll = dbState.get(key);
			int index=ll.indexOf(impactedStates.get(key));
			os.writeInt(index);
		
		}
		
	}

	public static Schedule deserialize(DataInputStream is, HashMap<String, LinkedList<DBState>> db,
			HashMap<String, LogRecord> logs) throws IOException {
		int sidSize=is.readInt();
		
		byte sidbuffer[]= new byte[sidSize];
		is.read(sidbuffer, 0, sidSize);
		String sid= new String(sidbuffer);
		Schedule schedule= new Schedule();
		schedule.sid=sid;
		schedule.newSchedule=false;
		int numRecs= is.readInt();
		
		for (int i=0;i<numRecs;i++){
			int logIdSize=is.readInt();
			byte b[]= new byte[logIdSize];
			is.read(b, 0, logIdSize);
			String id= new String (b);
			LogRecord r= logs.get(id);
			assert r!=null: "log is null!!";
			schedule.records.add(r);
		}
		int numKeys= is.readInt();
		
		for (int i=0; i<numKeys;i++){
			int keySize= is.readInt();
			byte[] b = new byte[keySize];
			is.read(b,0,keySize);
			String key= new String(b);
			
			LinkedList<DBState> ll = db.get(key);
			int index=is.readInt();
			schedule.impactedStates.put(key, ll.get(index));
		
		}
		return schedule;
	}


}
