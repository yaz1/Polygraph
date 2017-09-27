package edu.usc.polygraph;

public class CandidateValue {
	String eName;
	String eKey;
	String sid;
	boolean updateDocument=false;
	DBState dbState;

	
	CandidateValue(String name, String key, String sid, DBState state){
		eName=name;
		eKey=key;
		this.sid=sid;
		dbState=state;
	}
	CandidateValue(String name, String key, String sid, DBState state, boolean update){
		this(name, key, sid, state);
		updateDocument=update;
	}
}
