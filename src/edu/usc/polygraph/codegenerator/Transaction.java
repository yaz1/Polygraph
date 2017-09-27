package edu.usc.polygraph.codegenerator;

import java.util.*;

import edu.usc.polygraph.Entity;
import edu.usc.polygraph.Property;
import edu.usc.polygraph.ValidationParams;

public class Transaction {
	public Transaction(String name) {
		this.name=name;
		this.topic=null;
	}
	String topic;
	String name;
	String key;
	char type=' ';
	ArrayList<Entity> entities;
	ArrayList<ConflictedTransaction>conflicted;
	ArrayList<String> relationships;
	HashMap<String,String> propsVars;


	@Override
	public String toString(){
		StringBuilder sb= new StringBuilder();
		sb.append("TransName="+name+"\n");
		sb.append("TransTopic="+topic+"\n");
		for (Entity e:entities){
			sb.append("Entity:"+e.getName()+"\n");
			sb.append("numProps:"+e.getProperties().length+"\n");


		}
		sb.append("Conflicts:");
		for (ConflictedTransaction conflict: conflicted){
			sb.append(conflict.key+",");
		}
		sb.append("\npartitioning key:"+key);

		return sb.toString();

	}
	public void setType(){
		for (Entity e: entities){
			if (e.getProperties()==null)
				continue;
			for (Property p: e.getProperties()){
				if(type==' '){
					switch (p.getType()) {
					case ValidationParams.NEW_VALUE_UPDATE:
					case ValidationParams.VALUE_DELETED:
					case ValidationParams.DECREMENT_UPDATE_INTERFACE:
					case ValidationParams.INCREMENT_UPDATE:
						type = ValidationParams.UPDATE_RECORD;
						break;
					case ValidationParams.VALUE_READ:
					case ValidationParams.VALUE_NA:
						type = ValidationParams.READ_RECORD;
						break;
					}
				}
				else if (type==ValidationParams.READ_RECORD){
					if (p.getType()==ValidationParams.NEW_VALUE_UPDATE || p.getType()==ValidationParams.VALUE_DELETED ||p.getType()==ValidationParams.INCREMENT_UPDATE||p.getType()==ValidationParams.DECREMENT_UPDATE_INTERFACE)
						type=ValidationParams.READ_WRITE_RECORD;
				}
				else if (type==ValidationParams.UPDATE_RECORD){
					if (p.getType()==ValidationParams.VALUE_READ ||p.getType()==ValidationParams.VALUE_NA)
						type=ValidationParams.READ_WRITE_RECORD;
				}
				if (type==ValidationParams.READ_WRITE_RECORD){
					break;

				}
			} // prop loop
			if (type==ValidationParams.READ_WRITE_RECORD)
				break;
		
	} // entity loop
}
}
class ConflictedTransaction{
	Transaction transaction;
	String key;
	public ConflictedTransaction(Transaction t, String k){
		transaction=t;
		key=k;
	}
}