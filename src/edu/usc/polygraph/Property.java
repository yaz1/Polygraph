package edu.usc.polygraph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.usc.polygraph.snapshot.Snapshot;

public class Property {
	private String name;
	private String value;
	private char type = ValidationParams.NO_READ_UPDATE;

	public Property(String name, String value, char type) {
		this.name = name;
		this.value = value;
		this.type = type;
	}
	
	private Property(){
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public Property getCopy() {
		Property p = new Property(name, value, type);
		return p;
	}

	public static String getProprtyKey(Entity e, Property p) {
		StringBuilder sb = new StringBuilder();
		sb.append(e.getName());
		sb.append(ValidationParams.KEY_SEPERATOR);
		sb.append(e.getKey());
		sb.append(ValidationParams.KEY_SEPERATOR);
		sb.append(p.getName());
		return sb.toString();
	}

	public void serialize(DataOutputStream out) throws IOException {
		byte[] bName = name.getBytes();
		out.writeInt(bName.length);
		out.write(bName);
		byte[] bvalue = value.getBytes();
		out.writeInt(bvalue.length);
		out.write(bvalue);
		out.writeChar(type);		
	}

	public static Property deserialize(DataInputStream in) throws IOException {
		Property p = new Property();
		int size = in.readInt();
		if(size == Snapshot.nullObject){
			return null;
		}
		byte[] buffer = new byte[size];
		in.read(buffer, 0, size);
		p.name = new String(buffer);
		size = in.readInt();
		buffer = new byte[size];
		in.read(buffer, 0, size);
		p.value = new String(buffer);
		p.type = in.readChar();
		return p;
	}

	public String toPrint() {
		String result = String.format("%s:%s:%c", name, value, type);
		return result;
	}

	public String getProprtyKey(Entity e) {
		StringBuilder sb = new StringBuilder();
		sb.append(e.getName());
		sb.append(ValidationParams.KEY_SEPERATOR);
		sb.append(e.getKey());
		sb.append(ValidationParams.KEY_SEPERATOR);
		sb.append(getName());
		return sb.toString();
	}
}
