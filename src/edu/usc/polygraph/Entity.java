package edu.usc.polygraph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.usc.polygraph.snapshot.Snapshot;

public class Entity {
	protected String key;
	protected String name;
	protected Property[] properties;
	

	public Entity(String key, String name, Property[] properties) {
		this.key = key;
		this.name = name;
		this.properties = properties;
	}

	private Entity() {

	}

	// public Entity(String entityName, String entityKey) {
	// for (int i = 0; i < TPCCConstants.ENTITY_NAMES.length; i++) {
	// if (TPCCConstants.ENTITY_NAMES[i].equals(entityName)) {
	// properties = new Property[TPCCConstants.ENTITY_PROPERTIES[i].length];
	// for (int j = 0; j < TPCCConstants.ENTITY_PROPERTIES[i].length; j++) {
	// properties[j] = new Property(TPCCConstants.ENTITY_PROPERTIES[i][j], "", TPCCConstants.NEW_VALUE_UPDATE);
	// }
	// break;
	// }
	// }
	// this.key = entityKey;
	// this.name = entityName;
	// }

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Property[] getProperties() {
		return properties;
	}

	public void setProperties(Property[] properties) {
		this.properties = properties;
	}

	public Entity getCopy() {
		Property[] newPA = new Property[this.properties.length];
		for (int i = 0; i < properties.length; i++)
			newPA[i] = properties[i].getCopy();
		Entity result = new Entity(key, name, newPA);
		return result;
	}

	public boolean same(Entity e) {
		if (properties.length != e.properties.length)
			return false;
		for (int i = 0; i < properties.length; i++) {
			if (!properties[i].getValue().equals(e.properties[i].getValue())) {
				return false;
			}
		}
		return true;
	}

	public int getPropertyIndex(String pname) {
		String entityName = this.getName();
		int index = -1;
		for (int i = 0; i < ValidationParams.ENTITY_NAMES.length; i++) {
			if (entityName.equals(ValidationParams.ENTITY_NAMES[i])) {
				index = i;
				break;
			}
		}

		String pValue = null;
		int propIndex = -1;
		for (int i = 0; i < ValidationParams.ENTITY_PROPERTIES[index].length; i++) {
			if (ValidationParams.ENTITY_PROPERTIES[index][i].equals(pname)) {
				propIndex = i;
				break;
			}
		}
		return propIndex;
	}

	public String getEntityKey() {
		return Utilities.concat(name, ValidationParams.KEY_SEPERATOR, key);
	}

	public void serialize(DataOutputStream out) throws IOException {
		byte[] bkey = key.getBytes();
		out.writeInt(bkey.length);
		out.write(bkey);
		byte[] bName = name.getBytes();
		out.writeInt(bName.length);
		out.write(bName);
		out.writeInt(properties.length);
		for (Property p : properties) {
			if (p == null) {
				out.writeInt(Snapshot.nullObject);
			} else {
				p.serialize(out);
			}
		}
	}

	public static Entity deserialize(DataInputStream in) throws IOException {
		Entity e = new Entity();
		int size = in.readInt();
		byte[] buffer = new byte[size];
		in.read(buffer, 0, size);
		e.key = new String(buffer);
		size = in.readInt();
		buffer = new byte[size];
		in.read(buffer, 0, size);
		e.name = new String(buffer);
		size = in.readInt();
		e.properties = new Property[size];
		for (int i = 0; i < size; i++) {
			e.properties[i] = Property.deserialize(in);
		}
		return e;
	}

	public String toPrint() {
		String result = String.format("%s;%s;", name, key);
		String seperator = "";
		for (Property p : properties) {
			result += seperator + p.toPrint();
			seperator = "#";
		}
		return result;
	}

}
