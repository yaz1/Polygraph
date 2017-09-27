package edu.usc.polygraph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;

import edu.usc.polygraph.snapshot.Snapshot;

public class LogRecord implements Comparable<LogRecord> {
	private String id;
	private String actionName;
	private long startTime;
	private long endTime;
	private long originalEndTime;
	

	private char type;
	private Entity[] entities;
	private int partitionID = 0;
	private short groupNum;
	private long offset = -1;
	private int logSize = 0;
	private String queryParams=null;
	private boolean createdFromRead=false;
	public boolean isCreatedFromRead() {
		return createdFromRead;
	}

	public void setCreatedFromRead(boolean createdFromRead) {
		this.createdFromRead = createdFromRead;
	}
	public long getOriginalEndTime() {
		return originalEndTime;
	}

	public void setOriginalEndTime(long originalEndTime) {
		this.originalEndTime = originalEndTime;
	}
	public int getLogSize() {
		return logSize;
	}

	public void setLogSize(int logSize) {
		this.logSize = logSize;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	enum LogRecordToken {
		Type(0), Name(1), ThreadID(2), SequenceID(3), StartTime(4), EndTime(5), Entities(6);
		public final int Index;

		LogRecordToken(int index) {
			Index = index;
		}
	}

	enum LogRecordToken2 {
		Type(0), Name(1), TransactionID(2), StartTime(3), EndTime(4), Entities(5);
		public final int Index;

		LogRecordToken2(int index) {
			Index = index;
		}
	}

	enum EntityToken {
		Name(0), Key(1), Property(2);
		public final int Index;

		EntityToken(int index) {
			Index = index;
		}
	}

	enum PropertyToken {
		Name(0), Value(1), Type(2);
		public final int Index;

		PropertyToken(int index) {
			Index = index;
		}
	}

	public LogRecord(String id, String actionName, long startTime, long endTime, char type, Entity[] entities) {
		super();
		this.id = id;
		this.actionName = actionName;
		this.startTime = startTime;
		this.endTime = endTime;
		this.type = type;
		this.entities = entities;
		this.groupNum = -1;
	}

	public short getGroupNum() {
		return groupNum;
	}

	public void setGroupNum(short groupNum) {
		this.groupNum = groupNum;
	}

	public LogRecord() {

	}

	public static LogRecord createLogRecord(String line) {
		// R,OS,0,2,7535426455819328,7535426459286936,CUSTOMER;1-3-154;BALANCE:-10.0:R#YTD_P:10.0:R#P_CNT:1:R#LAST_O_ID:2564:R&ORDER;1-3-154-2564;OL_CNT:8:R#CARRID:0:R#OL_DEL_D:1444665544:R
		// U,NO,0,3,7535426460057615,7535426506998154,ORDER;1-2-689-3001;OL_CNT:3001:N#CARRID:6:N#OL_DEL_D:0:N&DISTRICT;1-2;N_O_ID:1:I&STOCK;1-96471;QUANTITY:87:N&STOCK;1-7459;QUANTITY:27:N&STOCK;1-26719;QUANTITY:67:N&STOCK;1-13925;QUANTITY:56:N&STOCK;1-15847;QUANTITY:18:N&STOCK;1-26531;QUANTITY:90:N
		
		LogRecord result = new LogRecord();
		String[] tokens = line.split("[" + ValidationParams.RECORD_ATTRIBUTE_SEPERATOR + "]");
		int added = 0;
//		if (tokens.length > 7) {
//			added = 1;
//		}
		try {
			if (ValidationParams.USE_SEQ) {
				result.type = tokens[LogRecordToken.Type.Index].charAt(0);
				result.actionName = tokens[LogRecordToken.Name.Index];
				result.id = Utilities.concat(tokens[LogRecordToken.ThreadID.Index], ValidationParams.KEY_SEPERATOR, tokens[LogRecordToken.SequenceID.Index]);
				// result.id = tokens[LogRecordToken.ThreadID.Index] +
				// TPCCConstants.KEY_SEPERATOR +
				// tokens[LogRecordToken.SequenceID.Index];

				result.startTime = Long.parseLong(tokens[LogRecordToken.StartTime.Index + added]);
				result.endTime = Long.parseLong(tokens[LogRecordToken.EndTime.Index + added]);
				result.entities = getEntities(tokens[LogRecordToken.Entities.Index + added]);
			} else {
				result.type = tokens[LogRecordToken2.Type.Index].charAt(0);
				result.actionName = tokens[LogRecordToken2.Name.Index];
				result.id = tokens[LogRecordToken2.TransactionID.Index];
				result.startTime = Long.parseLong(tokens[LogRecordToken2.StartTime.Index + added]);
				result.endTime = Long.parseLong(tokens[LogRecordToken2.EndTime.Index + added]);
				result.entities = getEntities(tokens[LogRecordToken2.Entities.Index + added]);

			}
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println(line);
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			System.exit(0);
		}
		if (ValidationParams.USE_DATABASE&& result.getActionName().equalsIgnoreCase("scan")){
			result.queryParams=tokens[tokens.length-1];
		}

//		if (result.type==ValidationConstants.UPDATE_RECORD)
//		result.setEndTime(result.getEndTime()+ (300 * 1000000));
		result.originalEndTime=result.endTime;
		return result;
	}

	public String getQueryParams() {
		return queryParams;
	}

	public void setQueryParams(String queryParams) {
		this.queryParams = queryParams;
	}

	private static Entity[] getEntities(String entitiesString) {
		// CUSTOMER 1-3-154
		// [BALANCE:-10.0:R#YTD_P:10.0:R#P_CNT:1:R#LAST_O_ID:2564:R]
		String[] allEntityStrings = entitiesString.split(ValidationParams.ENTITY_SEPERATOR_REGEX);
		Entity[] result = new Entity[allEntityStrings.length];
		for (int i = 0; i < allEntityStrings.length; i++) {
			String[] entityInfo = allEntityStrings[i].split(ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR_REGEX);
			String[] allProperties = entityInfo[EntityToken.Property.Index].split(ValidationParams.PROPERY_SEPERATOR_REGEX);

			Property[] properties = new Property[allProperties.length];
			for (int j = 0; j < allProperties.length; j++) {
				String[] propertyInfo = allProperties[j].split(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR_REGEX);
				properties[j] = new Property(propertyInfo[PropertyToken.Name.Index], decode(propertyInfo[PropertyToken.Value.Index]), propertyInfo[PropertyToken.Type.Index].charAt(0));

			}
			result[i] = new Entity(entityInfo[EntityToken.Key.Index], entityInfo[EntityToken.Name.Index], properties);
		}
		return result;
	}

	private static String decode(String input) {
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.KEY_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.KEY_SEPERATOR));
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.RECORD_ATTRIBUTE_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR));
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.ENTITY_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.ENTITY_SEPERATOR));
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.PROPERY_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.PROPERY_SEPERATOR));
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR));
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR));
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.ESCAPE_START_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.ESCAPE_START_CHAR));
		input = input.replaceAll(ValidationParams.ESCAPE_START_CHAR + ValidationParams.ESCAPE_END_NUM + ValidationParams.ESCAPE_END_CHAR, String.valueOf(ValidationParams.ESCAPE_END_CHAR));

		return input;
	}

	public static String escapeCharacters(String input) {
		input = input.replaceAll(String.valueOf(ValidationParams.ESCAPE_START_CHAR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.ESCAPE_START_NUM + ValidationParams.ESCAPE_END_CHAR);
		input = input.replaceAll("(?<!\\(\\d\\d?)" + String.valueOf(ValidationParams.ESCAPE_END_CHAR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.ESCAPE_END_NUM + ValidationParams.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(ValidationParams.KEY_SEPERATOR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.KEY_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(ValidationParams.RECORD_ATTRIBUTE_SEPERATOR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.RECORD_ATTRIBUTE_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(ValidationParams.ENTITY_SEPERATOR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.ENTITY_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(ValidationParams.PROPERY_SEPERATOR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.PROPERY_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR), ValidationParams.ESCAPE_START_CHAR + ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR_NUM + ValidationParams.ESCAPE_END_CHAR);

		return input;
	}

	public int getNumOfOccurrences(String[] tokens, char delimiter, int startFrom) {
		int count = 0;
		for (int i = startFrom; i < tokens.length; i++)
			if (tokens[i].charAt(0) == delimiter)
				count++;
		return count;
	}

	public boolean overlap(LogRecord i) {
		//(5,10) (10,20)
		if (startTime <= i.startTime && endTime >= i.endTime)
			return true;
		if (startTime >= i.startTime && endTime <= i.endTime)
			return true;
		if (startTime >= i.startTime && startTime <= i.endTime)
			return true;
		if (endTime >= i.startTime && endTime <= i.endTime)
			return true;
		return false;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public Entity[] getEntities() {
		return entities;
	}

	public void setEntities(Entity[] entities) {
		this.entities = entities;
	}

	public String getActionName() {
		return actionName;
	}

	public void setActionName(String actionName) {
		this.actionName = actionName;
	}

	public boolean intersect(LogRecord log) {
		if (ValidationParams.USE_DATABASE&& (log.getActionName().equalsIgnoreCase("scan") || this.getActionName().equalsIgnoreCase("scan")) ){
			LogRecord scan= this;
			LogRecord other= log;
			if (log.getActionName().equalsIgnoreCase("scan")){
				scan =log;
				other=this;
			}
			
			String tokens[]= scan.getQueryParams().split(":");
			int upper= Integer.parseInt(tokens[0]);
			int lower= Integer.parseInt(tokens[1]);
			int primaryKey= Integer.parseInt(other.getEntities()[0].getKey());
			if (primaryKey>lower && primaryKey < upper)
			return true;
			else
				return false;
		}
		for (int i = 0; i < entities.length; i++)
			for (int j = 0; j < log.entities.length; j++) {
				if (entities[i].getName().equals(log.entities[j].getName()) && entities[i].getKey().equals(log.entities[j].getKey())) {
					return areRefSameProperties(entities[i], log.entities[j]);
				}
			}
		return false;
	}

	// TODO make sure if this checks the same pointer or not
	private boolean areRefSameProperties(Entity entity1, Entity entity2) {
		for (int i = 0; i < entity1.getProperties().length; i++) {
			for (int j = 0; j < entity2.getProperties().length; j++) {
				if (entity1.getProperties()[i] == null || entity2.getProperties()[j] == null)
					continue;
				if (entity1.getProperties()[i].getName().equals(entity2.getProperties()[j].getName())) {
					if ((entity1.getProperties()[i].getType() != ValidationParams.NO_READ_UPDATE) && (entity2.getProperties()[j].getType() != ValidationParams.NO_READ_UPDATE))
						return true;
				}
			}
		}
		return false;
	}

	public boolean intersect(long et) {
		return (startTime <= et && et <= endTime ? true : false);
	}

	public int getPartitionID() {
		return partitionID;
	}

	public void setPartitionID(int partitionID) {
		this.partitionID = partitionID;
	}

	public static class Comparators {

		public static Comparator<LogRecord> START = new Comparator<LogRecord>() {

			@Override
			public int compare(LogRecord o1, LogRecord o2) {
				return (o1.startTime > o2.startTime ? 1 : (o1.startTime < o2.startTime ? -1 : 0));
			}
		};
		public static Comparator<LogRecord> END = new Comparator<LogRecord>() {
			@Override
			public int compare(LogRecord o1, LogRecord o2) {
				return (o1.endTime > o2.endTime ? 1 : (o1.endTime < o2.endTime ? -1 : 0));
			}
		};
	}

	@Override
	public String toString() {
		String result = "";
		if (ValidationParams.debugPrinter) {
			String newActionName = actionName;
			// switch (actionName) {
			// case "GetProfile":
			// newActionName = "GP";
			// break;
			// case "GetFriends":
			// newActionName = "GF";
			// break;
			// case "GetPendingFriends":
			// newActionName = "GPF";
			// break;
			// case "InviteFriends":
			// newActionName = "IF";
			// break;
			// case "RejectFriend":
			// newActionName = "RF";
			// break;
			// case "AcceptFriend":
			// newActionName = "AF";
			// break;
			// case "Unfriendfriend":
			// newActionName = "UF";
			// break;
			// }
			result = String.valueOf(type) + ',' + newActionName + ",\t" + id + ",\t" + startTime + ",\t" + endTime + ",\t";
			if (entities != null) {
				for (int i = 0; i < entities.length; i++) {
					result += getEntityString(entities[i]);
				}
			}
		} else {
			result = id + "(" + startTime + ", " + endTime + ")";
		}
		return result;
	}

	public String toPrint() {
		String result = String.format("%c,%s,%s,%d,%d,", type, actionName, id, startTime, endTime);
		String seperator = "";
		for (Entity e : entities) {
			result += seperator + e.toPrint();
			seperator = "&";
		}
		return result;
	}

	private String getEntityString(Entity entity) {
		StringBuilder sb = new StringBuilder();
		sb.append(entity.getName());
		sb.append(ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(entity.getKey());
		sb.append(ValidationParams.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(getProperitiesString(entity.getProperties()));
		return sb.toString();
	}

	private String getProperitiesString(Property[] properties) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < properties.length; i++) {
			sb.append(properties[i].getName());
			sb.append(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(properties[i].getValue());
			sb.append(ValidationParams.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(properties[i].getType());
			if ((i + 1) != properties.length)
				sb.append(ValidationParams.PROPERY_SEPERATOR);
		}
		return sb.toString();
	}

	@Override
	public int compareTo(LogRecord o) {
		// String st1 = startTime + id;
		// String st2 = o.startTime + o.id;
		return Long.compare(startTime, o.startTime);
		// if (startTime > o.startTime)
		// return 1;
		// else if (startTime < o.startTime)
		// return -1;
		// else if (id.compareTo(o.id) == 0)
		// return 0;
		// else
		// return -1;
		// return id.compareTo(o.id);
		// return (startTime > o.startTime ? 1 : (startTime < o.startTime ? -1 :
		// (id.compareTo(o.id) ? 0 : -1)));
	}

	public void serializeID(DataOutputStream out) throws IOException {
		byte[] bID = id.getBytes();
		out.writeInt(bID.length);
		out.write(bID);
	}

	public void serialize(DataOutputStream out) throws IOException {
		byte[] bID = id.getBytes();
		out.writeInt(bID.length);
		out.write(bID);
		byte[] bActionName = actionName.getBytes();
		out.writeInt(bActionName.length);
		out.write(bActionName);
		out.writeLong(startTime);
		out.writeLong(endTime);
		out.writeLong(offset);
		out.writeChar(type);
		out.writeInt(partitionID);
		out.writeInt(logSize);
		out.writeShort(groupNum);
		out.writeInt(entities.length);
		for (Entity e : entities) {
			e.serialize(out);
		}
	}

	public static LogRecord deserialize(DataInputStream in) throws IOException {
		LogRecord r = new LogRecord();
		int size = in.readInt();
		if (size == Snapshot.nullObject) {
			return null;
		}
		byte[] buffer = new byte[size];
		in.read(buffer, 0, size);
		r.id = new String(buffer);
		size = in.readInt();
		buffer = new byte[size];
		in.read(buffer, 0, size);
		r.actionName = new String(buffer);
		r.startTime = in.readLong();
		r.endTime = in.readLong();
		r.offset = in.readLong();
		r.type = in.readChar();
		r.partitionID = in.readInt();
		r.logSize = in.readInt();
		r.groupNum = in.readShort();
		int eSize = in.readInt();
		r.entities = new Entity[eSize];
		for (int i = 0; i < eSize; i++) {
			r.entities[i] = Entity.deserialize(in);
		}
		return r;
	}

}
