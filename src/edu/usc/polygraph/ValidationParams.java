package edu.usc.polygraph;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class ValidationParams {
	////////////// props
	public static boolean CREATE_SNAPSHOT=false;
	public static DateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss");
	public static  boolean DO_VALIDATION=true; //145
	public final static boolean SHRINK_BUCKET=true;
	public final static String APPLICATION_PROP = "app";
	public final static String DO_VALIDATION_PROP = "validation";
	public final static String NUM_PARTITIONS_PROP = "numpartitions";

	public final static String INIT_PROP = "init";
	public final static String USE_KFKA_PROP = "kafka";
	public final static String GLOBAL_SCHEDULE_PROP = "globalschedule";
	public final static String ER_PROP = "er";
	public final static String FILE_LOG_PROP = "filelogdir";
	public final static String USE_BUFFER_PROP="usebuffer";
	public final static String ZOOK_HOST_PROP = "zookhot";
	public final static String KAFKA_HOST_PROP = "kafkahost";
	public static String KAFKA_HOST ="10.0.0.240:9092";//,10.0.0.106:9092,10.0.0.240:9092,10.0.0.235:9092,10.0.0.200:9092";//"10.0.0.145:9092" ;
	public static final boolean PRINT_ALL_STATS=false;
	//public static final String ZookerperIP = "10.0.0.240";
	public static String ZOOKEEPER_HOST = KAFKA_HOST.split(":")[0]+":2181";
	public static final boolean WINDOWS = false;
	public static final String PRINT_FREQ_PROP = "printfreq";
	public static final String VALIDATOR_ID_PROP = "id";
	public static final String NUM_CLIENTS_PROP = "numclients";
	public static final String CLIENT_ID_PROP = "clientid";

	public static final String NUM_VALIDATORS_PROP = "numvalidators";
	public static final String ONLINE_RUNNING_PROP = "online";
	public static boolean onlineRunning=false; 


	public static String erFile = null;
	
	public final static long KAFKA_POLL_WAIT_MILLIS=100;
	public final static String CONSUMER_FETCH_MAX_WAIT_MS="100";
	public final static String CONSUMER_FETCH_MIN_BYTES="1";
    public static final int POLL_MAX_TRIES=10;
    
	public static  String READ_MAX_PARTITION_FETCH_BYTES="400"; //200 BG 1000 TPCC(400 read)
	public static String UPDATE_MAX_PARTITION_FETCH_BYTES="1000";//"1500"; buffered 1500 //1000 for BG and TPCC
	
	public static String READ_MAX_POLL_RECORDS="2147483647";
	public static String UPDATE_MAX_POLL_RECORDS="2147483647";//"200";
	
	
	
   
    public static  boolean PRODUCE_STATS=true;
    public static  boolean KAFKA_OS_STATS=false;
    
    public static final boolean POLL_DEBUG=false;



	// =bg -p =false -p kafka=true -p globalschedule=false -p er= -p filelogdir= -p kafkalogdir=

	/////
    public static final boolean MEASURE_MEMORY=false;
	
	public static boolean GLOBAL_SCHEDULE = false;
	public static boolean debugPrinter = false;
	public static boolean hasInitState = false;
	public static boolean countDiscardedWrites = false && !hasInitState;
	public static int THREAD_COUNT = 1;

	public final static int STATS_INTERVAL_SECONDS = 1;
	public static String dirSeparator = System.getProperty("file.separator");
	public static String lineSeparator = System.getProperty("line.separator");
	public static final boolean verbose = false;
	public static final boolean debug = false;
	public static final boolean staleHTML = false;

	public static final int MAX_WRITE_LOGS = -2;

	// ============ General =========================

	public static final char NEW_VALUE_UPDATE = 'N';
	public static final char INCREMENT_UPDATE = 'I';
	public static final char DECREMENT_UPDATE_INTERFACE = 'Q';
	public static final char NO_READ_UPDATE = 'X';
	public static final char VALUE_READ = 'R';

	public static final char RECORD_ATTRIBUTE_SEPERATOR = ',';
	public static final char ENTITY_SEPERATOR = '&';
	public static final char PROPERY_SEPERATOR = '#';
	public static final char PROPERY_ATTRIBUTE_SEPERATOR = ':';
	public static final char ENTITY_ATTRIBUTE_SEPERATOR = ';';
	public static final char RELATIONSHIP_ENTITY_SEPERATOR = PROPERY_SEPERATOR;
	public static final char KEY_SEPERATOR = '-';
	public static final String ESCAPE_START_CHAR = "\\(";
    public static final String ESCAPE_END_CHAR = "\\)";

    public static final int KEY_SEPERATOR_NUM = 1;
    public static final int RECORD_ATTRIBUTE_SEPERATOR_NUM = 2;
    public static final int ENTITY_SEPERATOR_NUM = 3;
    public static final int PROPERY_SEPERATOR_NUM = 4;
    public static final int PROPERY_ATTRIBUTE_SEPERATOR_NUM = 5;
    public static final int ENTITY_ATTRIBUTE_SEPERATOR_NUM = 6;
    public static final int ESCAPE_START_NUM = 7;
    public static final int ESCAPE_END_NUM = 8;
    
	public static final char UPDATE_RECORD = 'U';
	public static final char READ_RECORD = 'R';
	public static final char READ_WRITE_RECORD = 'Z';

	public static final String ENTITY_SEPERATOR_REGEX = "[" + ENTITY_SEPERATOR + "]";
	public static final String ENTITY_ATTRIBUTE_SEPERATOR_REGEX = "[" + ENTITY_ATTRIBUTE_SEPERATOR + "]";
	public static final String PROPERY_SEPERATOR_REGEX = "[" + PROPERY_SEPERATOR + "]";
	public static final String PROPERY_ATTRIBUTE_SEPERATOR_REGEX = "[" + PROPERY_ATTRIBUTE_SEPERATOR + "]";
	public static boolean USE_KAFKA = true;
	public static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##.00");

	public static String[][] ENTITIES_INSERT_ACTIONS;

	// ============= TPCC ============================

	public static final String NEWORDER_ACTION = "NO";
	public static final String ORDERSTATUS_ACTION = "OS";
	public static final String PAYMENT_ACTION = "PA";
	public static final String DELIVERY_ACTION = "DE";
	public static final String BUCKET_ACTION = "BU";
	public static final String STOCKLEVEL_ACTION = "SL";

	public static final String CUSTOMER_ENTITY = "CUS";// "CUSTOMER";
	public static final String ORDER_ENTITY = "ORD"; // ER";
	public static final String DISTRICT_ENTITY = "DIS";// TRICT";
	public static final String STOCK_ENTITY = "STK";
	public static final String CUST_ORDER_REL = CUSTOMER_ENTITY + "*" + ORDER_ENTITY;

	public static final long DELIVERYDATE = 1444665544000L;
	public static final double ERROR_MARGIN = 0.03;

	public static final String CUSTOMER_BALANCE = "BALANCE";
	public static final String CUSTOMER_YTD_PAYMENT = "YTD_P";
	public static final String CUSTOMER_PAYMENT_COUNT = "P_CNT";

	public static final String ORDER_ORDER_COUNT = "OL_CNT";
	public static final String ORDER_CARRIER_ID = "CARRID";
	public static final String ORDER_DELIVERY_DATE = "OL_DEL_D";

	public static int DELIVERY_DATE_DIVISION = 1000; // This used to discard digits from the delivery date in millis to match retrieved time stamp;

	public static final String[] CUST_ORDER_REL_PROPERIES = { CUSTOMER_ENTITY, ORDER_ENTITY };
	public static final String[] CUSTOMER_PROPERIES = { CUSTOMER_BALANCE, CUSTOMER_YTD_PAYMENT, CUSTOMER_PAYMENT_COUNT };
	public static final String[] ORDER_PROPERIES = { ORDER_ORDER_COUNT, ORDER_CARRIER_ID, ORDER_DELIVERY_DATE };
	public static final String[] DISTRICT_PROPERIES = { "N_O_ID" };
	public static final String[] STOCK_PROPERIES = { "QUANTITY" };

	// ============= BG ==============================

	// ============= YCSB ============================

	public static final String USER_ENTITY = "USR";
	// =================KAFKA

	
	//////////////////////////

	public static final String[] USER_PROPERIES = { "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10","F0"};

	public static String[] ENTITY_NAMES = { USER_ENTITY };
	public static String[][] ENTITY_PROPERTIES = { USER_PROPERIES };

	public static final String DELETED_STRING = "#DELETED#";

	public static final String NULL_STRING = "NULL";
	public static final char VALUE_NA = 'U';
	public static final char VALUE_DELETED = 'D';
	public static final String MEMBER_ENTITY = "MEMBER";
	public static final String[] FRIEND_COUNT_PROPERIES = { "FRIEND_CNT" };
	public static final String[] PENDING_COUNT_PROPERIES = { "PENDING_CNT" };

	public static final String[] MEMBER_PROPERIES = { "FRIEND_CNT", "PENDING_CNT" };
	public final static String VALIDATION_PROP = "validation";
	public static final String KAFKA_OS_STATS_PROP = "kafkaosstats";

	public static boolean USE_SEQ = false;
	public static final boolean KAFKA_TIME = false;
	public static final String MAX_BUFFER_SIZE_PROP = "buffer";
	public static final String BUFFER_THRESHOLD_PROP = "bufferthreshold";
	public static final Object SKEW_PROP = "skew";
	public static final String SNAPSHOT_DELTA = "ssDelta";
	public static final boolean BATCH_UNORDERED = true;
	public static final String SID_INIT = "INIT";
	public static final boolean USE_DATABASE = false;


	public static final boolean CDSE = false;
	public static final boolean COMBINE_AND_VALIDATE = true;
	public static final boolean ALL_STRING = false;
	public static final boolean YAZ_FIX = true; 
	public static final boolean COMPUTE_FRESHNESS=false;
	public static final boolean PRINT_EXPECTED = false;

	public static boolean INTEGER_KEY = false; 
	public static long maxBufferSize =Long.MAX_VALUE;//1024*50;//// *300; // 1024=10 logs
	// //
	// 100
	// 512
// 1024
	public static float bufferThreshold = 0.5f;
	public static boolean useBuffer = true;

	



}
