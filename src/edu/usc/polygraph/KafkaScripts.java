package edu.usc.polygraph;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import edu.usc.polygraph.utils.UtilConstants;

public class KafkaScripts {

	public static void deleteTopic(String topic) {
		topic = topic.toUpperCase();
		try {
			// bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic
			// MEMBER
			String cmd = UtilConstants.KAFKA_FLDR + "bin/kafka-topics.sh --zookeeper " + ValidationParams.ZOOKEEPER_HOST + " --delete --topic " + topic;
			Runtime.getRuntime().exec(cmd);
			// Process p = new ProcessBuilder(cmd).start();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

	}

	public static boolean isTopicExist(String topic) {
		topic = topic.toUpperCase();

		// bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic
		// MEMBER
		String cmd = UtilConstants.KAFKA_FLDR + "bin/kafka-topics.sh --zookeeper " + ValidationParams.ZOOKEEPER_HOST + " --list ";
		String r = Utilities.executeRuntime(cmd, true);
		if (r.toLowerCase().contains(topic.toLowerCase())) {
			return true;
		}
		return false;

	}

	public static boolean isValidatorRunning(String app) {
		String r = Utilities.executeRuntime("jps -m | grep \"ValidationMain\" | grep -ie \"-app " + app + "\"", true);
		if (r.toLowerCase().contains(app.toLowerCase())) {
			return true;
		}
		return false;
	}

	public static void killValidator(String app) {
		Utilities.executeRuntime("jps -m | grep \"ValidationMain\" | grep -ie \"-app " + app + "\" | cut -b1-6 | xargs -t kill -9", true);
	}

	public static String createTopic(String topic, int numPartitions) {
		String output = "";
		topic = topic.toUpperCase();

		try {
			// --create --zookeeper localhost:2181 --replication-factor 1
			// --partitions 4 --topic test
			String cmd = "sudo " + UtilConstants.KAFKA_FLDR + "bin/kafka-topics.sh --zookeeper " + ValidationParams.ZOOKEEPER_HOST + " --create --replication-factor 1 --partitions " + numPartitions + " --topic " + topic;
			Process p = Runtime.getRuntime().exec(cmd);
			Thread.sleep(1000);
			InputStream stdout = p.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
			String line = "";
			while ((line = reader.readLine()) != null) {
				if (line.contains("already exists")) {
					output = "already exists";
					break;
				}
				output = output + line;
			}
			// Process p = new ProcessBuilder(cmd).start();
			p.waitFor();
			stdout.close();
			reader.close();

		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		// output is: Created topic "test".
		// or: already exists.

		return output;
	}
	
	
	public static void reAssignPartitions(String topic, int numV, int numS) {
		String output = "";
		topic = topic.toUpperCase();

		try {
			// --create --zookeeper localhost:2181 --replication-factor 1
			// --partitions 4 --topic test
			
			String createfile = "sudo " + UtilConstants.KAFKA_FLDR + "bin/createpartition.sh " +topic +" " +numV + " " + numS ;
			Process pfile = Runtime.getRuntime().exec(createfile);
			Thread.sleep(1000);
//			bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file expand-cluster-reassignment.json --execute
//			

			String cmd = "sudo " + UtilConstants.KAFKA_FLDR + "bin/kafka-reassign-partitions.sh --zookeeper " + ValidationParams.ZOOKEEPER_HOST + " --reassignment-json-file /home/mr1/partition.txt --execute ";
			Process p = Runtime.getRuntime().exec(cmd);
			Thread.sleep(1000);
			InputStream stdout = p.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
			String line = "";
			while ((line = reader.readLine()) != null) {
				
					System.out.println(line);
				
				
			}
			// Process p = new ProcessBuilder(cmd).start();
			p.waitFor();
			stdout.close();
			reader.close();

		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		// output is: Created topic "test".
		// or: already exists.

	}
	

	public static void main(String[] args) {
		boolean r = true;
		killValidator("Bg");
		System.out.println(r);

	}

}
