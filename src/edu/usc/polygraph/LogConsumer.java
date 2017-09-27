package edu.usc.polygraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LogConsumer implements Callable<Object> {
	private KafkaConsumer<String, String> consumer;
	private String id;
	TopicPartition partition;
	String fileName;
	long startTime, endTime, readOffset;

	public LogConsumer(String id, String fileName, TopicPartition partition, long startTime, long endTime,
			long readOffset) {
		this.readOffset = readOffset;
		this.startTime = startTime;
		this.endTime = endTime;
		this.id = id;
		this.partition = partition;
		this.fileName = fileName;
		Properties props = new Properties();
		props.put("bootstrap.servers", ValidationParams.KAFKA_HOST);
		props.put("enable.auto.commit", "false");
		props.put("group.id", this.id);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<String, String>(props);
		List<TopicPartition> partitions = new ArrayList<>();

		partitions.add(partition);

		consumer.assign(partitions);

	}

	@Override
	public Object call() {
		File ufile = null;
		FileWriter ufstream = null;
		BufferedWriter log = null;
		try {

			// consumer.subscribe(topics);
			ufile = new File(fileName + ".txt");
			ufstream = new FileWriter(ufile);
			log = new BufferedWriter(ufstream);
			long startOffset = findStartOffset(readOffset);
			consumer.seek(partition, startOffset);
			long st = 0;
			while (st <= endTime) {

				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					if (ValidationParams.USE_SEQ) {
						st = Long.parseLong(record.value().split(String.valueOf(
								ValidationParams.RECORD_ATTRIBUTE_SEPERATOR))[LogRecord.LogRecordToken.StartTime.Index]);
					} else {
						st = Long.parseLong(record.value().split(String.valueOf(
								ValidationParams.RECORD_ATTRIBUTE_SEPERATOR))[LogRecord.LogRecordToken2.StartTime.Index]);
					}
					log.write(record.value());
					if (st > endTime) {
						break;
					}

				} // end for

			} // end while
		} catch (WakeupException e) {
			// ignore for shutdown
		} catch (IOException e1) {
			e1.printStackTrace(System.out);
		} finally {
			try {
				log.flush();
				log.close();
				ufstream.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			consumer.close();
		}
		return null;
	}

	private long findStartOffset(long offset) {
		int jump = 100;
		offset = offset - jump;
		if (offset <= 0)
			return 0;
		if (partition.partition() == 47)
			System.out.println();
		consumer.seek(partition, offset);
		ConsumerRecords<String, String> recs = consumer.poll(10000);
		Iterator<ConsumerRecord<String, String>> it = recs.iterator();
		ConsumerRecord<String, String> rec = it.next();
		long st = -1;
		if (ValidationParams.USE_SEQ) {
			st = Long.parseLong(rec.value().split(String.valueOf(
					ValidationParams.RECORD_ATTRIBUTE_SEPERATOR))[LogRecord.LogRecordToken.StartTime.Index]);
		} else {
			st = Long.parseLong(rec.value().split(String.valueOf(
					ValidationParams.RECORD_ATTRIBUTE_SEPERATOR))[LogRecord.LogRecordToken2.StartTime.Index]);
		}
		if (st <= startTime) {
			if (true)
				return rec.offset();
			while (it.hasNext()) {
				rec = it.next();
				if (ValidationParams.USE_SEQ) {
				st = Long.parseLong(rec.value().split(String.valueOf(
						ValidationParams.RECORD_ATTRIBUTE_SEPERATOR))[LogRecord.LogRecordToken.StartTime.Index]);
				} else {
					st = Long.parseLong(rec.value().split(String.valueOf(
							ValidationParams.RECORD_ATTRIBUTE_SEPERATOR))[LogRecord.LogRecordToken2.StartTime.Index]);
				}
				if (st >= startTime)
					return rec.offset();
			}
			return rec.offset();
		} else { // st > startTime, keep going backward
			if (offset <= 0)
				return 0;
			return findStartOffset(offset);
		}

	}

	public void shutdown() {
		consumer.wakeup();
	}
}