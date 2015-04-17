package com.hialan.learn.producer;

import com.google.common.base.Joiner;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 4/16/15 14:39
 */
public class TestProducer {
	public static void main(String[] args) {
		getBrokerList();
		long events = Long.parseLong("4");
		Random rnd = new Random();

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092,localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.hialan.learn.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<>(config);
		for (long nEvents = 0; nEvents < events; nEvents++) {
			long runtime = new Date().getTime();
			String ip = "192.168.2." + rnd.nextInt(255);
			String msg = runtime + ", www.hialan.com," + ip;
			KeyedMessage<String, String> data = new KeyedMessage<>("page_visits",
					ip, msg);
			producer.send(data);
		}
		producer.close();
	}

	public static String getBrokerList() {
		ZkClient zkClient = new ZkClient("localhost:2181", 400, 5000, new
				BytesPushThroughSerializer());
		List<String> brokerList = zkClient.getChildren("/brokers/ids");
		String brokerListStr = Joiner.on(",").join(brokerList);
		System.out.println(brokerListStr);

		return null;
	}
}
