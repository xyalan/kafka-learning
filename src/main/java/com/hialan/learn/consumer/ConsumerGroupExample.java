package com.hialan.learn.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 4/11/15 12:40
 */
public class ConsumerGroupExample {
	private final ConsumerConnector consumer;

	private final String topic;

	private ExecutorService executorService;

	public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig
				(a_zookeeper, a_groupId));
		this.topic = a_topic;
	}

	public void shutdown() {
		if (consumer != null) consumer.shutdown();
		if (executorService != null) executorService.shutdown();

		try {
			if (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	private ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", a_zookeeper);
		properties.put("group.id", a_groupId);
		properties.put("zookeeper.session.timeout.ms", "400");
		properties.put("zookeeper.sync.time.ms", "200");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("auto.offset.reset", "smallest");
		return new ConsumerConfig(properties);
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountmap = new HashMap<>();
		topicCountmap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountmap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		executorService = Executors.newFixedThreadPool(a_numThreads);
		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			executorService.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
		}
	}

	public static void main(String[] args) {
		String zookeeper = "10.200.187.78:2181";
		String groupId = "personalized";
		String topic = "my-replicated-topic";
		int threads = 1;

		ConsumerGroupExample example = new ConsumerGroupExample(zookeeper, groupId, topic);
		example.run(threads);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
		}
		example.shutdown();
	}
}
