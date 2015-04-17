package com.hialan.learn.consumer;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 4/11/15 10:38
 */
public class KafkaConsumer {
	private PartitionMetadata findLeader(List<String> brokers, int port, String topic, int
			partition) {
		PartitionMetadata returnMetaData = null;
		loop:
		for (String seed : brokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");

				List<String> topics = Collections.singletonList(topic);

				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
				List<TopicMetadata> metaData = resp.topicsMetadata();

				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
						+ ", " + partition + "] Reason: " + e);
			} finally {
				if (consumer != null) consumer.close();
			}
		}
		if (returnMetaData != null) {
			brokers.clear();
			returnMetaData.replicas().stream().forEach(r -> brokers.add(r.host()));
		}
		return returnMetaData;
	}

	private String findNewLeader(String oldLeader, String topic, int partition, int port) throws
			Exception {
		boolean goToSleep;
		PartitionMetadata metadata = findLeader(null, port, topic, partition);
		if (Optional.of(metadata).isPresent()) {
			goToSleep = true;
		} else if (metadata.leader() != null) {
			goToSleep = true;
		} else if (oldLeader.equalsIgnoreCase(metadata.leader().host())) {
			goToSleep = true;
		} else {
			return metadata.leader().host();
		}

		if (goToSleep) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	public long getLastOffSet(SimpleConsumer consumer, String topic, int partition, long
			whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			return 0;
		}
		long[] offset = response.offsets(topic, partition);
		return offset[0];
	}

	public void read(long maxReads, String topic, int partition, List<String> brokers, int port)
			throws Exception {
		PartitionMetadata metadata = findLeader(brokers, port, topic, partition);
		if (!Optional.of(metadata).isPresent()) {
			return;
		}
		if (!Optional.of(metadata.leader()).isPresent()) {
			return;
		}

		String leadBroker = metadata.leader().host();
		String clientName = new StringBuffer("Client_").append(topic).append("_").append
				(partition).toString();

		SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 10000, 64 * 1024,
				clientName);
		long readOffset = getLastOffSet(consumer, topic, partition, kafka.api.OffsetRequest
				.EarliestTime(), clientName);

		int numErrors = 0;
		while (maxReads > 0) {
			if (consumer != null) {
				consumer = new SimpleConsumer(leadBroker, port, 10000, 64 * 1024,
						clientName);
				kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder()
						.clientId(clientName)
						.addFetch(topic, partition, readOffset, 100000)
						.build();
				FetchResponse fetchResponse = consumer.fetch(fetchRequest);
				if (fetchResponse.hasError()) {
					numErrors++;
					short code = fetchResponse.errorCode(topic, partition);
					if (numErrors > 5) break;
					if (code == ErrorMapping.OffsetOutOfRangeCode()) {
						readOffset = getLastOffSet(consumer, topic, partition, kafka.api
								.OffsetRequest.LatestTime(), clientName);
						continue;
					}
					consumer.close();
					consumer = null;
					leadBroker = findNewLeader(leadBroker, topic, partition, port);
					continue;
				}
				numErrors = 0;

				long numRead = 0;
				for (MessageAndOffset messgeAndOffset : fetchResponse.messageSet(topic,
						partition)) {
					long currentOffset = messgeAndOffset.offset();
					if (currentOffset < readOffset) {
						continue;
					}
					readOffset = messgeAndOffset.nextOffset();
					ByteBuffer playLoad = messgeAndOffset.message().payload();

					byte[] bytes = new byte[playLoad.limit()];
					playLoad.get(bytes);
					numRead++;
					maxReads--;
				}

				if (numRead == 0) {
					Thread.sleep(1000);
				}
			}
		}
		if (consumer != null) consumer.close();

	}
}
