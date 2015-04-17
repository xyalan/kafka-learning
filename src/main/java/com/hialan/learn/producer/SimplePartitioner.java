package com.hialan.learn.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 4/16/15 14:40
 */
public class SimplePartitioner implements Partitioner {
	public SimplePartitioner (VerifiableProperties props) {

	}

	@Override
	public int partition(Object key, int numPartitions) {
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf(".");
		if (offset > 0) {
			partition = Integer.parseInt(stringKey.substring(offset + 1)) % numPartitions;
		}
		return partition;
	}
}
