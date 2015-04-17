package com.hialan.learn.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * User: Alan
 * Email:alan@hialan.com
 * Date: 4/11/15 12:59
 */
public class ConsumerTest implements Runnable {
	private KafkaStream m_stream;
	private int m_threadNumber;

	public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext())
			System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
		System.out.println("Shutting down Thread: " + m_threadNumber);

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
