/*******************************************************************************
 * Copyright 2017 Johns Hopkins University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This software was produced as part of the RMap Project (http://rmap-project.info),
 * The RMap Project was funded by the Alfred P. Sloan Foundation and is a 
 * collaboration between Data Conservancy, Portico, and IEEE.
 *******************************************************************************/
package info.rmapproject.loader.osf.jms;

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.rmapproject.loader.HarvestRecord;
import info.rmapproject.loader.jms.HarvestRecordListener;
import info.rmapproject.loader.jms.HarvestRecordWriter;
import info.rmapproject.loader.jms.JmsClient;

public class OsfJmsQueue implements AutoCloseable {

	/** The log. */
	private static final Logger LOG = LoggerFactory.getLogger(OsfJmsQueue.class);

	protected JmsClient client;
	
	protected static final int JMS_FAIL_TOLERANCE = 10;
	
	/**
	 * This provides a way to exit the countdownlatch wait loop if there are problems with JMS.
	 * When the jmsFailCount reaches the JMS_FAIL_TOLERANCE number, it will exit the loop.  
	 */
	protected int jmsFailCount=0;
	
		
	public OsfJmsQueue(){		
		ActiveMQConnectionFactory connectionFactory = 
				new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER, 
						ActiveMQConnection.DEFAULT_PASSWORD,ActiveMQConnection.DEFAULT_BROKER_URL);
		JmsClient rClient = new JmsClient(connectionFactory);
		rClient.init();
		this.client = rClient;	
	}
	
	public void add(HarvestRecord record, String queue) {
		final HarvestRecordWriter writer = new HarvestRecordWriter(client);
        writer.write(queue, record);
		LOG.info("Record: " + record.getRecordInfo().getId() + " added to queue: " + queue);
	}
		
	public void processMessages(String queue, Consumer<HarvestRecord> consumer) {
		LOG.info("Processing messages from : " +  queue);

		HarvestRecordListener listener = new HarvestRecordListener(consumer);
		CountDownLatch latch = new CountDownLatch(1);
		client.listen(queue, listener);
		try {		
			try {
				do {
					TimeUnit.SECONDS.sleep(2);
				} while (hasMoreMessages(queue));
				latch.countDown();
			} catch (JMSException ex){
				LOG.error("Repeated problems occurred while checking for messages on JMS. System will pause in an attempt to allow the process to complete naturally. "
						+ "It is strongly recommended that the results of this harvest are verified for completeness, or that this harvest cycle is re-run to ensure all data is transferred.", ex);	
				TimeUnit.SECONDS.sleep(10);
				latch.countDown();
			}
			latch.await();
		} catch (Exception e) {
			LOG.error("Failed to exit countdown latch gracefully.", e);	
		}	
	}
	
	public boolean hasMoreMessages(String queueName) throws JMSException
	{
		try {
			Session session = this.client.getSessionSupplier().get();
			Queue queue = session.createQueue(queueName);
			QueueBrowser browser = session.createBrowser(queue);
			Enumeration<?> e = browser.getEnumeration();
			if (e.hasMoreElements()){return true;}
			return false;		
		} catch (JMSException e){
			//we can afford some tolerance of JMS errors here, but if they keep happening this could get stuck in a countdown latch loop 
			//When the jmsFailCount hits the tolerance level specified, an error will be thrown
			jmsFailCount = jmsFailCount+1;
			if (jmsFailCount<=JMS_FAIL_TOLERANCE){
				LOG.error("Repeated JMS errors have occurred while checking for more messages.", e);
				throw e;
			} else {
				LOG.warn("Problem while checking JMS for more messages. Error logged, if errors continue an exception will be thrown.", e);
				return true;
			}
		}
	}
	
	public void close() {
		client.close();
	}

}
