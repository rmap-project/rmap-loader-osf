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
package info.rmapproject.loader.osf;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

import javax.jms.JMSException;

import org.openrdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.rmapproject.cos.osf.client.model.Node;
import info.rmapproject.cos.osf.client.model.RecordType;
import info.rmapproject.cos.osf.client.model.Registration;
import info.rmapproject.cos.osf.client.model.User;
import info.rmapproject.cos.osf.client.service.OsfClientService;
import info.rmapproject.loader.HarvestRecord;
import info.rmapproject.loader.osf.jms.OsfJmsQueue;
import info.rmapproject.loader.osf.model.QueueName;
import info.rmapproject.loader.osf.transformer.DiscoTransformer;
import info.rmapproject.loader.osf.transformer.OsfNodeDiscoTransformer;
import info.rmapproject.loader.osf.transformer.OsfRegistrationDiscoTransformer;
import info.rmapproject.loader.osf.transformer.OsfUserDiscoTransformer;
import info.rmapproject.loader.osf.transformer.TransformUtils;
import info.rmapproject.loader.util.LogUtil;

public class OsfTransformService {	

	/** The log. */
	private static final Logger LOG = LoggerFactory.getLogger(OsfTransformService.class);
	
	/**
	 * Number transformed
	 */
	Integer numTransformed = 0;
	/**
	 * JMS Queue instance to be used for queue management
	 */
	protected OsfJmsQueue jmsQueue;
	
	public OsfTransformService(){
		LogUtil.adjustLogLevels();
		this.jmsQueue = new OsfJmsQueue();
	}


	protected OutputStream transformRecord(String identifier, String type) {
		OutputStream rdf = null;
		if (identifier.length()>0){
			OsfClientService osf = new OsfClientService();
			RecordType harvesterType = RecordType.getType(type);

			DiscoTransformer transformer = null;
			
			switch (harvesterType){
			case OSF_NODE : 
				Node node = osf.getNode(identifier);
				transformer = new OsfNodeDiscoTransformer(node);
				break;
			case OSF_REGISTRATION : 
				Registration reg = osf.getRegistration(identifier);
				transformer = new OsfRegistrationDiscoTransformer(reg);
				break;
			case OSF_USER : 
				User user = osf.getUser(identifier);
				transformer = new OsfUserDiscoTransformer(user);
				break;
			default : 
				Node defaultNode = osf.getNode(identifier);
				transformer = new OsfNodeDiscoTransformer(defaultNode);
				break;
			}
			
			Model model = transformer.getModel();
			rdf = TransformUtils.generateTurtleRdf(model);
			LOG.debug("Transformed record id:" + identifier);
		}
		
		return rdf;
	}

	/**
	 * Reads records in queue specified, moves failures to fail queue, successes to ingest queue.
	 * @param fromQueue
	 * @param failQueue
	 * @return
	 * @throws JMSException 
	 */
	protected Integer transformRecords(String fromQueue, String failQueue, RecordType harvesterType) throws JMSException{
		
		Consumer<HarvestRecord> consumer = received -> {
			String id = new String(received.getBody());
			String type = received.getRecordInfo().getContentType();
			try {
					ByteArrayOutputStream rdf = (ByteArrayOutputStream) transformRecord(id, type);
					received.setBody(rdf.toByteArray());
					received.getRecordInfo().setContentType("text/turtle");
					String ingestQ = QueueName.getQueueName(QueueName.INGEST, harvesterType, null);
					jmsQueue.add(received, ingestQ);
					numTransformed = numTransformed + 1;
					LOG.info("Record transformed:" + id + " from queue: " + fromQueue + " and added to Ingest queue");
			} catch (Exception ex) {
				LOG.error("Transform failed for record from source: " + id, ex);
				//add to fail queue
				jmsQueue.add(received, failQueue);
				LOG.error("Record not transformed:" + received.getRecordInfo().getId() + " added to fail queue:" + failQueue);
			}
			
	      };	
	      
		jmsQueue.processMessages(fromQueue, consumer);
		LOG.debug(numTransformed + " records moved from " + fromQueue + " to ingest queue");		
		return numTransformed;
	}
	
    
    public void close() {
		jmsQueue.close();
    }
	
	
	
	
}
