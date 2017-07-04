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
package info.rmapproject.loader.osf.model;

import java.util.StringJoiner;

import info.rmapproject.cos.osf.client.model.RecordType;

/**
 * Manages OSF Queue Names
 * @author khanson
 */
public class QueueName {

	/** Node transform queue - first try. */
	public static final String TRANSFORM = "rmap.osf.transform";

	/** Node transform queue - first try. */
	public static final String INGEST = "rmap.osf.ingest";
	
	/** Added to end of queue name to denote we are waiting for a retry. */
	public static final String RETRY1 = "retry1";
	
	/** Added to end of queue name to denote we are waiting for a second retry. */
	public static final String RETRY2 = "retry2";
	
	/** Added to end of queue name to denote we are waiting for a third retry. */
	public static final String RETRY3 = "retry3";
	
	/** Added to end of queue name to denote 3 retries have failed. */
	public static final String FAIL = "fail";
		
	/**
	 * Builds the queue name. It will concatenate the prefix, type, and postfix as available present, and delimit with a "."
	 * @param prefix
	 * @param type
	 * @param postfix
	 * @return
	 */
	public static String getQueueName(String queueName, RecordType harvestType, String retryLevel) {
		if (queueName == null) {
			throw new RuntimeException("Queue name prefix required in order to generate a complete queue name");
		}
		
		StringJoiner joiner = new StringJoiner(".");
		joiner.add(queueName);
		
		String sType = harvestType.getTypeString();
		if (sType!=null && sType.length()>0){
			joiner.add(sType);
		}
		
		if (retryLevel!=null && sType.length()>0){
			joiner.add(retryLevel);
		}
		
		return joiner.toString();
	}
	
	
}
