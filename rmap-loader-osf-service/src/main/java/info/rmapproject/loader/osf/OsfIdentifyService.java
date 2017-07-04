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

import static info.rmapproject.loader.util.ConfigUtil.string;

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.DataSource;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import info.rmapproject.cos.osf.client.model.LightNode;
import info.rmapproject.cos.osf.client.model.RecordType;
import info.rmapproject.cos.osf.client.service.OsfClientService;
import info.rmapproject.loader.HarvestRecord;
import info.rmapproject.loader.RdbmsHarvestRunRegistry;
import info.rmapproject.loader.osf.jms.OsfJmsQueue;
import info.rmapproject.loader.osf.model.OsfHarvestableRecord;
import info.rmapproject.loader.osf.model.OsfLightRecordDTO;
import info.rmapproject.loader.osf.model.QueueName;
import info.rmapproject.loader.osf.utils.OSFLoaderUtils;

public class OsfIdentifyService {
	
	
	/** The log. */
	private static final Logger LOG = LoggerFactory.getLogger(OsfIdentifyService.class);
	
	private final static String DATE_MODIFIED_FILTER = "filter[date_modified]";
	private final static String FROM_FILTER = "[gte]";
	private final static String TO_FILTER = "[lte]";
	private final static String DATE_MODIFIED_FROM_FILTER = "filter[date_modified]" + FROM_FILTER;
	private final static String DATE_MODIFIED_TO_FILTER = "filter[date_modified]" + TO_FILTER;
	private final static String DATE_CREATED_FILTER = "filter[date_created]";
	private final static String DATE_CREATED_FROM_FILTER = "filter[date_created]" + FROM_FILTER;
	private final static String DATE_CREATED_TO_FILTER = "filter[date_created]" + TO_FILTER;
	private final static String DATE_REGISTERED_FILTER = "filter[date_registered]";
	private final static String DATE_REGISTERED_FROM_FILTER = "filter[date_registered]" + FROM_FILTER;
	private final static String DATE_REGISTERED_TO_FILTER = "filter[date_registered]" + TO_FILTER;
	
	private final static String PUBLIC_FILTER = "filter[public]";
	private final static String PUBLIC_FILTER_DEFAULT_VALUE = "true";	
	

	/**
	 * JMS Queue instance to be used for queue management
	 */
	private OsfJmsQueue jmsQueue;

	/**
	 * Datasource for saved data
	 */
	private DataSource datasource;
	
	private RecordType harvestType;
		
	private HashMap<String,String> params;
	
	/**
	 * Holds run date for harvester run. Initiated when class is initiated so it remains consistent throughout data load.
	 */
	private final DateTime currRunDate = new DateTime(DateTimeZone.UTC);
	
	private DateTime lastRunDate = null;
	
	private boolean filterByRunDate = false;
	
	
	/**
	 * Initiate with harvester type
	 * @param type
	 */
	public OsfIdentifyService(RecordType type, String filters){
		
		try {
			this.harvestType=type;
			this.params = OSFLoaderUtils.readParamsIntoMap(filters);
			this.jmsQueue = new OsfJmsQueue();
			
			//initiate data source
	        final HikariDataSource ds = new HikariDataSource();
	        ds.setJdbcUrl(string("jdbc.url", "jdbc:sqlite:"));
	        ds.setUsername(string("jdbc.username", null));
	        ds.setPassword(string("jdbc.password", null));
	        ds.setDriverClassName(string("jdbc.driver",null));
	        this.datasource = ds;
	        

		} catch (Exception e) {
			throw new RuntimeException("Could not complete identify records process.", e);    
		}
	}
	
	public void setHarvesterType(RecordType type) {
		this.harvestType=type;
	}
	

	public void setParams(HashMap<String,String> params) {
		this.params=params;
	}

	public void setParams(String params) {
		try {
			this.params = OSFLoaderUtils.readParamsIntoMap(params);
		} catch (Exception e) {
			throw new RuntimeException("Could not process filters into key value pairs:" + params, e);   
		}
	}
	
	
	/**
	 * Identifies OSF records with filters applied that should be loaded to the transform queue 
	 * @param filters
	 * @return
	 */
	public Integer identifyNewRecords(){
		Integer numIdentified = 0;
		try {			
			String harvestName = "osf." + harvestType.getTypeString();
						
			if (!params.containsKey(PUBLIC_FILTER) && !harvestType.equals(RecordType.OSF_USER)){
				params.put(PUBLIC_FILTER, PUBLIC_FILTER_DEFAULT_VALUE);
			}
			
			if (!containsDateFilter(params)) {
				filterByRunDate = true;
				String startDate = null;
				lastRunDate = getLastHarvestDate(harvestName);
				if (lastRunDate != null){
					startDate = OSFLoaderUtils.convertToOsfDateParam(lastRunDate);
				} else {
					//if no start date, set default to get yesterday's results only - this prevents harvesting the whole dataset by default			
					startDate = OSFLoaderUtils.previousDayAsString(currRunDate);	
				}
				
				if (!harvestType.equals(RecordType.OSF_USER)) {
					params.put(DATE_MODIFIED_FROM_FILTER, startDate);
				} else if (lastRunDate==null){
					lastRunDate = new DateTime().minusDays(2);
				}

				//only record harvest date if we're doing default date handling
				setNewHarvestDate(harvestName, currRunDate);
			}
			
			Iterator<OsfLightRecordDTO> iterator = initiateIterator();
			String queueName = QueueName.getQueueName(QueueName.TRANSFORM, harvestType, null);
			LOG.info("Adding records to queue: " + queueName);
			numIdentified = addAllRecords(iterator, queueName);
			
		} catch (Exception e) {
			throw new RuntimeException("Could not complete identify records process.", e);    
		}
		return numIdentified;
	}
	
	
	private Iterator<OsfLightRecordDTO> initiateIterator() throws Exception{
		Iterator<OsfLightRecordDTO> iterator = new OsfIteratorAdapter(params, harvestType); 
		return iterator;
	}
	

	private Integer addAllRecords(Iterator<OsfLightRecordDTO> iterator, String queue) {
		//Reset counter
		Integer counter = 0;   
		Set<String> identifiedIds = new HashSet<String>();                                                      
		do {
	        String id = null;
    		try {
    			OsfLightRecordDTO osfRecord = iterator.next();
    			DateTime filterDate = osfRecord.getFilterDate();
    			if (!filterByRunDate 
    					|| (filterByRunDate 
    						&& (lastRunDate==null || filterDate.isAfter(lastRunDate)||filterDate.equals(lastRunDate)))
    						&& filterDate.isBefore(currRunDate)) {
    				    
    				id = osfRecord.getId();
    				
    				//For nodes or registrations, check if parent is accessible, if so use parent record instead
    				if (osfRecord.getType().equals(RecordType.OSF_NODE)
    						|| osfRecord.getType().equals(RecordType.OSF_REGISTRATION)){
    					
    					String newId = getHighestAccessibleParentNode(id, osfRecord.getType());
    					if (!newId.equals(id)){
    						LOG.info("An accessible parent record for the one harvested was identified.  Record: " + newId + " was added to the transform queue in place of " + id);
    						//note: keep the modified date, since a subcomponent has been modified - this might not register in the modified date of the parent.
    						osfRecord.setId(newId);
    						id = newId;
    					}
    					
    				}

    				if (!identifiedIds.contains(id)) {
	    				HarvestRecord record = new OsfHarvestableRecord(osfRecord);
	   					jmsQueue.add(record, queue);
	   	    			counter = counter + 1;
	   	    			identifiedIds.add(id);
					} else {
						LOG.info("Record " + id + " from queue " + queue + " was skipped. Record was already added in this session.");					
					}
    			} else if (filterDate.isBefore(lastRunDate)) {
    				//exit loop - the rest of the records will be even earlier!
    				break;
    			}
    			
    		} catch (Exception e) {
    			if (id==null){
    				Integer i = counter+1;
    				id = i.toString();
    			}
    			String logMsg = "Could not complete export for record " + id + "\n Continuing to next record. Msg: " + e.getMessage();
    			LOG.error(logMsg,e);
    		}
		} while(iterator.hasNext());
				
		return counter;
	}
	
	/**
	 * Attempts to connect to URL provided to see if it returns a 401, which means it exists but is not publicly accessible.
	 * @param testUrl
	 * @return
	 * @throws Exception
	 */
	public boolean urlAccessible(String testUrl) throws Exception{
		URL url = new URL(testUrl); 
		HttpURLConnection connection = (HttpURLConnection)url.openConnection(); 
		connection.setRequestMethod("GET"); connection.connect(); 
		int code = connection.getResponseCode();
		if (code==401){// process this
			return false; //there is a parent node but it isn't accessible!
		} else {
			return true; //there is a parent node and it is accessible
		}
	}
	
	/**
	 * Walks up node/registration tree through parents to find the highest level accessible node
	 * @param id
	 * @param type
	 * @return
	 * @throws Exception
	 */
	public String getHighestAccessibleParentNode(String id, RecordType type) throws Exception{
		OsfClientService osfClient = new OsfClientService();      
		//use id to get individual record and go up tree to top.  Replace osfRecord with top record.
		LightNode newRecord = null;
		String newId = id;
		String parentId = id;
		String lastParentId = null;
		do {
			lastParentId = parentId;
			if (type.equals(RecordType.OSF_NODE)) {
				newRecord = osfClient.getLightNode(newId);
			} else {
				newRecord = osfClient.getLightRegistration(newId);
			}
			parentId = newRecord.getParent();
			
			if (parentId!=null && urlAccessible(parentId)) {
				parentId = OSFLoaderUtils.extractLastSubFolder(parentId);
				newId = parentId;    							
			}
		} while (!newId.equals(lastParentId));

		//cleanup
		osfClient = null;
		return newId;
	}
	
	
	
	

	/**
	 * Retrieve date of last harvest for the harvest name specified
	 * @param harvestName
	 * @return
	 */
	private DateTime getLastHarvestDate(String harvestName) {
        final RdbmsHarvestRunRegistry harvestRegistry = new RdbmsHarvestRunRegistry();
        harvestRegistry.setDataSource(datasource);
        harvestRegistry.init();
		Date lastRunDate = harvestRegistry.getLastRunDate(harvestName);
		if (lastRunDate!=null){
			DateTime lastRunDateJoda = new DateTime(lastRunDate).withZoneRetainFields(DateTimeZone.UTC);
			return lastRunDateJoda;
		} else {
			return null;
		}
	}
	
	/**
	 * Save current harvest date
	 * @param harvestName
	 * @return
	 */
	private void setNewHarvestDate(String harvestName, DateTime newHarvestDate) {
		try {
	        final RdbmsHarvestRunRegistry harvestRegistry = new RdbmsHarvestRunRegistry();
	        harvestRegistry.setDataSource(datasource);
	        harvestRegistry.init();
	        Date jdkDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(newHarvestDate.toString("yyyy-MM-dd HH:mm:ss"));
			harvestRegistry.addRunDate(harvestName, jdkDate);
		} catch (Exception e){
			LOG.error("Could not parse harvest date. The harvest date could not be recorded.", e);
			throw new RuntimeException("Could not parse harvest date. The harvest date could not be recorded.", e);
		}
	}
	
	
	private boolean containsDateFilter(HashMap<String,String> params) {
		if (params.containsKey(DATE_MODIFIED_FILTER)
				||params.containsKey(DATE_MODIFIED_FROM_FILTER)
				||params.containsKey(DATE_MODIFIED_TO_FILTER)
				||params.containsKey(DATE_CREATED_FILTER)
				||params.containsKey(DATE_CREATED_FROM_FILTER)
				||params.containsKey(DATE_CREATED_TO_FILTER)
				||params.containsKey(DATE_REGISTERED_FILTER)
				||params.containsKey(DATE_REGISTERED_FROM_FILTER)
				||params.containsKey(DATE_REGISTERED_TO_FILTER)){
			return true;
		}
		
		return false;
	}
	
    public void close() {
		jmsQueue.close();
    }

	
}
