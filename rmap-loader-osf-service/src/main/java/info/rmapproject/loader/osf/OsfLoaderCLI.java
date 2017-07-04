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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.rmapproject.cos.osf.client.model.RecordType;
import info.rmapproject.loader.osf.model.QueueName;

/**
 * Imports SHARE JSON into a SHARE object and then transforms it to and RMap DiSCO.
 *
 * @author khanson
 */
public class OsfLoaderCLI {

    /** The Constant log. */
    private static final Logger LOG = LoggerFactory.getLogger(OsfLoaderCLI.class);
    
	/** The default data type. */
	private static final String DEFAULT_TYPE = "node";

	/** The default process type. */
	private static final String DEFAULT_PROCESS = "all";
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		
		String harvestType = DEFAULT_TYPE;
        String processType = DEFAULT_PROCESS;
		String filters = "";
        
        
        //create options
        Options options = new Options();
        options.addOption("t", "type", true, "Defines which type to harvest from OSF. Options are: user, node, or registration. Default is node");
        options.addOption("f", "filters", true, "API request filters formatted in the style of a querystring e.g. filter[modified_date]=2017-05-05, filter[id]=kjd2d (default: no filters). Note that this only applies to the identify process");
        options.addOption("p", "process", true, "Defines which process to run against the selected type. Options are: identify, transform, ingest, or all");
        options.addOption("h", "help", false, "Print help message");
        
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd =  null;
        
        try {
        	
        	StringBuilder errmsg = new StringBuilder();
        	cmd = parser.parse(options, args);

            /* Handle general options such as help */
            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("OSF Loader CLI", options);
            } else {
            	if (cmd.hasOption("type")) {
            		harvestType=cmd.getOptionValue("type");
            		if (!harvestType.equals("node") && !harvestType.equals("registration") && !harvestType.equals("user")) {
            			errmsg.append("Only the following parameters are allowed for types: node, registration, or user");
            			errmsg.append(System.getProperty("line.separator"));
            		}
            	}
            	if (cmd.hasOption("process")) {
            		processType=cmd.getOptionValue("process");
            		if (!processType.equals("all") && !processType.equals("identify") && !processType.equals("transform") && !processType.equals("ingest")) {
            			errmsg.append("Only the following parameters are allowed for process: identify, transform, ingest, or all");
            			errmsg.append(System.getProperty("line.separator"));
            		}
            	}
            	
            	if (cmd.hasOption("filters")) {
            		filters = cmd.getOptionValue("filters");
            	}
            	
            }

            if (errmsg.length()>0){
    			System.out.println(errmsg.toString());		
    			LOG.error(errmsg.toString());			
    			System.exit(1);            	
            }
            
            /* Run the package generation application proper */
            OsfLoaderCLI application = new OsfLoaderCLI();
            application.run(harvestType, processType, filters);
             	           	
        } catch (Exception e) {
            /*
             * This is an error in command line args, just print out usage data
             * and description of the error.
             */
            System.err.println(e.getMessage());
        	e.printStackTrace();
            System.exit(1);
        }
    }	

	/**
	 * Run the command
	 *
	 * @throws Exception the exception
	 */
	public void run(String harvesterType, String process, String filters) throws Exception{
		RecordType type = RecordType.getType(harvesterType);
		
		if (type==null){
			System.out.println("Only the following parameters are allowed for types: node, registration, or user");		
			LOG.error("Only the following parameters are allowed for types: node, registration, or user");			
			System.exit(1);
		}
		
		if (process.equals("identify")||process=="all") {
			OsfIdentifyService identify = new OsfIdentifyService(type, filters);
			Integer numIdentified = identify.identifyNewRecords();
			identify.close();
			String identifyMsg = "Number of " + harvesterType + "s identified for harvest:" + numIdentified;
			LOG.info(identifyMsg);
			System.out.println(identifyMsg);	
		}

		if (process.equals("transform")||process=="all") {
			Integer totalTransformed = 0;

			String transformQ = QueueName.getQueueName(QueueName.TRANSFORM, type, null);
			String transformRetry1Q = QueueName.getQueueName(QueueName.TRANSFORM, type, QueueName.RETRY1);
			String transformRetry2Q = QueueName.getQueueName(QueueName.TRANSFORM, type, QueueName.RETRY2);
			String transformRetry3Q = QueueName.getQueueName(QueueName.TRANSFORM, type, QueueName.RETRY3);
			String transformFailQ = QueueName.getQueueName(QueueName.TRANSFORM, type, QueueName.FAIL);
			
			Integer count = runTransform(transformRetry3Q, transformFailQ, type);
			totalTransformed = totalTransformed + count;

			count = runTransform(transformRetry2Q, transformRetry3Q, type);
			totalTransformed = totalTransformed + count;

			count = runTransform(transformQ, transformRetry1Q, type);
			totalTransformed = totalTransformed + count;

			count = runTransform(transformRetry1Q, transformRetry2Q, type);
			totalTransformed = totalTransformed + count;
			
			String transformMsg = "Number of " + harvesterType + "s processed from transform queues:" + totalTransformed;
			LOG.info(transformMsg);
			System.out.println(transformMsg);				
		}

		if (process.equals("ingest")||process=="all") {

			Integer numIngested = 0;
			
			String ingestQ = QueueName.getQueueName(QueueName.INGEST, type, null);
			String ingestRetry1Q = QueueName.getQueueName(QueueName.INGEST, type, QueueName.RETRY1);
			String ingestRetry2Q = QueueName.getQueueName(QueueName.INGEST, type, QueueName.RETRY2);
			String ingestRetry3Q = QueueName.getQueueName(QueueName.INGEST, type, QueueName.RETRY3);
			String ingestFailQ = QueueName.getQueueName(QueueName.INGEST, type, QueueName.FAIL);
			
			Integer count = runIngest(ingestRetry3Q, ingestFailQ, type);
			numIngested = numIngested + count;

			count = runIngest(ingestRetry2Q, ingestRetry3Q, type);
			numIngested = numIngested + count;

			count = runIngest(ingestQ, ingestRetry1Q, type);
			numIngested = numIngested + count;

			count = runIngest(ingestRetry1Q, ingestRetry2Q, type);
			numIngested = numIngested + count;
			
			String ingestMsg = "Number of " + harvesterType + "s processed from ingest queues:" + numIngested;
			LOG.info(ingestMsg);
			System.out.println(ingestMsg);				
		}
		
		String completeMsg = "Harvest process completed!";
		LOG.info(completeMsg);
		System.out.println(completeMsg);				
		
	}
	
	
	private Integer runTransform(String fromQueue, String failQueue, RecordType type) throws Exception {
		//checks failures from last harvest first
		OsfTransformService transformService = new OsfTransformService();
		Integer count = transformService.transformRecords(fromQueue, failQueue, type);
		transformService.close();
		return count;
	}
	
	
	private Integer runIngest(String fromQueue, String failQueue, RecordType type) throws Exception {
		//checks failures from last harvest first
		OsfIngestService ingestService = new OsfIngestService();
		Integer count = ingestService.ingestRecords(fromQueue, failQueue);
		ingestService.close();
		return count;
	}
	
	

}
