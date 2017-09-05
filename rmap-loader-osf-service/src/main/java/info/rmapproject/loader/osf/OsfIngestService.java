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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import info.rmapproject.loader.HarvestRecord;
import info.rmapproject.loader.HarvestRecordStatus;
import info.rmapproject.loader.deposit.disco.DiscoDepositConsumer;
import info.rmapproject.loader.deposit.disco.RdbmsHarvestRecordRegistry;
import info.rmapproject.loader.osf.jms.OsfJmsQueue;
import info.rmapproject.loader.util.LogUtil;
import info.rmapproject.loader.validation.DiscoValidator;
import info.rmapproject.loader.validation.DiscoValidator.Format;

public class OsfIngestService {	

	/** The log. */
	private static final Logger LOG = LoggerFactory.getLogger(OsfIngestService.class);

	/**
	 * Datasource for saved data
	 */
	private DataSource datasource;
	
		/**
	 * JMS Queue instance to be used for queue management
	 */
	private OsfJmsQueue jmsQueue;
	
	/**Tally of ingested in current session**/
	private Integer numProcessed = 0;
	
	
	public OsfIngestService(){
		
		LogUtil.adjustLogLevels();
		
		this.jmsQueue = new OsfJmsQueue();
		
		//initiate data source
        final HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(string("jdbc.url", "jdbc:sqlite:"));
        ds.setUsername(string("jdbc.username", null));
        ds.setPassword(string("jdbc.password", null));
        ds.setDriverClassName(string("jdbc.driver",null));
        this.datasource = ds;
		
	}


	
	/**
	 * Reads records in queue specified, moves failures to fail queue, successes to ingest queue.
	 * @param fromQueue
	 * @param failQueue
	 * @return
	 */
	protected Integer ingestRecords(String fromQueue, String failQueue) {
		DiscoDepositConsumer discoDepositer = new DiscoDepositConsumer();
		RdbmsHarvestRecordRegistry registry = new RdbmsHarvestRecordRegistry();
		registry.setDataSource(datasource);
		registry.init();
		
		discoDepositer.setHarvestRegistry(registry);        
		discoDepositer.setAuthToken(string("rmap.api.auth.token", null));
		discoDepositer.setRmapDiscoEndpoint(makeDiscoEndpointUri());
		
		Consumer<HarvestRecord> consumer = received -> {
			InputStream rdf = new ByteArrayInputStream(received.getBody());
			String id = received.getRecordInfo().getId().toString();
			try {
				DiscoValidator.validate(rdf, Format.TURTLE);
				
				HarvestRecordStatus status = registry.getStatus(received.getRecordInfo());
				if (!status.isUpToDate()) {
					
					if (status.recordExists()){
						//compare rdf
						rdf = new ByteArrayInputStream(received.getBody());
						InputStream currRmapRdf = getDiscoRdf(status.latest().toString());
						if (DiscoValidator.different(currRmapRdf, rdf, Format.TURTLE)) {
							discoDepositer.accept(received);
						} else {
							//update the registry date even though it didn't change
							registry.register(received.getRecordInfo(), status.latest());
							LOG.info("The DiSCO for record: " + id + " from ingest queue: " + fromQueue + " has not changed since the last harvest. Skipping.");							
						}
					} else {
						discoDepositer.accept(received);
					}
				} else {
					LOG.info("The latest version of the record: " + id + " from ingest queue: " + fromQueue + " already exists. Skipping.");			
				}
				numProcessed = numProcessed+1;
								
				LOG.info("Processed record: " + id + " from ingest queue: " + fromQueue);
			} catch (Exception ex) {
				LOG.error("Ingest failed for record from source: " + id, ex);
				//add to fail queue
				jmsQueue.add(received, failQueue);
				LOG.error("Record not ingested:" + received.getRecordInfo().getId() + " added to fail queue:" + failQueue);
			} 
		};
				
		jmsQueue.processMessages(fromQueue, consumer);
		
		LOG.info(numProcessed + " records processed from ingest queue " + fromQueue);
		return numProcessed;

	}

    private static URI makeDiscoEndpointUri() {
        return URI.create(string("rmap.api.baseuri",
                "https://test.rmap-hub.org/api/").replaceFirst("/$", "") + "/discos/");
    }
     
    private InputStream getDiscoRdf(String latestDiscoUri) {

        try {
        	HttpClient client = HttpClientBuilder.create()
                    .setConnectionManager(new PoolingHttpClientConnectionManager(
                            RegistryBuilder
                                    .<ConnectionSocketFactory> create()
                                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                                    .register("https", new SSLConnectionSocketFactory(
                                            SSLContexts.custom()
                                                    .loadTrustMaterial(new TrustSelfSignedStrategy()).build(),
                                            new NoopHostnameVerifier()))
                                    .build()))
                    .setRedirectStrategy(new DefaultRedirectStrategy())
                    .build();
        	
        	
            URI uri = new URI(makeDiscoEndpointUri() + URLEncoder.encode(latestDiscoUri, "UTF-8"));
			HttpGet get = new HttpGet(uri);
			
			HttpResponse response = client.execute(get);

	            if (response.getStatusLine().getStatusCode() == 200) {
	                return response.getEntity().getContent();
	            } else {
	                throw new RuntimeException(String.format("Unexpected status code %s; '%s'", response.getStatusLine()
	                        .getStatusCode(), IOUtils.toString(response.getEntity().getContent(), UTF_8)));
	            }

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    
    
    public void close() {
		jmsQueue.close();
    }
    
}
