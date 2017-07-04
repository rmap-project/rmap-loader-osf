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

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.rmapproject.cos.osf.client.model.RecordType;
import info.rmapproject.loader.HarvestRecord;
import info.rmapproject.loader.model.HarvestInfo;
import info.rmapproject.loader.model.RecordInfo;

public class OsfHarvestableRecord extends HarvestRecord{

	/** The log. */
	private static final Logger LOG = LoggerFactory.getLogger(OsfHarvestableRecord.class);
	
	protected OsfLightRecordDTO harvestableRec;
		
	public OsfHarvestableRecord(OsfLightRecordDTO harvestableRec) {

		try {
			RecordType type = harvestableRec.getType();
			String id = harvestableRec.getId();
	
			final RecordInfo recordInfo = new RecordInfo();
	        recordInfo.setContentType(type.getTypeString());
	        Date filterDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(harvestableRec.getFilterDate().toString("yyyy-MM-dd HH:mm:ss"));
	        recordInfo.setDate(filterDate);
	        recordInfo.setId(URI.create("https://osf.io/" + id + "/"));
	        recordInfo.setSrc(URI.create("https://api.osf.io/v2/" + type.getTypeString() + "/" + id + "/"));
	
	        final HarvestInfo harvestInfo = new HarvestInfo();
	        recordInfo.setHarvestInfo(harvestInfo);
	        Date retrieveDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(harvestableRec.getRetrievedDate().toString("yyyy-MM-dd HH:mm:ss"));
	        harvestInfo.setDate(retrieveDate);
	        harvestInfo.setId(URI.create("https://osf.io/" + id + "/"));
	        harvestInfo.setSrc(URI.create("https://api.osf.io/v2/" + type.getTypeString() + "/" + id + "/"));
	
	        this.setRecordInfo(recordInfo);
	        this.setBody(id.getBytes());
		} catch (Exception ex){
			LOG.error("Could not convert OsfLightRecord to a HarvestRecord.",ex);
			throw new RuntimeException("Could not convert OsfLightRecord to a HarvestRecord.",ex);
		}
	}

}
