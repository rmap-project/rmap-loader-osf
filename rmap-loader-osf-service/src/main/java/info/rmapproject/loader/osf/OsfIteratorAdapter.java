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

import java.util.HashMap;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import info.rmapproject.cos.osf.client.model.LightNode;
import info.rmapproject.cos.osf.client.model.LightRegistration;
import info.rmapproject.cos.osf.client.model.LightUser;
import info.rmapproject.cos.osf.client.model.RecordType;
import info.rmapproject.cos.osf.client.service.OsfApiIterator;
import info.rmapproject.cos.osf.client.service.OsfNodeApiIterator;
import info.rmapproject.cos.osf.client.service.OsfRegistrationApiIterator;
import info.rmapproject.cos.osf.client.service.OsfUserApiIterator;
import info.rmapproject.loader.osf.model.OsfLightRecordDTO;

/**
 * This optional adapter sits over the top of the various RMap OSF client iterators and converts the records to a 
 * light DTO for use in harvesting.
 * @author khanson
 *
 */
public class OsfIteratorAdapter implements Iterator<OsfLightRecordDTO>{

	HashMap<String,String> params;
	
	RecordType type;
	
	Iterator<?> iterator;
		
	public OsfIteratorAdapter(HashMap<String,String> params, RecordType type) {
		this.params = params;
		this.type = type;
		Iterator<?> iterator = null;
		
		switch (type) {
		case OSF_NODE : 
			iterator = new OsfNodeApiIterator(params); //new OsfNodeApiIterator(params);
			break;
		case OSF_REGISTRATION :
			iterator = new OsfRegistrationApiIterator(params);
			break;
		case OSF_USER :
			iterator = new OsfUserApiIterator(params);
			break;
		default:
			iterator = new OsfNodeApiIterator(params);
			break;
		}
		
		this.iterator = iterator;		
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}
	

	@Override
	public OsfLightRecordDTO next() {
		DateTime datFilterBy=null;
		DateTime datRetrieved= null;
		String id = null;

		switch (type) {
		case OSF_NODE : 
			LightNode node = (LightNode)iterator.next();
			id = node.getId();
			datFilterBy = node.getDate_modified();
			break;
		case OSF_REGISTRATION :
			LightRegistration reg = (LightRegistration)iterator.next();
			id = reg.getId();
			datFilterBy = reg.getDate_modified();			
			break;
		case OSF_USER :
			LightUser user = (LightUser)iterator.next();
			id = user.getId();
			datFilterBy = user.getDate_registered();
			break;
		default:
			LightNode node2 = (LightNode) iterator.next();
			id = node2.getId();
			datFilterBy = node2.getDate_modified();
			break;
		}

		datRetrieved = ((OsfApiIterator<?>) iterator).getRecordRetrievedDate();
		
		//TODO: temporary fix - OSF client is converting date to local timezone without adjusting time.
		DateTime datTimeZoneCorrected = datFilterBy.withZoneRetainFields(DateTimeZone.UTC);
		OsfLightRecordDTO osfRecord = new OsfLightRecordDTO(id, datTimeZoneCorrected,type, datRetrieved);
		return osfRecord;
	}
	
	
}
