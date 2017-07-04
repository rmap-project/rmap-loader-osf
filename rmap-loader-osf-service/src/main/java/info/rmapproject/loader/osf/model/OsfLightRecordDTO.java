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

import org.joda.time.DateTime;

import info.rmapproject.cos.osf.client.model.RecordType;

public class OsfLightRecordDTO {

	String id;
	
	DateTime filterDate;
	
	RecordType type;
	
	DateTime retrievedDate;
	
	public OsfLightRecordDTO(String id, DateTime filterDate, RecordType type, DateTime retrievedDate) {
		this.id=id;
		this.filterDate=filterDate;
		this.type = type;
		this.retrievedDate = retrievedDate;
	}

	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public DateTime getFilterDate() {
		return filterDate;
	}

	public void setFilterDate(DateTime filterDate) {
		this.filterDate = filterDate;
	}

	public RecordType getType() {
		return type;
	}

	public void setType(RecordType type) {
		this.type = type;
	}
	
	public DateTime getRetrievedDate() {
		return retrievedDate;
	}

	public void setRetrievedDate(DateTime retrievedDate) {
		this.retrievedDate = retrievedDate;
	}

	

}
