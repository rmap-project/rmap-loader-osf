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
package info.rmapproject.loader.osf.transformer;

import info.rmapproject.cos.osf.client.model.RecordType;

/**
 * A factory for creating DiscoBuilder objects.
 * @author khanson
 */
public class DiscoTransformerFactory {
	
	/**
	 * Creates a new DiscoBuilder object based on type requested.
	 *
	 * @param type the source data type
	 * @param discoDescription the DiSCO description
	 * @return the new DiSCO builder
	 */
	public static DiscoTransformer createDiscoBuilder(RecordType type) {
			
			DiscoTransformer transformer = null;
						
			switch(type){
			case OSF_REGISTRATION:
				transformer = new OsfRegistrationDiscoTransformer();
				break;
			case OSF_USER:
				transformer = new OsfUserDiscoTransformer();
				break;
			case OSF_NODE:
				transformer = new OsfNodeDiscoTransformer();
				break;
			default:
				transformer = new OsfNodeDiscoTransformer();
				break;		
			}
			
			return transformer;
	}
	

}
