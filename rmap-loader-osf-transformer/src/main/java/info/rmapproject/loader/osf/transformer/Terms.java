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

import org.openrdf.model.IRI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;

/**
 * Vocabulary constants for the RMapProject Metadata Element Set, version 1.0
 *
 * @author khanson
 * @see http://rmap-project.org/elements/
 */
public class Terms {
	
 	/**RMap vocabulary constants*/
	public static final String RMAP_NAMESPACE = "http://purl.org/ontology/rmap#";
	
	/** The RMap ontology prefix */
	public static final String RMAP_PREFIX = "rmap";
	
	/** The RMap ontology term for DiSCO. */
	public static final String DISCO = "DiSCO";

	/**ORE ontology namespace*/
	public static final String ORE_NAMESPACE = "http://www.openarchives.org/ore/terms/";
	
	/** The ORE ontology prefix. */
	public static final String ORE_PREFIX = "ore";
	
	/** The ORE ontology term for aggregates. */
	public static final String AGGREGATES = "aggregates";

	/** The v-card ontology namespace. */
	public static final String VCARD_NAMESPACE = "http://www.w3.org/2006/vcard/ns#";
	
	/** The v-card ontology prefix. */
	public static final String VCARD_PREFIX = "vcard";
	
	/** The v-card ontology for additional-name. */
	public static final String ADDITIONALNAME = "additional-name";

	/** The v-card ontology for honorific-suffix. */
	public static final String HONORIFICSUFFIX = "honorific-suffix";
	

	/** The prov ontology namespace. */
	public static final String PROV_NAMESPACE = "http://www.w3.org/ns/prov#";

	/** The prov ontology prefix. */
	public static final String PROV_PREFIX = "prov";

	/** The prov ontology for wasDerivedFrom. */
	public static final String WASDERIVEDFROM = "wasDerivedFrom";

	/** The pro ontology namespace. */
	public static final String PRO_NAMESPACE = "http://purl.org/spar/pro/";

	/** The pro ontology prefix. */
	public static final String PRO_PREFIX = "pro";

	/** The pro ontology for holdsRoleInTime. */
	public static final String HOLDSROLEINTIME = "holdsRoleInTime";

	/** The pro ontology for RoleInTime. */
	public static final String ROLEINTIME = "RoleInTime";

	/** The pro ontology for withRole. */
	public static final String WITHROLE = "withRole";

	/** The pro ontology for relatesToOrganization. */
	public static final String RELATESTOORGANIZATION = "relatesToOrganization";


	/** The frapo ontology namespace. */
	public static final String FRAPO_NAMESPACE = "http://purl.org/cerif/frapo/";

	/** The frapo ontology prefix. */
	public static final String FRAPO_PREFIX = "frapo";

	/** The frapo ontology term for isOutputOf. */
	public static final String ISOUTPUTOF = "isOutputOf";

	/** The frapo ontology term for isFundedBy. */
	public static final String ISFUNDEDBY = "isFundedBy";

	/** The frapo ontology term for isAwardedBy. */
	public static final String ISAWARDEDBY = "isAwardedBy";

	/** The frapo ontology term for FundingAgency. */
	public static final String FUNDINGAGENCY = "FundingAgency";

	/** The frapo ontology term for Funding. */
	public static final String FUNDING = "Funding";

	/** The fabio ontology namespace*/
	public static final String FABIO_NAMESPACE = "http://purl.org/spar/fabio/";

	/** The fabio ontology prefix*/
	public static final String FABIO_PREFIX = "fabio";

	/** The fabio ontology term for Expression*/
	public static final String EXPRESSION = "Expression";

	/** The fabio ontology term for ComputerFile*/
	public static final String COMPUTERFILE = "ComputerFile";

	/** The scoro ontology namespace*/
	public static final String SCORO_NAMESPACE = "http://purl.org/spar/scoro/";

	/** The scoro ontology prefix*/
	public static final String SCORO_PREFIX = "scoro";

	/** The scoro ontology term for affiliate*/
	public static final String AFFILIATE = "affiliate";
		

	/** The premis ontology namespace*/
	public static final String PREMIS_NAMESPACE = "http://www.loc.gov/premis/rdf/v1#";

	/** The premis ontology prefix*/
	public static final String PREMIS_PREFIX = "premis";

	/** The premis ontology term for file*/
	public static final String FILE = "File";

	/** The RMap Agent ontology namespace*/
	public static final String RMAPAGENT_NAMESPACE = "http://rmap-hub.org/rmap/agents/";
		
	/** Mailto term for email prefixes. */
	public static final String MAILTO = "mailto:";
	
	/**  rmap:DiSCO. */
	public static final IRI RMAP_DISCO;
	
	/**  ore:aggregates. */
	public static final IRI ORE_AGGREGATES;
	
	/**  vcard:additional-name. */
	public static final IRI VCARD_ADDITIONALNAME;
	
	/**  vcard:honorific-suffix. */
	public static final IRI VCARD_HONORIFICSUFFIX;
	
	/**  prov:wasDerivedFrom. */
	public static final IRI PROV_WASDERIVEDFROM;
	
	/**  pro:holdsRoleInTime. */
	public static final IRI PRO_HOLDSROLEINTIME;
	
	/**  pro:RoleInTime. */
	public static final IRI PRO_ROLEINTIME;
	
	/**  pro:withRole. */
	public static final IRI PRO_WITHROLE;
	
	/**  pro:relatesToOrganization. */
	public static final IRI PRO_RELATESTOORGANIZATION;
	
	/**  scoro:Affiliate. */
	public static final IRI SCORO_AFFILIATE;
	
	/**  frapo:isOutputOf. */
	public static final IRI FRAPO_ISOUTPUTOF;
	
	/**  frapo:isFundedBy. */
	public static final IRI FRAPO_ISFUNDEDBY;
	
	/**  frapo:isAwardedBy. */
	public static final IRI FRAPO_ISAWARDEDBY;
	
	/**  frapo:FundingAgency. */
	public static final IRI FRAPO_FUNDINGAGENCY;
	
	/**  frapo:Funding. */
	public static final IRI FRAPO_FUNDING;
	
	/**  fabio:Expression. */
	public static final IRI FABIO_EXPRESSION;
	
	/**  fabio:ComputerFile. */
	public static final IRI FABIO_COMPUTERFILE;
	
	/**  premis:File. */
	public static final IRI PREMIS_FILE;
	
	static {
		final ValueFactory f = SimpleValueFactory.getInstance();
		//rmap object types 
		RMAP_DISCO = f.createIRI(RMAP_NAMESPACE, DISCO);
		ORE_AGGREGATES = f.createIRI(ORE_NAMESPACE, AGGREGATES);
		VCARD_ADDITIONALNAME = f.createIRI(VCARD_NAMESPACE, ADDITIONALNAME);
		VCARD_HONORIFICSUFFIX = f.createIRI(VCARD_NAMESPACE, HONORIFICSUFFIX);
		PROV_WASDERIVEDFROM = f.createIRI(PROV_NAMESPACE,WASDERIVEDFROM);
		PRO_HOLDSROLEINTIME = f.createIRI(PRO_NAMESPACE, HOLDSROLEINTIME);
		PRO_ROLEINTIME = f.createIRI(PRO_NAMESPACE, ROLEINTIME);
		PRO_WITHROLE = f.createIRI(PRO_NAMESPACE, WITHROLE);
		PRO_RELATESTOORGANIZATION = f.createIRI(PRO_NAMESPACE, RELATESTOORGANIZATION);
		SCORO_AFFILIATE = f.createIRI(SCORO_NAMESPACE, AFFILIATE);		
		FRAPO_ISFUNDEDBY = f.createIRI(FRAPO_NAMESPACE, ISFUNDEDBY);		
		FRAPO_ISOUTPUTOF = f.createIRI(FRAPO_NAMESPACE, ISOUTPUTOF);		
		FRAPO_ISAWARDEDBY = f.createIRI(FRAPO_NAMESPACE, ISAWARDEDBY);		
		FRAPO_FUNDINGAGENCY = f.createIRI(FRAPO_NAMESPACE, FUNDINGAGENCY);		
		FRAPO_FUNDING = f.createIRI(FRAPO_NAMESPACE, FUNDING);		
		FABIO_EXPRESSION = f.createIRI(FABIO_NAMESPACE, EXPRESSION);	
		FABIO_COMPUTERFILE = f.createIRI(FABIO_NAMESPACE, COMPUTERFILE);
		PREMIS_FILE = f.createIRI(PREMIS_NAMESPACE, FILE);		
	}
}