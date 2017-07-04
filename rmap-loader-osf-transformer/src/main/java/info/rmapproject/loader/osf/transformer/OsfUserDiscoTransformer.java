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

import info.rmapproject.cos.osf.client.model.Institution;
import info.rmapproject.cos.osf.client.model.User;
import info.rmapproject.loader.osf.transformer.DiscoTransformer;
import info.rmapproject.loader.osf.transformer.Terms;

import java.util.List;

import org.openrdf.model.IRI;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

/** 
 * Performs mapping from OSF Registration Java model to RDF DiSCO model.  
 * (Java Model -> RDF).
 * @author khanson
 *
 */

public class OsfUserDiscoTransformer extends DiscoTransformer {

	/** The User record. */
	private User record;
	
	/** The user id. */
	private IRI userId;

	/** Default DiSCO creator. */
	protected static final String DEFAULT_CREATOR = Terms.RMAPAGENT_NAMESPACE + "RMap-OSF-Harvester-0.1";
	
	/** Default DiSCO description. */
	protected static final String DEFAULT_DESCRIPTION = "User record harvested from OSF API v2";
	
	/** OSF path prefix. */
	protected static final String OSF_PATH_PREFIX = "https://osf.io/";
	
	/** ORCID path prefix. */
	protected static final String ORCID_PREFIX = "http://orcid.org/";
	
	/** Researcher ID path prefix. */
	protected static final String RESEARCHERID_PREFIX = "http://researcherid.com/rid/";
	
	/** Twitter path prefix. */
	protected static final String TWITTER_PREFIX = "https://twitter.com/";
	
	/** GitHub path prefix. */
	protected static final String GITHUB_PREFIX = "https://github.com/";
	
	/** Google Scholar path prefix. */
	protected static final String GOOGLESCHOLAR_PREFIX = "http://scholar.google.com/citations?user=";
	
	/** LinkedIn path prefix. */
	protected static final String LINKEDIN_PREFIX = "https://www.linkedin.com/";
	
	/** ImpactStory path prefix. */
	protected static final String IMPACTSTORY_PREFIX = "https://impactstory.org/";
	
	/** ResearchGate path prefix. */
	protected static final String RESEARCHGATE_PREFIX = "https://researchgate.net/profile/";
	
	/** BaiduScholar path prefix. */
	protected static final String BAIDUSCHOLAR_PREFIX = "http://xueshu.baidu.com/scholarID/";
	
	/** Academia path prefix. */
	protected static final String ACADEMIA_PREFIX = ".academia.edu/";
	

	
	/**
	 * Constructor for Node to pass default params up to super().
	 */
	public OsfUserDiscoTransformer(){
		super(DEFAULT_CREATOR, DEFAULT_DESCRIPTION);
	}
	
	
	/**
	 * Constructor for Node to pass default params up to super().
	 */
	public OsfUserDiscoTransformer(User user){
		super(DEFAULT_CREATOR, DEFAULT_DESCRIPTION);
		setRecord(user);
	}
	
	
	/**
	 * Constructor for Node to pass params up to super().
	 *
	 * @param discoDescription the disco description
	 */
	public OsfUserDiscoTransformer(User user, String discoDescription){
		super(DEFAULT_CREATOR, discoDescription);
		setRecord(user);
	}
		
	
	/**
	 * Constructor for Node to pass params up to super().
	 *
	 * @param discoCreator the DiSCO creator
	 * @param discoDescription the DiSCO description
	 */
	public OsfUserDiscoTransformer(User user, String discoCreator, String discoDescription){
		super(discoCreator, discoDescription);
		setRecord(user);
	}

	

	/**
	 * 
	 * @param record
	 */
	public void setRecord(User record) {
		this.record = record;
		discoId = null;
		model = null;
	}
	
	/* (non-Javadoc)
	 * @see info.rmapproject.transformer.DiscoBuilder#getModel()
	 */
	@Override
	public Model getModel()	{
		model = new LinkedHashModel();		
		discoId = factory.createBNode(); 	
		userId = factory.createIRI(OSF_PATH_PREFIX + record.getId() + "/");				

		//disco header
		addDiscoHeader();
		addUser();
				
		return model;		
	}

	/**
	 * Adds the user to the model.
	 */
	private void addUser(){
				
		addStmt(discoId, Terms.ORE_AGGREGATES, userId);
		
		addStmt(userId, RDF.TYPE, FOAF.PERSON);
		addLiteralStmt(userId, FOAF.GIVEN_NAME, record.getGiven_name());
		addLiteralStmt(userId, FOAF.FAMILY_NAME, record.getFamily_name());
		addLiteralStmt(userId, FOAF.NAME, record.getFull_name());
		//TODO: not sure if this is the appropriate use of this...
		addLiteralStmt(userId, Terms.VCARD_ADDITIONALNAME, record.getMiddle_names());
		//TODO: ditto with this mapping - not sure if it's exactly right... is "jr." honorific?!
		addLiteralStmt(userId, Terms.VCARD_HONORIFICSUFFIX, record.getSuffix());
		
		// now for all of the possible accounts...
		//TODO: this not working at teh moment so commenting out.
//		List<String> websites = record.getPersonal_websites();
//		if (websites!=null){
//			for (String website : websites){
//				addIriStmt(userId, RDFS.SEEALSO, website);				
//			}
//		}
		if (record.getAcademiaProfileId()!=null 
				&& record.getAcademicaInstitution()!=null
				&& record.getAcademiaProfileId().length()>0 
				&& record.getAcademicaInstitution().length()>0){
			String academiaUrl = "https://" + record.getAcademicaInstitution() 
								+ ACADEMIA_PREFIX + record.getAcademiaProfileId();
			addIriStmt(userId, RDFS.SEEALSO, academiaUrl);
		}
		
		if (record.getBaiduScholar()!=null && record.getBaiduScholar().length()>0){
			addIriStmt(userId, RDFS.SEEALSO, BAIDUSCHOLAR_PREFIX + record.getBaiduScholar());
		}
		if (record.getGitHub()!=null && record.getGitHub().length()>0){
			addIriStmt(userId, RDFS.SEEALSO, GITHUB_PREFIX + record.getGitHub());
		}
		if (record.getImpactStory()!=null && record.getImpactStory().length()>0){		
			addIriStmt(userId, RDFS.SEEALSO, IMPACTSTORY_PREFIX + record.getImpactStory());
		}
		if (record.getLinkedIn()!=null && record.getLinkedIn().length()>0){		
			addIriStmt(userId, RDFS.SEEALSO, LINKEDIN_PREFIX + record.getLinkedIn());
		}
		if (record.getOrcid()!=null && record.getOrcid().length()>0){		
			addIriStmt(userId, RDFS.SEEALSO, ORCID_PREFIX + record.getOrcid());
		}
		if (record.getResearcherId()!=null && record.getResearcherId().length()>0){		
			addIriStmt(userId, RDFS.SEEALSO, RESEARCHERID_PREFIX + record.getResearcherId());
		}
		if (record.getResearchGate()!=null && record.getResearchGate().length()>0){		
			addIriStmt(userId, RDFS.SEEALSO, RESEARCHGATE_PREFIX + record.getResearchGate());
		}
		if (record.getScholar()!=null && record.getScholar().length()>0){		
			addIriStmt(userId, RDFS.SEEALSO, GOOGLESCHOLAR_PREFIX + record.getScholar());
		}
		if (record.getTwitter()!=null && record.getTwitter().length()>0){		
			addIriStmt(userId, RDFS.SEEALSO, TWITTER_PREFIX + record.getTwitter());
		}

		addInstitutions();
	}
	
	
	/**
	 * Adds the institutions to the model
	 */
	private void addInstitutions(){
		List<Institution> institutions = record.getInstitutions();
		if (institutions!=null){
			for(Institution inst : institutions){
				Resource roleNode = factory.createBNode();
				IRI instIri = factory.createIRI(OSF_PATH_PREFIX + inst.getId());
				addStmt(userId, Terms.PRO_HOLDSROLEINTIME, roleNode);
				addStmt(roleNode, Terms.PRO_WITHROLE, Terms.SCORO_AFFILIATE);
				addStmt(roleNode, Terms.PRO_RELATESTOORGANIZATION, instIri);
				addStmt(instIri, RDF.TYPE, FOAF.ORGANIZATION);
				addLiteralStmt(instIri, FOAF.NAME, inst.getName());
			}
		}
	}
	
}
