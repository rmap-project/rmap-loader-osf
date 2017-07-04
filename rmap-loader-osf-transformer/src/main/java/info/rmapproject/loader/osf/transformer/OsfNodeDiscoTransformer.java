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

import java.util.List;
import java.util.Map;

import org.openrdf.model.IRI;
import org.openrdf.model.Model;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.rmapproject.cos.osf.client.model.Category;
import info.rmapproject.cos.osf.client.model.Contributor;
import info.rmapproject.cos.osf.client.model.Identifier;
import info.rmapproject.cos.osf.client.model.Node;

/** 
 * Performs mapping from OSF Registration Java model to RDF DiSCO model.  
 * (Java Model -> RDF).
 * @author khanson
 *
 */

public class OsfNodeDiscoTransformer extends DiscoTransformer {

	/** The log. */
	private static final Logger log = LoggerFactory.getLogger(OsfNodeDiscoTransformer.class);
	
	/** The Node record. */
	private Node record;

	/** Default DiSCO creator. */
	protected static final String DEFAULT_CREATOR = Terms.RMAPAGENT_NAMESPACE + "RMap-OSF-Harvester-0.1";
	
	/** Default DiSCO description. */
	protected static final String DEFAULT_DESCRIPTION = "Record harvested from OSF API";
	
	/** OSF path prefix. */
	protected static final String OSF_PATH_PREFIX = "https://osf.io/";
	
	/** OSF ontology prefix. */
	//TODO: replace these with proper ontology term!
	protected static final String OSF_TERMS_PREFIX = "http://osf.io/terms/";
	
	/** OSF Registration ontology term. */
	protected static final String OSF_REGISTRATION = OSF_TERMS_PREFIX + "Registration";
	
	/** OSF Project ontology term. */
	protected static final String OSF_PROJECT = OSF_TERMS_PREFIX + "Project";
		
	/** DOI identifier category String **/
	protected static final String DOI_ID_CATEGORY = "doi";
	
	/** ARK identifier category String **/
	protected static final String ARK_ID_CATEGORY = "ark";
	
	/** ARK prefix. */
	protected static final String ARK_PREFIX = "ark:/";
		

	
	/**
	 * Constructor for Node to pass default params up to super().
	 */
	public OsfNodeDiscoTransformer(){
		super(DEFAULT_CREATOR, DEFAULT_DESCRIPTION);
	}
	
	
	/**
	 * Constructor for Node to pass default params up to super().
	 */
	public OsfNodeDiscoTransformer(String discoCreator, String discoDescription){
		super(discoCreator, discoDescription);
		setRecord(record);
	}
	
	/**
	 * Constructor for Node to pass default params up to super().
	 */
	public OsfNodeDiscoTransformer(Node record){
		super(DEFAULT_CREATOR, DEFAULT_DESCRIPTION);
		setRecord(record);
	}
	
	
	/**
	 * Constructor for Node to pass params up to super().
	 *
	 * @param discoDescription the DiSCO description
	 */
	public OsfNodeDiscoTransformer(Node record, String discoDescription){
		super(DEFAULT_CREATOR, discoDescription);
		setRecord(record);
	}
		
	
	/**
	 * Constructor for Node to pass params up to super().
	 *
	 * @param discoCreator the DiSCO creator
	 * @param discoDescription the DiSCO description
	 */
	public OsfNodeDiscoTransformer(Node record, String discoCreator, String discoDescription){
		super(discoCreator, discoDescription);
		setRecord(record);
	}


	
	/**
	 * Set Node record to convert
	 * @param record
	 */
	public void setRecord(Node record) {
		this.record = record;
		discoId = null;
		model = null;
	}
	
	/* (non-Javadoc)
	 * @see info.rmapproject.transformer.DiscoBuilder#getModel()
	 */
	@Override
	public Model getModel()	{
		if (record==null){
			throw new RuntimeException("Record value not set. Record value required before a model can be retrieved");
		}
		
		model = new LinkedHashModel();		
		discoId = factory.createBNode(); 					
		//disco header
		addDiscoHeader();
		addNode(record, null);
		
		//fill in
		
		return model;		
	}

	/**
	 * Adds the node to DiSCO graph
	 *
	 * @param node the Node
	 * @param parentId the parent node ID
	 */
	private void addNode(Node node, IRI parentId){
				
		IRI nodeId = factory.createIRI(OSF_PATH_PREFIX + node.getId() + "/");

		log.info("OSF DiSCO Transform: Adding data for node ID " + nodeId + "to DiSCO");
		
		addStmt(discoId, Terms.ORE_AGGREGATES, nodeId);

		if (parentId!=null){
			addStmt(parentId, DCTERMS.HAS_PART, nodeId);
		}

		IRI category = mapCategoryToIri(node.getCategory());
		addStmt(nodeId, RDF.TYPE, category);

		addIdentifiers(node.getIdentifiers(), nodeId);

		addForkedFrom(node.getForked_from(), nodeId);
		
		addLiteralStmt(nodeId, DCTERMS.CREATED, node.getDate_created());

		addLiteralStmt(nodeId, DCTERMS.TITLE, node.getTitle());
		addLiteralStmt(nodeId, DCTERMS.DESCRIPTION, node.getDescription());
		
		addContributors(node.getContributors(), nodeId);				
		addChildNodes(node.getChildren(), nodeId);

		//addFiles(node, nodeId); 
	}
	
	
	/**
	 * Add child node metadata to DiSCO graph.
	 *
	 * @param children the children
	 * @param parentId the parent id
	 */
	private void addChildNodes(List<Node> children, IRI parentId){
		if (children!=null){
			for (Node child : children) {
				addNode(child, parentId);
			}
		}
		
	}
	
	
	/**
	 * Add contributor metadata to DiSCO model.
	 *
	 * @param contributors the contributors
	 * @param nodeId the node id
	 */
	protected void addContributors(List<Contributor> contributors, IRI nodeId){
		if (contributors!=null){
			for (Contributor contributor : contributors) {
				Map<String,?> links = contributor.getLinks();				
				String link = (String) links.get("self");
				link = TransformUtils.extractLastSubFolder(link);
				//TODO... does this have /user/ in the path?
				IRI userId = factory.createIRI(OSF_PATH_PREFIX + link + "/");
				addStmt(nodeId, DCTERMS.CONTRIBUTOR, userId);
				addStmt(userId, RDF.TYPE, FOAF.PERSON);
			}
		}	
	}

	/**
	 * Adds the forked from metadata to the DiSCO graph.
	 *
	 * @param forkRef the fork reference
	 * @param nodeId the Node ID
	 */
	protected void addForkedFrom(String forkRef, IRI nodeId){
		if (forkRef!=null && forkRef.length()>0){
			String forkedFromNodeId = TransformUtils.extractLastSubFolder(forkRef);
			IRI forkId = factory.createIRI(OSF_PATH_PREFIX + forkedFromNodeId + "/");
			addStmt(nodeId, Terms.PROV_WASDERIVEDFROM, forkId);			
		}
	}
	
	
	
	/**
	 * Adds the identifiers (ark, doi) to the model.
	 * @param identifiers list of identifiers
	 * @param regIdIri registration IRI
	 */
	protected void addIdentifiers(List<Identifier> identifiers, IRI regIdIri){
		if (identifiers!=null){
			for (Identifier identifier : identifiers) {
				String category = identifier.getCategory();
				String value = identifier.getValue();
				if (category.equals(DOI_ID_CATEGORY) && TransformUtils.isDoi(value)){ 
					
					String doi = TransformUtils.normalizeDoi(value);
					addIriStmt(regIdIri, DCTERMS.IDENTIFIER, doi); // https://doi.org format
					
					//also add non-http format of ID - replace 
					doi = doi.replace(TransformUtils.getHttpDoiPrefix(), TransformUtils.getNonHttpDoiPrefix());
					addIriStmt(regIdIri, DCTERMS.IDENTIFIER, doi); //doi: format
					
				} else if (category.equals(ARK_ID_CATEGORY)) {
					addIriStmt(regIdIri, DCTERMS.IDENTIFIER, ARK_PREFIX + value);
				}
			}
		}	
	}	
	
	
	
	/**
	 * Add file metadata to Model.
	 *
	 * @param root the root
	 * @param regId the reg id
	 */
	/*protected void addFiles(Object root, IRI regId) {
		List<File> files = null;
		if (root instanceof Registration) {
			Registration registration = (Registration) root;
			files = registration.getFiles();
		} else if (root instanceof File) {
			File file = (File) root;
			files = file.getFiles();
		} else if (root instanceof Node){
			NodeBase node = (NodeBase) root;
			files = node.getFiles();
		}
		
		if (files!=null)	{
			for (File file : files){
				addFile(file, regId);
			}
		}
	}*/

	/**
	 * Add OSF file metadata to model.
	 *
	 * @param file the file
	 * @param parentRegId the parent reg id
	 */
	/*protected void addFile(File file, IRI parentRegId){
		if (file.getKind().equals("file")){
			IRI fileId = factory.createIRI(file.getLinks().get("download").toString());
			addStmt(parentRegId, DCTERMS.HAS_PART, fileId);
			addStmt(fileId, RDF.TYPE, Terms.FABIO_COMPUTERFILE);		
			//TODO:giving these temp predicates... need to find terms for these
			//addLiteralStmt(fileId, RDFS.LABEL, file.getName());
			//addLiteralStmt(fileId, SKOS.ALT_LABEL, file.getMaterialized_path());
			//just using one path for now... was doing two but they tend to be repetitive
			addLiteralStmt(fileId, RDFS.LABEL, file.getMaterialized_path());
						
			String created = file.getDate_created();
			String modified = file.getDate_modified();
			addLiteralStmt(fileId, DCTERMS.CREATED, created);
			//only show modified if it is different from created
			if (created!=null && modified !=null && !created.equals(modified)){
				addLiteralStmt(fileId, DCTERMS.MODIFIED, modified);				
			}
			
		} else {
			addFiles(file, parentRegId);
		}
	}*/
	
	
	
	/**
	 * Map category to an IRI.
	 *
	 * @param category the category
	 * @return the iri
	 */
	protected IRI mapCategoryToIri(Category category) {
		if (category!=null){
			String cat = category.value();
			cat = cat.replace(" ", "-");
			String firstletter = cat.substring(0,1);
			firstletter = firstletter.toUpperCase();
			cat = firstletter + cat.substring(1);
			return factory.createIRI(OSF_TERMS_PREFIX + cat);			
		} else {
			return null;
		}
	}

	
}
