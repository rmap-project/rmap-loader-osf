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


import java.net.URI;
import java.util.Date;
import java.util.List;

import org.openrdf.model.BNode;
import org.openrdf.model.IRI;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;

/**
 * Abstract class for DiSCO Builders. Each data source will implement a custom DiSCO builder
 * for that specific data source.
 * @author khanson
 */
public abstract class DiscoTransformer {

	/** The model  (list of triples that form the DiSCO Graph). */
	protected Model model;	
	
	/** The temporary DiSCO ID. */
	protected BNode discoId;
	
	/** The DiSCO creator. */
	protected String discoCreator=null;
	
	/** The DiSCO description. */
	protected String discoDescription=null;
	
	/** The value factory for generating triplestore objects. */
	protected static ValueFactory factory = SimpleValueFactory.getInstance();
	
	/**
	 * Instantiates a new DiSCO builder.
	 */
	protected DiscoTransformer() {
		super();
		this.model = new LinkedHashModel();		
		discoId = factory.createBNode(); 
	}

	/**
	 * Initiates DiSCO builder.
	 *
	 * @param discoCreator the DiSCO creator
	 * @param discoDescription the DiSCO description
	 */
	protected DiscoTransformer(String discoCreator, String discoDescription){
		this();
		this.discoCreator = Utils.setEmptyToNull(discoCreator);	
		this.discoDescription = Utils.setEmptyToNull(discoDescription);		
	}
		
	/**
	 * Initiates DiSCO builder.
	 *
	 * @param discoCreator the DiSCO creator
	 * @param discoDescription the DiSCO description
	 */
	public DiscoTransformer(URI discoCreator, String discoDescription){
		this();
		if (discoCreator!=null) {
			this.discoCreator = discoCreator.toString();
			this.discoCreator = Utils.setEmptyToNull(this.discoCreator);	
		}
		else {
			this.discoCreator = null;
		}	
		this.discoDescription = Utils.setEmptyToNull(discoDescription);	
	}	

	/**
	 * Gets the DiSCO model (list of triples that form the DiSCO Graph)
	 *
	 * @return the model
	 */
	public abstract Model getModel();
	
	/**
	 * Sets the record.
	 *
	 * @param record the new record
	 */
	//public abstract void setRecord(Object record);

	/**
	 * If all values are not null add a statement to the model .
	 *
	 * @param subject the subject
	 * @param predicate the predicate
	 * @param object the object
	 */
	protected void addStmt(Resource subject, IRI predicate, Value object){
		if (subject!=null && predicate!=null && object!=null){
			Statement stmt = factory.createStatement(subject, predicate, object);
			model.add(stmt);
		}
	}	
	
	/**
	 * If all values are not null add a statement to the model with an IRI as the object.
	 *
	 * @param subject the subject
	 * @param predicate the predicate
	 * @param object the object
	 */
	protected void addIriStmt(Resource subject, IRI predicate, String object){
		if (object!=null && object.length()>0){
			addStmt(subject, predicate, factory.createIRI(object));
		}
	}
	
	/**
	 * If all values are not null add a statement to the model with a Literal as the object.
	 *
	 * @param subject the subject
	 * @param predicate the predicate
	 * @param object the object
	 */
	protected void addLiteralStmt(Resource subject, IRI predicate, String object) {
		if (object!=null && object.length()>0){
			try {
				addStmt(subject, predicate, factory.createLiteral(object));
			} catch (Exception e){
				throw new RuntimeException("Could not generate Literal statement", e);
			}
		}		
	}

	/**
	 * If all values are not null add a statement to the model with a Date Literal as the object.
	 *
	 * @param subject the subject
	 * @param predicate the predicate
	 * @param object the object
	 */
	protected void addLiteralStmt(Resource subject, IRI predicate, Date object){
		if (object!=null){
			addStmt(subject, predicate, factory.createLiteral(object));
		}		
	}

	/**
	 * If all values are not null add a statement to the model with an Integer Literal as the object.
	 *
	 * @param subject the subject
	 * @param predicate the predicate
	 * @param object the object
	 */
	protected void addLiteralStmt(Resource subject, IRI predicate, Integer object){
		if (object!=null){
			addStmt(subject, predicate, factory.createLiteral(object));
		}		
	}
	
	/**
	 * Creates a statement using an object that is either a URI or a Literal - tries to create as URI first, if it fails
	 * it create it as a Literal. Returns null if subject and object have same value.
	 *
	 * @param subject the subject
	 * @param predicate the predicate
	 * @param object the object
	 * @return the statement
	 */
	protected Statement addIRIorLiteralStmt(IRI subject, IRI predicate, String object) {
		Statement stmt = null;
		if (!subject.toString().equals(object.toString())){
			try {
				URI uri = new URI (object);
				addIriStmt(subject, predicate, uri.toString());			
			}catch (Exception e){
				addLiteralStmt(subject, predicate, object);			
			}
		}
		return stmt;
	}
	
	
	/**
	 * Looks at list of identifiers. If there are values in the list, it takes the first one and
	 * generates an IRI to use as the primary identifier in the RDF. Otherwise returns a blank node.
	 *
	 * @param uriList the URI list
	 * @return the first IRI or a BNode
	 */
	protected static Resource getFirstIriOrBNode(List<URI> uriList) {
		Resource val = null;
		if (uriList==null||uriList.size()==0){
			val = factory.createBNode();
		}
		else {
			URI firstUri = uriList.get(0);
			val = getIriOrBNode(firstUri);
		}		
		return val;
	}
		

	/**
	 *  
	 * Looks at URI value provided. If it's null it passes back a blank node, 
	 * if it has a value it passes it back as IRI
	 *
	 * @param uriVal the URI
	 * @return the IRI or BNode
	 */
	protected static Resource getIriOrBNode(URI uriVal) {
		Resource val = null;
		if (uriVal==null){
			val = factory.createBNode();
		}
		else {
			val = factory.createIRI(uriVal.toString());				
		}		
		return val;
	}

	/**
	 * Starts building DiSCO model by generating header statements.
	 */
	protected void addDiscoHeader(){
		//Generate DiSCO header
		addStmt(discoId, RDF.TYPE, Terms.RMAP_DISCO);
		addIriStmt(discoId, DCTERMS.CREATOR, discoCreator);
		addLiteralStmt(discoId, DCTERMS.DESCRIPTION, discoDescription);
	}

}