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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.openrdf.model.Model;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;


/**
 * Utilities used in transform
 * @author khanson
 */
public class TransformUtils {
	
	/** Name of transform properties file. */
	private static final String PROP_FILE = "rmaptransform";
	
	/** DOI HTTP URI prefix. */
	private static final String DOI_PREFERRED_HTTP_PREFIX = "https://doi.org/"; //the preferred format - normalize to this

	/** DOI HTTP URI prefix. */
	private static final String DOI_HTTP_PREFIX = "http://doi.org/"; //Incorrect! Use https
	
	/** DOI non-HTTP URI prefix. */
	private static final String DOI_COLON_PREFIX = "doi:"; //going to normalize to https://doi.org/ address
	
	/** DOI HTTPS URI prefix. */
	private static final String DOI_DX_HTTP_PREFIX = "http://dx.doi.org/"; //going to normalize to https://doi.org/ address
	
	/** DOI HTTPS URI prefix. */
	private static final String DOI_DX_HTTPS_PREFIX = "https://dx.doi.org/"; //going to normalize to https://doi.org/ address
	
	/** DOI first characters. */
	private static final String DOI_VALID_FIRST_CHARS = "10.";
		
	/**
	 * Convenience method for extracting a single property name/value pair from a property file.
	 *
	 * @param propKey property name
	 * @return String containing property value, or null if not found
	 * @throws NullPointerException the null pointer exception
	 * @throws MissingResourceException the missing resource exception
	 */
	public static String getPropertyValue(String propKey) throws NullPointerException, MissingResourceException {
		String propValue = null;
		ResourceBundle resources = ResourceBundle.getBundle(PROP_FILE, Locale.getDefault());
		for (String key:resources.keySet()){
			if (key.equals(propKey)){
				propValue = resources.getString(key);
				break;
			}
		}			
		return propValue;
	}
	
	/**
	 * Extract the last subfolder name from a path.
	 * e.g. for https://api.osf.io/v2/registrations/sdfkj/ sdfkj will be extracted
	 *
	 * @param linkUrl the link url
	 * @return the last subfolder
	 */
	public static String extractLastSubFolder(String linkUrl){
		if (linkUrl!=null && linkUrl.length()>0 && linkUrl.contains("/")){
			if (linkUrl.endsWith("/")){
				linkUrl = linkUrl.substring(0,linkUrl.length()-1);
			}
			String id = linkUrl.substring(linkUrl.lastIndexOf('/') + 1);
			return id;
		} else {
			return null;
		}		
	}
	
	/**
	 * Convert URL params to Map<String,String>.
	 *
	 * @param filters the filters
	 * @param charset the charset
	 * @return the URL params as a Map
	 * @exception URISyntaxException the URI syntax exception
	 */
	public static HashMap<String, String> readParamsIntoMap(String filters, String charset) throws URISyntaxException {
		HashMap<String, String> params = new HashMap<>();
		
		String url = "http://fakeurl.fake";
		if (!filters.startsWith("?")){
			url = url + "?";
		}
		url = url + filters;
		
	    List<NameValuePair> result = URLEncodedUtils.parse(new URI(url), charset);

	    for (NameValuePair nvp : result) {
	        params.put(nvp.getName(), nvp.getValue());
	    }

	    return params;
	}
	

	/**
	 * Converts an issn to a URN format if possible.
	 *
	 * @param issn the issn
	 * @return the formatted issn
	 */
	public static String issnFormatter(String issn){
		if (issn==null){
			return null;
		}
		String newIssn = issn.toUpperCase();
		newIssn = newIssn.replace(" ","-");
		newIssn = newIssn.replace("URN:","");
		newIssn = newIssn.replace("EISSN-","");
		newIssn = newIssn.replace("EISSN:","");
		newIssn = newIssn.replace("ISSN-","");
		newIssn = newIssn.replace("ISSN:","");
		
		String regex1 = "\\d{8}"; //12341234
		String regex2 = "\\d{4}-\\d{4}"; //1234-1234
		String regex3 = "\\d{4}-\\d{3}X"; //1234-123X
		
		if (newIssn.matches(regex1)||newIssn.matches(regex2)
				||newIssn.matches(regex3)){
			return "urn:issn:" + newIssn;
		} else {
			return issn;
		}
		
		
	}
	
	/**
	 * Converts an isbn to a URN format if possible.
	 *
	 * @param isbn the isbn
	 * @return the formatted isbn
	 */
	public static String isbnFormatter(String isbn){
		if (isbn==null){
			return null;
		}
		String newIsbn = isbn.toUpperCase();
		newIsbn = newIsbn.replace(" ","-");
		newIsbn = newIsbn.replace("URN:","");
		newIsbn = newIsbn.replace("ISBN-","");
		newIsbn = newIsbn.replace("ISBN:","");
		
		String regex = "^(?:ISBN(?:-1[03])?:? )?(?=[0-9X]{10}$|(?=(?:[0-9]+[- ]){3})[- 0-9X]{13}$|97[89][0-9]{10}$|(?=(?:[0-9]+[- ]){4})[- 0-9]{17}$)(?:97[89][- ]?)?[0-9]{1,5}[- ]?[0-9]+[- ]?[0-9]+[- ]?[0-9X]$";
		
		if (newIsbn.matches(regex)){
			return "urn:isbn:" + newIsbn;
		} else {
			return isbn;
		}
	}
	
	/**
	 *  
	 * Generates Turtle RDF given a Model.
	 *
	 * @param model the model (set of statements that form the graph)
	 * @return the RDF as an OutputStream
	 */
	public static OutputStream generateTurtleRdf(Model model) {
		OutputStream bOut = new ByteArrayOutputStream();
		try {
			Writer writer = new OutputStreamWriter(bOut, "UTF-8");
			Rio.write(model, writer, RDFFormat.TURTLE);
		} catch (Exception e) {
			throw new RuntimeException("Exception thrown creating RDF from statement list", e);
		}
		return bOut;	
	}
	
	/**
	 * Very basic validation to check if a string looks like a doi. 
	 *
	 * @param doi the doi
	 * @return true, if is doi
	 */
	public static boolean isDoi(String doi) {
		//TODO: make this better, add regex validation?
		doi = doi.toLowerCase(); 
		if (doi!=null 
				&& doi.contains(DOI_VALID_FIRST_CHARS)
				&& !doi.contains(" ") 
				&& !doi.equals(DOI_PREFERRED_HTTP_PREFIX)
				&& !doi.equals(DOI_COLON_PREFIX)
				&& !doi.equals(DOI_DX_HTTP_PREFIX) 
				&& !doi.equals(DOI_DX_HTTPS_PREFIX) 
				&& !doi.equals(DOI_HTTP_PREFIX)
				&& (doi.startsWith(DOI_PREFERRED_HTTP_PREFIX) || doi.startsWith(DOI_HTTP_PREFIX) 
						|| doi.startsWith(DOI_COLON_PREFIX) || doi.startsWith(DOI_DX_HTTP_PREFIX) 
						|| doi.startsWith(DOI_DX_HTTPS_PREFIX) || doi.startsWith(DOI_VALID_FIRST_CHARS))){
			return true;
		} 

		return false;
	}
	
	
	/**
	 * Normalizes doi:10.xxx, https://dx.doi.org/10.xxx and other variations to currently recommended
	 * https://doi.org/10.xxx. Will throw runtime exception if invalid doi provided
	 *
	 * @param doi the doi
	 * @return the normalized DOI
	 */
	public static String normalizeDoi(String doi){
		String normalizedDoi = "";
		if (doi == null){
			return normalizedDoi;
		}
		doi = doi.trim();

		if (!isDoi(doi)) {
			throw new RuntimeException("Invalid DOI provided: " + doi);
		}		
		
		if (doi.startsWith(DOI_VALID_FIRST_CHARS)) {
			doi = DOI_PREFERRED_HTTP_PREFIX + doi;
		} else if (doi.startsWith(DOI_HTTP_PREFIX)){
			doi = doi.replace(DOI_HTTP_PREFIX, DOI_PREFERRED_HTTP_PREFIX);
		} else if (doi.startsWith(DOI_COLON_PREFIX)){
			doi = doi.replace(DOI_COLON_PREFIX, DOI_PREFERRED_HTTP_PREFIX);
		} else if (doi.startsWith(DOI_DX_HTTP_PREFIX)){
			doi = doi.replace(DOI_DX_HTTP_PREFIX, DOI_PREFERRED_HTTP_PREFIX);
		} else if (doi.startsWith(DOI_DX_HTTPS_PREFIX)){
			doi = doi.replace(DOI_DX_HTTPS_PREFIX, DOI_PREFERRED_HTTP_PREFIX);
		}
		return doi;
	}
	
	
	/**
	 * Returns preferred DOI HTTP prefix
	 *
	 * @return the normalized DOI
	 */
	public static String getHttpDoiPrefix() {
		return DOI_PREFERRED_HTTP_PREFIX;
	}

	
	/**
	 * Returns preferred DOI non-HTTP prefix
	 *
	 * @return the normalized DOI
	 */
	public static String getNonHttpDoiPrefix() {
		return DOI_COLON_PREFIX;
	}
	
	
}
