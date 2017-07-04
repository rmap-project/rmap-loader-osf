/*******************************************************************************
 * Copyright 2016 Johns Hopkins University
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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util procedures for Transformer.
 *
 * @author khanson
 */
public class Utils {

    /** The log. */
    private static final Logger log = LoggerFactory.getLogger(Utils.class);
	
	/**
	 * Trims a String and if it is empty converts it to nulls .
	 *
	 * @param str the string
	 * @return the string
	 */
	public static String setEmptyToNull(String str){
		if (str!=null){
			str = str.trim();
			if (str.length()==0){
				str=null;
			}
		}	
		return str;
	}

	/**
	 * Converts a string to a URI.
	 *
	 * @param str the str
	 * @return the string as a URI
	 */
	public static URI convertStringToUri(String str) {
		return convertStringToUri(str, "");
	}
	
	/**
	 * Converts a string to a URI, adds prefix if provided and not already present.
	 *
	 * @param str the str
	 * @param prefix the prefix
	 * @return the uri
	 */
	public static URI convertStringToUri(String str, String prefix) {
		if (prefix==null){prefix="";}
		prefix = prefix.trim();
		
		str = setEmptyToNull(str);
		if (str==null){return null;}
		
		if (prefix.length()>0 && !str.startsWith(prefix)) {
			str = prefix + str;
		}
		
		try {
			return new URI(str);
		} catch (URISyntaxException e) {
			log.error("String '" + str + "' is not a valid URI.");
			throw new RuntimeException("String '" + str + "' is not a valid URI.", e);
		}
	}
	
	
	/**
	 * Converts a List of strings to a list of URIs.
	 *
	 * @param strList the str list
	 * @return the URI list
	 */
	public static List<URI> convertStringListToUris(List<String> strList){
		return convertStringListToUris(strList, "");
	}
	
	
	/**
	 * Converts a list of strings to a list of URIs, adds a prefix to each string where that prefix 
	 * is not already there.
	 *
	 * @param strList the str list
	 * @param prefix the prefix
	 * @return the URI list
	 */
	public static List<URI> convertStringListToUris(List<String> strList, String prefix){
		if (prefix==null){prefix="";}
		if (strList==null){return null;}
		
		List<URI> uriList = new ArrayList<URI>();
		for (String str : strList){
			URI uri = convertStringToUri(str, prefix);
			if (uri!=null){
				uriList.add(uri);
			}
		}
		return uriList;
	}
	
	
	
}
