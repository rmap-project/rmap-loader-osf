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
package info.rmapproject.loader.osf.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.joda.time.DateTime;


/**
 * Utilities used in transform
 * @author khanson
 */
public class OSFLoaderUtils {
	
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
	public static HashMap<String, String> readParamsIntoMap(String filters) throws URISyntaxException {
		HashMap<String, String> params = new HashMap<>();
		
		String url = "http://fakeurl.fake";
		if (!filters.startsWith("?")){
			url = url + "?";
		}
		url = url + filters;
		
	    List<NameValuePair> result = URLEncodedUtils.parse(new URI(url), StandardCharsets.UTF_8);

	    for (NameValuePair nvp : result) {
	        params.put(nvp.getName(), nvp.getValue());
	    }

	    return params;
	}

	
	/**
	 * Returns date provided minus one day
	 * @param date
	 * @return
	 */
	public static Date subtractDay(Date date) {
		//default filter to yesterday
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    cal.add(Calendar.DAY_OF_MONTH, -1);
		Date dayBefore = cal.getTime();		
		return dayBefore;
	}
	

	/**
	 * Returns date provided minus one day
	 * @param date
	 * @return
	 */
	public static String previousDayAsString(DateTime date) {
		DateTime dayBefore = date.minusDays(1);
		String sDayBefore = convertToOsfDateParam(dayBefore);
		return sDayBefore;
	}
	
	
	/**
	 * Returns string format of date, suitable for OSF date filter
	 * @param date
	 * @return
	 */
	public static String convertToOsfDateParam(DateTime date) {
		String dateParam = date.toString("yyyy-MM-dd");	
		return dateParam;
	}	
	
	
}
