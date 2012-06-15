/*
#
# Copyright 2012 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: knn
# File:  SolrClient.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.index.solr;

import edu.indiana.d2i.htrc.HTRCConstants;
import gov.loc.repository.pairtree.Pairtree;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

public class SolrClient {
	
	private DefaultHttpClient httpClient;
	private XMLInputFactory factory = XMLInputFactory.newInstance();
	
	private String mainURL = "http://chinkapin.pti.indiana.edu:9994/solr/";
	private final String PREFIX = "TermVector/?prefix=*&ngram=false&offset=false&volumeID=";
	private final String VOLUME_OCR = "ocr";
	private final String VOLUME_ID = "uniqueKey";
	
	private final Pairtree pairtree = new Pairtree();
	
	private Dictionary dictionary = null;
	
	private void initFilters(Configuration conf) throws IOException {
		dictionary = new Dictionary(conf.get(HTRCConstants.DICTIONARY_PATH));
	}
	
	private String generateURL(String id) throws UnsupportedEncodingException {
//		String cleanId = pairtree.cleanId(id);
//		cleanId = cleanId.replace(",", ".");
		
		String cleanId = id;
		
//		System.out.println(cleanId);
		
		cleanId = URLEncoder.encode(cleanId, "UTF-8");
//		cleanId = URLEncoder.encode(cleanId);
		return mainURL + PREFIX + cleanId;
	}
	
	private Vector createVector(XMLStreamReader parser) throws XMLStreamException {
		Vector vector = new RandomAccessSparseVector(dictionary.size());
		while (parser.hasNext()) {			
			int event = parser.next();
			if (event == XMLStreamConstants.START_ELEMENT) {
				String attributeValue = parser.getAttributeValue(null, "name");
				if (attributeValue != null) {
					if (dictionary.containsKey(attributeValue)) {
						parser.next();
						int tf = Integer.valueOf(parser.getElementText());
						vector.setQuick(dictionary.get(attributeValue), tf);
					}
				}
			}
		}
		return vector;
	}
	
	private NamedVector parseOneVolume(InputStream content) throws XMLStreamException, IOException {
//		java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(content));
//		String line = "";
//		while ((line = br.readLine()) != null) {
//			System.out.println(line);
//		}
//		br.close();
		
		String volumeID = null;
		Vector vector = null;
		XMLStreamReader parser = factory.createXMLStreamReader(content);
		while (parser.hasNext()) {
			int event = parser.next();
			if (event == XMLStreamConstants.START_ELEMENT) {
				String attributeValue = parser.getAttributeValue(null,
						"name");
				if (attributeValue != null) {
					if (attributeValue.equals(VOLUME_ID)) {
						volumeID = parser.getElementText();
						volumeID = pairtree.uncleanId(volumeID);
					} else if (attributeValue.equals(VOLUME_OCR)) {
						vector = createVector(parser);
					}				
				}
			}
		}
		
		NamedVector tv = new NamedVector(vector, volumeID);
		return tv;
	}
	
	public SolrClient(Configuration conf) throws IOException {
		httpClient = new DefaultHttpClient();
		this.mainURL = conf.get(HTRCConstants.SOLR_TV_URL, "http://chinkapin.pti.indiana.edu:9994/solr/");
		initFilters(conf);
	}
	
	public Iterable<NamedVector> getTermVectors(String[] ids) {
		List<NamedVector> res = new ArrayList<NamedVector>();
		
		try {
			for (int i = 0; i < ids.length; i++) {
				String url = generateURL(ids[i]);
				
//				System.out.println(url);
				
				HttpGet getRequest = new HttpGet(url);
				HttpResponse response = httpClient.execute(getRequest);
				InputStream content = response.getEntity().getContent();
				NamedVector tv = parseOneVolume(content);
				res.add(tv);
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
		
		return res;
	}
}
