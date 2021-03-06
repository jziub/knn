package edu.indiana.d2i.htrc.io.dataapi;

import edu.indiana.d2i.htrc.HTRCConstants;
import gov.loc.repository.pairtree.Pairtree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.amber.oauth2.client.OAuthClient;
import org.apache.amber.oauth2.client.URLConnectionClient;
import org.apache.amber.oauth2.client.request.OAuthClientRequest;
import org.apache.amber.oauth2.client.request.OAuthClientRequest.TokenRequestBuilder;
import org.apache.amber.oauth2.client.response.OAuthAccessTokenResponse;
import org.apache.amber.oauth2.common.message.types.GrantType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HTRCDataAPIClient {
	private final int BUFFER = 2048;
//	private String apiEPR = "129-79-49-119.dhcp-bl.indiana.edu:25443/data-api";
	private String apiEPR = HTRCConstants.DATA_API_DEFAULT_URL_PREFIX + HTRCConstants.DATA_API_DEFAULT_URL;
	private String delimitor = "|";
	private final String URLSUFFIX = "https://";
	private final String VOLUMEURLPREFIX = "/data-api/volumes?volumeIDs=";
	private final String PAGEURLPREFIX = "/data-api/pages?pageIDs=";
	private static Pairtree pairtree = new Pairtree();   

	// authentication 
	private boolean useAuth = false;
	private String clientID, clientSecrete, tokenLocation;
	private boolean selfsigned = true;
	private static String token = null;
	
	private HttpsURLConnection httpsURLConnection = null;
	
	private static final Log logger = LogFactory.getLog(HTRCDataAPIClient.class);
	
	// ssl stuffs
	SSLContext sslContext = null;
	private void initSSL() throws Exception {
		if (selfsigned) {
			TrustManager[] trustAllCerts = new TrustManager[] {
	                new X509TrustManager() {
	                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
	                        return null;
	                    }
	                
	                    public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType){}
	                
	                    public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType){}
	                    
	                    public boolean isServerTrusted(java.security.cert.X509Certificate[] certs) { return true; }
	                    public boolean isClientTrusted(java.security.cert.X509Certificate[] certs) { return true; }
	                }
	        };
	        
	        SSLContext sslContext = SSLContext.getInstance("SSL");
	        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
	        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
//	        System.out.println("token: " + response.getAccessToken());
		}
		
		TokenRequestBuilder tokenRequestBuilder = OAuthClientRequest.tokenLocation(tokenLocation);
        tokenRequestBuilder.setGrantType(GrantType.CLIENT_CREDENTIALS);
        tokenRequestBuilder.setClientId(clientID);
        tokenRequestBuilder.setClientSecret(clientSecrete);
        
        OAuthClientRequest clientRequest = tokenRequestBuilder.buildQueryMessage();
        OAuthClient client = new OAuthClient(new URLConnectionClient());
        OAuthAccessTokenResponse response = client.accessToken(clientRequest);     
        token = response.getAccessToken();
	}	
	
	class ID2ContentIterator implements Iterator<Entry<String, String>> {
		private ZipInputStream zipInputStream = null;
		private ZipEntry zipEntry = null;
		
		public ID2ContentIterator(ZipInputStream zipinput) {
			this.zipInputStream = zipinput;
			try {
				zipEntry = zipInputStream.getNextEntry();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public boolean hasNext() {
			return (zipEntry != null) ? true: false;
		}

		@Override
		public Entry<String, String> next() {
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				
				// zip entry has clean id, transfer to unclean id, data API use unclean id
				String cleanid = zipEntry.getName();
				cleanid = cleanid.substring(0, cleanid.length()-1);
				String volumeID = pairtree.uncleanId(cleanid);
				do {
					zipEntry = zipInputStream.getNextEntry();
					if (zipEntry == null){
						Map.Entry<String,String> entry =
							    new AbstractMap.SimpleEntry<String, String>(volumeID, out.toString());
						return entry;
					}
					
					int count;
					byte data[] = new byte[BUFFER];
					while ((count = zipInputStream.read(data, 0, BUFFER)) != -1) {
						out.write(data, 0, count);
					}				
				} while (!zipEntry.isDirectory());
				
				Map.Entry<String,String> entry =
					    new AbstractMap.SimpleEntry<String, String>(volumeID, out.toString());
				return entry;
			} catch (IOException e) {
				System.err.println(e);
				return null;
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	class ID2ContentEntry implements Iterable<Entry<String, String>> {
		private ID2ContentIterator iterator = null;
		
		public ID2ContentEntry(ZipInputStream zipinput) {
			this.iterator = new ID2ContentIterator(zipinput);
		}
		
		@Override
		public Iterator<Entry<String, String>> iterator() {
			return iterator;
		}
	}
	
	public void close() {
		if (httpsURLConnection != null) {
			httpsURLConnection.disconnect(); 
			httpsURLConnection = null;
		}
	}
	
	public Iterable<Entry<String, String>> getID2Content(String queryStr) throws Exception {
		if (useAuth)
			if (token == null) initSSL();
        
		String path = null;
		if (queryStr.contains("<"))
			path = apiEPR + PAGEURLPREFIX + queryStr;
		else
			path = apiEPR + VOLUMEURLPREFIX + queryStr;
		
		URL url = new URL(path);        
        URLConnection connection = url.openConnection();
        connection.addRequestProperty("Authorization", "Bearer " + token);
      
        httpsURLConnection = (HttpsURLConnection) connection;
		httpsURLConnection.setRequestMethod("GET");
		httpsURLConnection.setReadTimeout(0); // infinite time out

		if (httpsURLConnection.getResponseCode() == 200) {
			InputStream inputStream = httpsURLConnection.getInputStream();
			ZipInputStream zipinput = new ZipInputStream(inputStream);
			return new ID2ContentEntry(zipinput);
		} else {
			logger.fatal(queryStr + " leads to fatal, rejected code " + httpsURLConnection.getResponseCode());
//			throw new RuntimeException("Https connection is rejected, code " + httpsURLConnection.getResponseCode());
			return null;
		}
	}
	
	private HTRCDataAPIClient(String apiEPR, String delimitor) {
		String url = apiEPR;
		if (!url.contains(URLSUFFIX))
			url = URLSUFFIX + url;   
		if (url.lastIndexOf("/") == url.length() - 1)
			this.apiEPR = url.substring(0, url.length() - 1);
		else
			this.apiEPR = url;
		this.delimitor = delimitor;
	}
	
	public static class Builder {
		private String apiEPR = HTRCConstants.DATA_API_DEFAULT_URL_PREFIX + HTRCConstants.DATA_API_DEFAULT_URL;
		private String delimitor = "|";
		
		private boolean useAuth = false;
		private String clientID, clientSecrete, tokenLocation;
		private boolean selfsigned = true;
		private String token = null;
		
		public Builder(String apiEPR, String delimitor) {
			this.apiEPR = apiEPR;
			this.delimitor = delimitor;
		}
		
		public Builder selfsigned(boolean selfsigned) {
			this.selfsigned = selfsigned;
			return this;
		}


		public Builder tokenLocation(String tokenLocation) {
			this.tokenLocation = tokenLocation;
			return this;
		}


		public Builder clientSecrete(String clientSecrete) {
			this.clientSecrete = clientSecrete;
			return this;
		}


		public Builder clientID(String clientID) {
			this.clientID = clientID;
			return this;
		}
		
		public Builder token(String token) {
			this.token = token;
			return this;
		}

		public Builder authentication(boolean useAuth) {
			this.useAuth = useAuth;
			return this;
		}
		
		public HTRCDataAPIClient build() {
			HTRCDataAPIClient client = new HTRCDataAPIClient(apiEPR, delimitor);
			client.useAuth = useAuth;
			client.selfsigned = selfsigned;
			client.clientID = clientID;
			client.clientSecrete = clientSecrete;
			client.tokenLocation = tokenLocation;
			client.token = token;
			return client;
		}
	}
}