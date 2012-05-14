package edu.indiana.d2i.htrc;

import gov.loc.repository.pairtree.Pairtree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

public class HTRCDataAPIClient {
	private final int BUFFER = 2048;
//	private String apiEPR = "http://smoketree.cs.indiana.edu:25443/data-api";
	private String apiEPR = "http://129-79-49-119.dhcp-bl.indiana.edu:25443/data-api";
	private String delimitor = "|";
	private final String VOLUMEURLPREFIX = "/volumes?volumeIDs=";
	private final String PAGEURLPREFIX = "/pages?pageIDs=";
	private static Pairtree pairtree = new Pairtree();   

	// authentication 
	private boolean useAuth = false;
	private String clientID, clientSecrete, tokenLocation;
	private boolean selfsigned = true;
	private static String token = null;
	
	public void setSelfsigned(boolean selfsigned) {
		this.selfsigned = selfsigned;
	}


	public void setTokenLocation(String tokenLocation) {
		this.tokenLocation = tokenLocation;
	}


	public void setClientSecrete(String clientSecrete) {
		this.clientSecrete = clientSecrete;
	}


	public void setClientID(String clientID) {
		this.clientID = clientID;
	}
	
	public void setToken(String token) {
		this.token = token;
	}

	public void useAuthentication(boolean useAuth) {
		this.useAuth = useAuth;
	}
	
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

	private VolumeTree decomposeZipStream(ZipInputStream zipinput)
			throws IOException {
		VolumeTree root = new VolumeTree("root");

		ZipEntry entry;
		while ((entry = zipinput.getNextEntry()) != null) {
			String name = entry.getName();
			if (entry.isDirectory()) {
				VolumeTree node = new VolumeTree(name.split("/")[0]);
				root.children.add(node);
			} else {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				int count;
				byte data[] = new byte[BUFFER];
				while ((count = zipinput.read(data, 0, BUFFER)) != -1) {
					out.write(data, 0, count);
				}

				String[] names = name.split("/");
				if (names.length > 1) {
					// assume parent is always the last child of root!
					VolumeTree parent = root.children
							.get(root.children.size() - 1);
					VolumeTree child = new VolumeTree(names[1], out);
					parent.children.add(child);
				} else {
					VolumeTree node = new VolumeTree(names[0], out);
					root.children.add(node);
				}
			}
		}

		return root;
	}

	/**
	 * The structure of the data in zip stream.
	 *
	 */
	class VolumeTree {
		public String name;
		public ByteArrayOutputStream value = null;
		public List<VolumeTree> children = new ArrayList<VolumeTree>();
		public int currentChild = 0; // for travel

		public VolumeTree(String name) {
			this.name = name;
		}

		public VolumeTree(String name, ByteArrayOutputStream value) {
			this.name = name;
			this.value = value;
		}
	}

	public static String ids2URL(List<String> ids, String delimitor) {
		// unclean the solr id, hardcode
		StringBuilder url = new StringBuilder();
		for (String id : ids)
			url.append(pairtree.uncleanId(id) + delimitor);
		return url.toString();
	}

	/**
	 * It contains all the pages content back from data API server. One way
	 * to use it is to iterate over it. The other ways to use it depend on
	 * different view of it. 
	 */
	public final static class Contents implements Iterable<String> {
		private int index = 0;
		private Stack<VolumeTree> nodes = new Stack<VolumeTree>();
		private VolumeTree root;

		protected Contents(VolumeTree source) {
			VolumeTree tmp = source;
			while (tmp.value == null) {
				nodes.push(tmp);
				tmp = tmp.children.get(0);
			}
			this.root = source;
		}
		
		class ContentsIterator implements Iterator<String> {
			@Override
			public boolean hasNext() {
				return (!nodes.isEmpty()) ? true : false;
			}

			@Override
			public String next() {
				VolumeTree top = nodes.peek();
				String result = top.children.get(index++).value.toString();

				if (index > top.children.size() - 1) {
					index = 0;
					nodes.pop();
					if (nodes.empty())
						return result;

					top = nodes.peek();
					while (top.currentChild >= top.children.size() - 1) {
						nodes.pop();
						if (nodes.empty())
							return result;
						top = nodes.peek();
					}
					// change this
					nodes.push(top.children.get(++top.currentChild));
				}
				return result;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public Iterator<String> iterator() {
			return new ContentsIterator();
		}
		
		/**
		 * A different view of the contents. 
		 */
		public Map<String, List<String>> asVolumeList() {
			Map<String, List<String>> volumes = new HashMap<String, List<String>>();
			
			// hard code for this application
			for (VolumeTree node : root.children) {
				ArrayList<String> pages = new ArrayList<String>();
				for (VolumeTree childnode : node.children) 
					pages.add(childnode.value.toString());
				volumes.put(node.name, pages);
			}
			return volumes;
		}
	}

	public HTRCDataAPIClient(String apiEPR, String delimitor) {
		if (apiEPR.lastIndexOf("/") == apiEPR.length() - 1)
			this.apiEPR = apiEPR.substring(0, apiEPR.length() - 1);
		else
			this.apiEPR = apiEPR;
		this.delimitor = delimitor;
	}

	/**
	 * Accept a query string for the data API and return {@link Contents}
	 * An sample string is as follows
	 * mdp.39015016879747|uc2.ark:/13960/t6sx65p7k|miun.aqj5792.0001.001
	 * 
	 * Another sample string is as follows 
	 * mdp.39015016879747<15,6,12,23>|uc2.ark:/13960/t6sx65p7k<1,34,2>|
	 * miun.aqj5792.0001.001<7,10,24,11,34>
	 * 
	 * @throws Exception  
	 */
	public Contents getContents(String queryStr) throws Exception {
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
        
		HttpsURLConnection httpsURLConnection = (HttpsURLConnection) connection;
		httpsURLConnection.setRequestMethod("GET");

		if (httpsURLConnection.getResponseCode() == 200) {
			InputStream inputStream = httpsURLConnection.getInputStream();
			ZipInputStream zipinput = new ZipInputStream(inputStream);
			VolumeTree root = decomposeZipStream(zipinput);
			inputStream.close();
			Contents content = new Contents(root);
			return content;
		} else {
			System.err.println(httpsURLConnection.getResponseCode());
			return null;
		}
	}
	
	public final static class StreamContents {
		private ZipEntry zipEntry = null;
		private ZipInputStream zipInputStream = null;
		
		private final int BUFFER = 2048;
		
		protected StreamContents(ZipInputStream zipInputStream) throws IOException {
			this.zipInputStream = zipInputStream;
			zipEntry = zipInputStream.getNextEntry();
		}
		
		class PageIterator implements Iterator<String> {
			private boolean pageWithID;
			
			public PageIterator(boolean pageWithID) {
				this.pageWithID = pageWithID;
			}
			
			@Override
			public boolean hasNext() {
				try {
					if (pageWithID) {
						zipEntry = zipInputStream.getNextEntry();
						return (zipEntry == null) ? false : true;
					} else {
						zipEntry = zipInputStream.getNextEntry();
						if (zipEntry == null) return false;
						if (zipEntry.isDirectory()) 
							zipEntry = zipInputStream.getNextEntry();
						return (zipEntry == null) ? false : true;
					}					
				} catch (IOException e) {
					e.printStackTrace();
				}
				return false;
			}

			@Override
			public String next() {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				int count;
				byte data[] = new byte[BUFFER];
				try {
					while ((count = zipInputStream.read(data, 0, BUFFER)) != -1) {
						out.write(data, 0, count);
					}
					return out.toString();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;				
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		}
		
		class PageIterable implements Iterable<String> {
			@Override
			public Iterator<String> iterator() {
				return new PageIterator(false);
			}
		}
		
		public Iterable<String> getPages() {
			return new PageIterable();
		}
		
		public Map<String, List<String>> nextPagesInOneVlume() throws IOException {
			if (zipEntry == null) return null;
			
			Map<String, List<String>> pagesWithID = new HashMap<String, List<String>>();
			String volumeID = zipEntry.getName();
			pagesWithID.put(volumeID, new ArrayList<String>());
			do {
				zipEntry = zipInputStream.getNextEntry();
				if (zipEntry == null) return pagesWithID;
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				int count;
				byte data[] = new byte[BUFFER];
				while ((count = zipInputStream.read(data, 0, BUFFER)) != -1) {
					out.write(data, 0, count);
				}
				pagesWithID.get(volumeID).add(out.toString());
				
			} while (!zipEntry.isDirectory());
			
			return pagesWithID;
		}
	}
	
	public StreamContents getStreamContents(String queryStr) throws Exception {
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
        
		HttpsURLConnection httpsURLConnection = (HttpsURLConnection) connection;
		httpsURLConnection.setRequestMethod("GET");
		httpsURLConnection.setReadTimeout(1000);

		if (httpsURLConnection.getResponseCode() == 200) {
			InputStream inputStream = httpsURLConnection.getInputStream();
			ZipInputStream zipinput = new ZipInputStream(inputStream);
			return new StreamContents(zipinput);
		} else {
			System.err.println(httpsURLConnection.getResponseCode());
			return null;
		}
	}
}