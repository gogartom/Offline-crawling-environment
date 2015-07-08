package org.apache.nutch.protocol.s3;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class KeyGenerator {
	private static 	Set<Integer> mergedPorts = new HashSet<Integer>();
	private static Set<String> mergedProtocols = new HashSet<String>();
	
	static{
		//--- ADDING MERGED PORTS --//
		mergedPorts.add(80);
		mergedPorts.add(443);
		mergedPorts.add(8080);
		
		//--- ADDING MERGED PROTOCOLS --//
		mergedProtocols.add("https");
		mergedProtocols.add("http");
	}
	
	public static String getKeyForUrl(String urlString){
		//-- try parse url
		try {
			
			//parse url
			URL url = new URL(urlString);
			
			//build new string
			StringBuilder sb = new StringBuilder();
			
			//get parts
			int port = url.getPort();
			String host = url.getHost();
			String filepath = url.getFile();
			String protocol = url.getProtocol();
			
			//append reversed host	
			String[] hostParts = host.split("\\.");
			for(int i=hostParts.length-1; i>=0; i--){
				sb.append(hostParts[i]);
				if(i!=0) sb.append(".");
			}
			
			//append filepath
			sb.append(filepath);
			
			//append protocol
			if(!protocol.isEmpty() & !mergedProtocols.contains(protocol)){
				sb.append(" ");
				sb.append(protocol);
			}
						
			//append port
			//only if port is set and is not in merged ports
			if(port!=-1 && !mergedPorts.contains(port)){
				sb.append(" ");
				sb.append(port);
			}
			
			return sb.toString();
		}
		
		//-- if it is not possible 
		catch (MalformedURLException e) {
			//if does not start with http or https
			if(!urlString.startsWith("http://") && !urlString.startsWith("https://")){
				//tell us
				e.printStackTrace();
				
				//try to add http and try it again
				return getKeyForUrl("http://"+urlString);
			}
			//or return original
			else{
				return urlString;
			}	
		}
		
	}
}
