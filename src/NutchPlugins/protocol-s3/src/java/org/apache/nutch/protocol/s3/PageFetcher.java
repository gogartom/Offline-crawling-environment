package org.apache.nutch.protocol.s3;

import java.io.IOException;

	
public class PageFetcher {

	public static String fetchPage(String url) throws IOException{
		 
		ArcFile arcFile = HBaseRepository.getArcFileForUrl(url);

		 //file exists
		 if(arcFile!=null){
			 return arcFile.getContent();			 
		 }
		 // file does not exist
		 else{
			 return null;
		 }
	}
	
	

	// USE: needs connection to hbase !!!
	
	public static void main(String[] args) throws IOException {

		 String origURLString = "http://www.programmableweb.com/";
		 
		 ArcFile arcFile = HBaseRepository.getArcFileForUrl(origURLString);

		 //file exists
		 if(arcFile!=null){
			 String content = arcFile.getContent();			 
			 System.out.println("Content:");
			 System.out.println(content);
		 }
		 // file does not exist
		 else{
			 System.out.println("Does not exist.");
		 }
	}
	
}
