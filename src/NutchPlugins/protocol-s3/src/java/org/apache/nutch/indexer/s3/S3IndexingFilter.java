package org.apache.nutch.indexer.s3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.s3.S3Response;
import org.apache.hadoop.io.BooleanWritable;


public class S3IndexingFilter implements IndexingFilter {

	private static final String AVAILABLE_KEY = "available_in_s3";
    private Configuration conf;

    @Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException {	
		
		BooleanWritable boolWrit = (BooleanWritable) datum.getMetaData().get(S3Response.AVAILABLE_KEY);
		if(boolWrit.get()){
			doc.add(AVAILABLE_KEY,true);
		}else{
			doc.add(AVAILABLE_KEY,false);
		}
		return doc;
	}

    @Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}
}