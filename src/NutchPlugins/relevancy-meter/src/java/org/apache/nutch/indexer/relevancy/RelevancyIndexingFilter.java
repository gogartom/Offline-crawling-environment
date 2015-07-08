package org.apache.nutch.indexer.relevancy;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.relevancy.TopicSimilarityMeter;
import org.apache.nutch.parse.Parse;

public class RelevancyIndexingFilter implements IndexingFilter {

	private static final int MINIMUM_DOC_LENGTH = 5; // at least five words
	private static final String KEY = "topic_relevancy";
	private Configuration conf;
	private TopicSimilarityMeter topicSimilarity;

	@Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException {

		// -- COMPUTE ONLY IF:
		// ------- WE HAVE TOPIC_SIMILARITY OBJECT
		// ------- AND IT CONTAINS ENOUGH WORDS
		if (this.topicSimilarity != null
				&& parse.getText().split(" ").length >= MINIMUM_DOC_LENGTH) {
			Double similarity = this.topicSimilarity.getSimilarity(parse
					.getText());
			doc.add(KEY, similarity);
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

		URL url = conf.getResource("topic.txt");

		if (url != null) {
			try {
				File file = new File(url.getFile());
				this.topicSimilarity = new TopicSimilarityMeter(file);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}