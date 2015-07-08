package cz.cvut.crawling.relevancy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

public class Main {

	public static void main(String[] args) throws Exception {
		String docs_folder = "... path to your folder";
		String generic_df_file = "generic_df.txt";
		
		// Load documents
		List<String> documents = loadDocuments(docs_folder);

		// Load words idf
		List<String> genericDf = loadGenericDfFile(generic_df_file);
		
		
		//--- Learning part
		TopicSimilarityMeter similarity = new TopicSimilarityMeter(documents, genericDf);
		similarity.saveTopic("hospital_topic.txt");
	}

	/***
	 * Loads generic df file
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	private static List<String> loadGenericDfFile(String file) throws IOException{
		List<String> lines = new ArrayList<String>();
		
		BufferedReader in = new BufferedReader(new InputStreamReader(
				new FileInputStream(file), "UTF-8"));
		try {
			String str;
			while ((str = in.readLine()) != null) {
				lines.add(str);
			}
		} finally {
			in.close();
		}

		return lines;
	}

	/***
	 * Loads documents from folder
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	private static List<String> loadDocuments(String path) throws Exception {
		File folder = new File(path);
		ArrayList<String> documents = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			System.out.println(fileEntry.getName());
			if (fileEntry.isFile() && !fileEntry.isHidden()) {
				String content = getTextFromHtml(fileEntry);
				documents.add(content);
			}
		}
		return documents;
	}

	/***
	 * Removes html comments and tags
	 * 
	 * @param f
	 * @return
	 * @throws Exception
	 */
	private static String getTextFromHtml(File f) throws Exception {
		InputStream is = null;
		try {
			is = new FileInputStream(f);
			ContentHandler contenthandler = new BodyContentHandler();
			Metadata metadata = new Metadata();
			Parser parser = new AutoDetectParser();
			parser.parse(is, contenthandler, metadata, new ParseContext());
			return contenthandler.toString();

		} catch (Exception e) {
			throw e;
		} finally {
			if (is != null)
				is.close();
		}
	}
}
