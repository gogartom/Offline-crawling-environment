package cz.cvut.crawling.relevancy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TopicSimilarityMeter {
	
	// ###############################################################
	// ####################### PROPERTIES ############################
	// ###############################################################

	// -- stopwords
	private static final String[] english_stopwords = ("a about above across after afterwards again against all almost alone along already also"
			+ " although always am among amongst amoungst amount amp an and another any anyhow anyone anything anyway anywhere are around as at"
			+ " back be became because become becomes becoming been before beforehand behind being below beside besides between beyond bill both "
			+ "bottom but by call can cannot cant co computer con could couldnt cry de describe detail did do does doesn doing done down due during "
			+ "each eg eight either eleven else elsewhere empty enough etc even ever every everyone everything everywhere except few fifteen fify"
			+ " fill find fire first five for former formerly forty found four from front full further get give go had has hasnt have having he"
			+ " hence her here hereafter hereby herein hereupon hers herself him himself his how however hundred i ie if in inc indeed interest into"
			+ " is it its itself just keep kg km last latter latterly least less ltd made many may me meanwhile might mill mine more moreover "
			+ "most mostly move much must my myself name namely nbsp neither never nevertheless next nine no nobody none noone nor not nothing "
			+ "now nowhere of off often on once one only onto or other others otherwise our ours ourselves out over own part per perhaps please"
			+ " put quite rather rather re really regarding same see seem seemed seeming seems serious several she should show side since sincere "
			+ "six sixty so some somehow someone something sometime sometimes somewhere still such system take ten than that the their theirs them "
			+ "themselves then thence there thereafter thereby therefore therein thereupon these they thick thin third this those though three "
			+ "through throughout thru thus to together too top toward towards twelve twenty two un under unless until up upon us used using various "
			+ "very very via was we well were what whatever when whence whenever where whereafter whereas whereby wherein whereupon wherever whether"
			+ " which while whither who whoever whole whom whose why will with within without would yet you your yours yourself yourselves")
			.split(" ");

	// -- properties

	private UniversalIdf universalIdf;
	private HashMap<String, Double> idf;
	private LinkedHashMap<String, Double> topicVector;
	private static final Set<String> stopwordSet = new HashSet<String>(
			Arrays.asList(english_stopwords));

	// #################################################################
	// ####################### CONSTRUCTORS ############################
	// #################################################################

	public TopicSimilarityMeter(File topicFile) throws IOException {
		loadTopic(topicFile);
	}

	public TopicSimilarityMeter(List<String> documents, List<String> universalDF) {
		// ------- Preprocess documents
		List<String> preprocessedDocuments = new ArrayList<String>();
		for (String document : documents) {
			String preprocessed = preprocessText(document);

			if (!preprocessed.isEmpty()) {
				preprocessedDocuments.add(preprocessed);
			}
		}

		// ------- Prepare IDF object
		this.universalIdf = new UniversalIdf(universalDF);

		// ------- Learn topic
		learnTopic(preprocessedDocuments, 10);
	}

	// ###################################################################
	// ##################### PUBLIC METHODS ##############################
	// ###################################################################

	public double getSimilarity(File file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");
		String s;

		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}

		s = stringBuilder.toString();
		return getSimilarity(s);
	}

	public double getSimilarity(String document) {

		// -- VECTORIZE DOCUMENT
		LinkedHashMap<String, Double> vector = vectorizeDocument(document);
		return cosineSimilarity(vector);
	}

	public void saveTopic(String path) throws Exception {
		// -- check topic and idf
		if (this.topicVector == null || this.idf == null) {
			throw new Exception("Cannot save topic. Needs topic and its idf");
		}

		// -- save to text file
		Writer out = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(path), "UTF-8"));
		try {
			for (Entry<String, Double> entry : this.topicVector.entrySet()) {
				double idf = this.universalIdf.getIdf(entry.getKey());
				out.write(entry.getKey() + "\t" + entry.getValue() + "\t" + idf
						+ "\n");
			}
		} finally {
			out.close();
		}
	}

	// #################################################################
	// ###################### PRIVATE METHODS ##########################
	// #################################################################

	private LinkedHashMap<String, Double> vectorizeDocument(String document) {
		LinkedHashMap<String, Integer> TF = new LinkedHashMap<String, Integer>();

		for (String word : document.split("[\\p{Punct}\\s]+")) { // split by
																	// punctuation
			String processedWord = processWord(word);

			// IF word is processed and is in topic vector
			if (processedWord != null
					&& this.topicVector.containsKey(processedWord)) {
				addToCountMap(TF, processedWord);
			}
		}

		return normalizeVector(getTfidfVector(TF));
	}

	private void learnTopic(List<String> preprocessedDocuments, int minDF) {
		LinkedHashMap<String, Integer> totalTF = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> DF = new LinkedHashMap<String, Integer>();

		// -------- Compute totalTF and DF
		for (String document : preprocessedDocuments) {
			// document words
			Set<String> docWords = new HashSet<String>();

			for (String word : document.split(" ")) {
				// add to totalTF
				addToCountMap(totalTF, word);

				// if we have not seen the word in document
				// add it to DF
				if (!docWords.contains(word)) {
					docWords.add(word);
					addToCountMap(DF, word);
				}
			}
		}

		// -------- Sample DF pruning
		Iterator<Entry<String, Integer>> it = DF.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> entry = it.next();
			if (entry.getValue() < minDF) {
				it.remove();
				totalTF.remove(entry.getKey());
			}
		}

		// ------ Prepare IDF
		this.idf = new HashMap<String, Double>();

		for (String word : totalTF.keySet()) {
			double idf = this.universalIdf.getIdf(word);
			this.idf.put(word, idf);
		}

		// ------- GET TF-IDF from Universal IDF
		LinkedHashMap<String, Double> tfidfVector = getTfidfVector(totalTF);

		// ------- SAVE normalized vector
		this.topicVector = normalizeVector(tfidfVector);
	}

	private double cosineSimilarity(LinkedHashMap<String, Double> vector2) {
		double sum = 0;

		for (Entry<String, Double> vector_value : vector2.entrySet()) {
			String key = vector_value.getKey();
			Double value = vector_value.getValue();
			Double topic_value = this.topicVector.get(key);

			sum += value * topic_value;
		}

		// -- normalize by number of matches
		// double pseudo_normalizer = ((double) vector2.size() / (double)
		// topicVector.size());
		// return sum * pseudo_normalizer;

		return sum;
	}

	private LinkedHashMap<String, Double> getTfidfVector(
			LinkedHashMap<String, Integer> countVector) {

		LinkedHashMap<String, Double> tfidfVector = new LinkedHashMap<String, Double>();

		for (Entry<String, Integer> entry : countVector.entrySet()) {
			String word = entry.getKey();
			double tf = (double) entry.getValue();
			double idf = this.idf.get(word);
			tfidfVector.put(word, tf * idf);
		}

		return tfidfVector;
	}

	private LinkedHashMap<String, Double> normalizeVector(
			LinkedHashMap<String, Double> tfidfVector) {

		// ------- COMPUTE VECTOR LENGTH
		double sum = 0;
		for (Double tf : tfidfVector.values()) {
			sum += Math.pow(tf, 2);
		}
		double length = Math.sqrt(sum);

		// ------- NORMALIZE
		LinkedHashMap<String, Double> normalized = new LinkedHashMap<String, Double>();
		for (Entry<String, Double> entry : tfidfVector.entrySet()) {
			normalized.put(entry.getKey(), (entry.getValue()) / length);
		}

		return normalized;
	}

	private static void addToCountMap(Map<String, Integer> map, String word) {
		Integer count = map.get(word);
		if (count == null)
			count = 0;
		map.put(word, ++count);
	}

	/***
	 * Preprocessing of text document. Removes punction, multiple whitespaces,
	 * splits on nonaplhabetics. Removes numbers and stopwords
	 * 
	 * @param s
	 * @return preprocessed string
	 */
	private static String preprocessText(String s) {
		StringBuilder sb = new StringBuilder();
		for (String word : s.split("[\\p{Punct}\\s]+")) { // split on puctuation
			String processedWord = processWord(word);
			if (processedWord != null) {
				sb.append(processedWord);
				sb.append(" ");
			}
		}
		return sb.toString();
	}

	private static String processWord(String word) {
		word = word.toLowerCase();

		// IF WORD
		// .. does not include a number
		// .. is longer than 2 characters
		// .. is shorter than 25 chracters
		// .. is not a stopword
		if (!word.matches(".*\\d.*") && (word.length() > 2)
				&& (word.length() < 25) && !stopwordSet.contains(word)) {
			return word;
		} else {
			return null;
		}
	}

	private void loadTopic(File file) throws IOException {
		// -- prepare topic vector and idf
		this.topicVector = new LinkedHashMap<String, Double>();
		this.idf = new LinkedHashMap<String, Double>();

		// -- read from file
		BufferedReader in = new BufferedReader(new InputStreamReader(
				new FileInputStream(file), "UTF-8"));
		try {
			String str;
			while ((str = in.readLine()) != null) {
				String[] parts = str.split("\t");
				if (parts.length == 3) {
					this.topicVector.put(parts[0], new Double(parts[1]));
					this.idf.put(parts[0], new Double(parts[2]));
				}
			}
		} finally {
			in.close();
		}
	}

	// #################################################################
	// ###################### HELPER CLASSES ###########################
	// #################################################################

	private class UniversalIdf {
		private double total_doc_count;
		private Map<String, Double> word_df = new HashMap<String, Double>();

		public UniversalIdf(List<String> df_content) {

			for (int i = 0; i < df_content.size(); i++) {
				String line = df_content.get(i);
				String[] parts = line.split("\t");

				// --- first line is a total document count
				if (i == 0 && parts[0].equals("__total_document__count")) {
					this.total_doc_count = Double.valueOf(parts[1]);
				}
				// --- other lines
				else {
					double count = Double.valueOf(parts[1]);
					word_df.put(parts[0], count);
				}
			}
		}

		public double getIdf(String word) {
			Double df = word_df.get(word);

			// if it was not found, it's zero
			if (df == null)
				df = 0d;

			// add 1, so we do not divide by zero
			df++;

			return Math.log(this.total_doc_count / df);
		}
	}

}
