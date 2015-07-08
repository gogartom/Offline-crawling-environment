package org.apache.nutch.protocol.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.apache.commons.codec.binary.Base64;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HBaseRepository {

	public static ArcFile getArcFileForUrl(String origURLString)
			throws IOException {

		// -- GET Key and Hbase URL
		String key = KeyGenerator.getKeyForUrl(origURLString);
		String encodedKey = URLEncoder.encode(key, "UTF-8");
		URL hbaseUrl = new URL("... path to you index database ..."
				+ encodedKey);

		// -- Connect
		HttpURLConnection conn;
		conn = (HttpURLConnection) hbaseUrl.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");

		int response = conn.getResponseCode();

		// -- ArcFile does not exist		
		if(response == 404){
			return null;
		}
		//-- different problem
		else if (response != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ conn.getResponseCode());
		}
		// -- ArcFile exists, get reponse
		System.out.println("ArcFile exist!");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(
				(conn.getInputStream())));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
			sb.append("\n");
		}

		return parseJson(sb.toString());
	}

	private static ArcFile parseJson(String jsonString)
			throws UnsupportedEncodingException {
		
		//-- Parse json
		JsonParser parser = new JsonParser();
		JsonObject obj = (JsonObject) parser.parse(jsonString);

		//-- Get First row
		JsonObject row = obj.getAsJsonArray("Row").get(0).getAsJsonObject();

		//-- Get key
		String key = decode(row.get("key").getAsString());

		//-- Get cells
		ArcFile arcFile = new ArcFile();
		for (JsonElement cellElement : row.getAsJsonArray("Cell")) {
			JsonObject cell = cellElement.getAsJsonObject();
			String column = decode(cell.get("column").getAsString());

			//segment
			if (column.equals("loc:segment")) {
				arcFile.setSegment(decode(cell.get("$").getAsString()));
			}
			//file
			else if (column.equals("loc:file")) {
				arcFile.setFile(decode(cell.get("$").getAsString()));
			}
			//partition
			else if (column.equals("loc:partition")) {
				arcFile.setPartition(decode(cell.get("$").getAsString()));
			}
			//offset
			else if (column.equals("loc:offset")) {
				arcFile.setOffset(decode(cell.get("$").getAsString()));
			}
			//length
			else if (column.equals("loc:length")) {
				arcFile.setLength(decode(cell.get("$").getAsString()));
			}
		}

		return arcFile;
	}

	private static String decode(String utf8string)
			throws UnsupportedEncodingException {
		byte[] decoded = Base64.decodeBase64(utf8string);
		return new String(decoded, "UTF-8");
	}
}
