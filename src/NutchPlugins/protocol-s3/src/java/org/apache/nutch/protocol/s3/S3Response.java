/**
 * Adopted from org.apache.nutch.protocol.http
 * 
 */

package org.apache.nutch.protocol.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.URL;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.protocol.http.api.HttpException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.s3.PageFetcher;
import org.apache.nutch.protocol.s3.S3;



/** An HTTP response. */
public class S3Response implements Response {
	
	public static final Text AVAILABLE_KEY = new Text("available_in_s3");

	private Configuration conf;
	private HttpBase http;
	private URL url;
	private String orig;
	private String base;
	private byte[] content;
	private int code;
	private Metadata headers = new SpellCheckedMetadata();

	protected enum Scheme {
		HTTP, HTTPS,
	}

	/**
	 * Default public constructor.
	 * 
	 * @param http
	 * @param url
	 * @param datum
	 * @throws ProtocolException
	 * @throws IOException
	 */
	public S3Response(HttpBase http, URL url, CrawlDatum datum)
			throws ProtocolException, IOException {

		this.http = http;
		this.url = url;
		this.orig = url.toString();
		this.base = url.toString();

		if (!"http".equals(url.getProtocol())
				&& !"https".equals(url.getProtocol())) {
			throw new HttpException("Unknown scheme (not http/https) for url:"
					+ url);
		}

		if (S3.LOG.isTraceEnabled()) {
			S3.LOG.trace("fetching " + url);
		}


		//#################################################
		//########### GET ARC FILE CONTENT  ###############
		//#################################################
	
		this.conf = http.getConf();
		String arcFileContent = PageFetcher.fetchPage(url.toString());

		//#################################################
		//########### URL IS IN THE DATASET ###############
		//#################################################
		if (arcFileContent != null) {
			
			// --- INPUT STREAM IMPLEMENTATION
			// We convert string back to byte input stream,
			// since original implementation use it

			ByteArrayInputStream bis = new ByteArrayInputStream(
					arcFileContent.getBytes());
			PushbackInputStream in = new PushbackInputStream(bis);
			StringBuffer line = new StringBuffer();

			// --- skip the first line - it is not header
			readLine(in, line, false);

			// --- We read headers as in http protocol
			// --- Because there may be headers over multiple lines etc.
			boolean haveSeenNonContinueStatus = false;
			while (!haveSeenNonContinueStatus) {
				// parse status code line
				this.code = parseStatusLine(in, line);
				// parse headers
				parseHeaders(in, line);
				haveSeenNonContinueStatus = code != 100; // 100 is "Continue"
			}

			// --- we do not know the real content-length, because originally it
			// could be compressed
			headers.remove("Content-Length");

			// --- read content, without using content-length
			readPlainContent(in);
			
			// it is not compressed from commoncrawl
			headers.remove("Content-Encoding");

			// set the real length
			headers.set("Content-Length", String.valueOf(content.length));
			
			//set available
			datum.getMetaData().put(AVAILABLE_KEY, new BooleanWritable(true));
		}

		//#################################################
		//########### URL NOT in THE DATASET ##############
		//#################################################
		else {

			//-- this is for debuging purposes
			// this.code = 200;	
			// datum.getMetaData().put(AVAILABLE_KEY, new BooleanWritable(false));
			// content = (url+" NOT_AVAILABLE_IN_S3").getBytes("UTF-8");
			// headers.set("Content-Type:", "text/html");
			// headers.set("Content-Length", String.valueOf(content.length));		

			this.code = 404;	
		}
	}

	/*
	 * ------------------------- * <implementation:Response> *
	 * -------------------------
	 */

	public URL getUrl() {
		return url;
	}

	public int getCode() {
		return code;
	}

	public String getHeader(String name) {
		return headers.get(name);
	}

	public Metadata getHeaders() {
		return headers;
	}

	public byte[] getContent() {
		return content;
	}

	/*
	 * ------------------------- * <implementation:Response> *
	 * -------------------------
	 */

	private void readPlainContent(InputStream in) throws HttpException,
			IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream(S3.BUFFER_SIZE);
		byte[] bytes = new byte[S3.BUFFER_SIZE];
		// int length = 0; // read content
		for (int i = in.read(bytes); i != -1; i = in.read(bytes)) {

			out.write(bytes, 0, i);
			// length += i;
		}
		content = out.toByteArray();
	}

	private int parseStatusLine(PushbackInputStream in, StringBuffer line)
			throws IOException, HttpException {
		readLine(in, line, false);

		int codeStart = line.indexOf(" ");
		int codeEnd = line.indexOf(" ", codeStart + 1);

		// handle lines with no plaintext result code, ie:
		// "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
		if (codeEnd == -1)
			codeEnd = line.length();

		int code;
		try {
			code = Integer.parseInt(line.substring(codeStart + 1, codeEnd));
		} catch (NumberFormatException e) {
			throw new HttpException("bad status line '" + line + "': "
					+ e.getMessage(), e);
		}

		return code;
	}

	private void processHeaderLine(StringBuffer line) throws IOException,
			HttpException {

		int colonIndex = line.indexOf(":"); // key is up to colon
		if (colonIndex == -1) {
			int i;
			for (i = 0; i < line.length(); i++)
				if (!Character.isWhitespace(line.charAt(i)))
					break;
			if (i == line.length())
				return;
			throw new HttpException("No colon in header:" + line);
		}
		String key = line.substring(0, colonIndex);

		int valueStart = colonIndex + 1; // skip whitespace
		while (valueStart < line.length()) {
			int c = line.charAt(valueStart);
			if (c != ' ' && c != '\t')
				break;
			valueStart++;
		}
		String value = line.substring(valueStart);
		headers.set(key, value);
	}

	// Adds headers to our headers Metadata
	private void parseHeaders(PushbackInputStream in, StringBuffer line)
			throws IOException, HttpException {

		while (readLine(in, line, true) != 0) {

			// handle HTTP responses with missing blank line after headers
			int pos;
			if (((pos = line.indexOf("<!DOCTYPE")) != -1)
					|| ((pos = line.indexOf("<HTML")) != -1)
					|| ((pos = line.indexOf("<html")) != -1)) {

				in.unread(line.substring(pos).getBytes("UTF-8"));
				line.setLength(pos);

				try {
					// TODO: (CM) We don't know the header names here
					// since we're just handling them generically. It would
					// be nice to provide some sort of mapping function here
					// for the returned header names to the standard metadata
					// names in the ParseData class
					processHeaderLine(line);
				} catch (Exception e) {
					// fixme:
					S3.LOG.warn("Error: ", e);
				}
				return;
			}

			processHeaderLine(line);
		}
	}

	private static int readLine(PushbackInputStream in, StringBuffer line,
			boolean allowContinuedLine) throws IOException {
		line.setLength(0);
		for (int c = in.read(); c != -1; c = in.read()) {
			switch (c) {
			case '\r':
				if (peek(in) == '\n') {
					in.read();
				}
			case '\n':
				if (line.length() > 0) {
					// at EOL -- check for continued line if the current
					// (possibly continued) line wasn't blank
					if (allowContinuedLine)
						switch (peek(in)) {
						case ' ':
						case '\t': // line is continued
							in.read();
							continue;
						}
				}
				return line.length(); // else complete
			default:
				line.append((char) c);
			}
		}
		throw new EOFException();
	}

	private static int peek(PushbackInputStream in) throws IOException {
		int value = in.read();
		in.unread(value);
		return value;
	}

}
