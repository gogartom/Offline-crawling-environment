package org.apache.nutch.protocol.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class ArcFile {

	private static final String bucketName = "aws-publicdatasets";
	private static final String key2012 = "common-crawl/parse-output/segment/";

	private String segment;
	private String file;
	private String partition;
	private String offset;
	private String length;

	public String getContent() throws IOException {

		// -GET offset in the file
		int start = Integer.valueOf(this.offset);
		int end = start + Integer.valueOf(this.length) - 1;

		// -GET keyname
		String key = key2012 + this.segment + "/" + this.file + "_" + partition
				+ ".arc.gz";

		// //////////
		// // S3 ////
		// //////////

		AmazonS3 s3Client = new AmazonS3Client();

		// -CALL S3
		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName,
				key);
		rangeObjectRequest.setRange(start, end); // retrieve 1st 10 bytes.
		S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

		// -READ OUTPUT
		InputStream objectData = objectPortion.getObjectContent();
		GZIPInputStream gzis = new GZIPInputStream(objectData);
		InputStreamReader reader = new InputStreamReader(gzis);
		BufferedReader in = new BufferedReader(reader);

		String readed;
		StringBuilder sb = new StringBuilder();
		while ((readed = in.readLine()) != null) {
			sb.append(readed);
			sb.append("\n");
		}
		objectData.close();

		return sb.toString();
	}

	public String getSegment() {
		return segment;
	}

	public void setSegment(String segment) {
		this.segment = segment;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public String getLength() {
		return length;
	}

	public void setLength(String length) {
		this.length = length;
	}

}
