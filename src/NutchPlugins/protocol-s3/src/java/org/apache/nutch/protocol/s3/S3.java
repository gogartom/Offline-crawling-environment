/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.protocol.s3;

// JDK imports
import java.io.IOException;
import java.net.URL;


// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.util.NutchConfiguration;


public class S3 extends HttpBase {

  public static final Logger LOG = LoggerFactory.getLogger(S3.class);


  /**
   * Public default constructor.
   */
  public S3() {
    super(LOG);
  }

  /**
   * Set the {@link org.apache.hadoop.conf.Configuration}
   * object.
   * @param conf
   */
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  public static void main(String[] args) throws Exception {
    S3 http = new S3();
    http.setConf(NutchConfiguration.create());
    main(http, args);
  }

  protected Response getResponse(URL url, CrawlDatum datum, boolean redirect)
    throws ProtocolException, IOException {
    return new S3Response(this, url, datum);
  }

}