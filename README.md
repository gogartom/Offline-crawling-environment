# Offline-crawling-environment

This repository stores complementary materials to paper "Offline Environment for Focused Crawler Evaluation".
In this work we suggest to use precrawled dataset of web pages for repeatable comparisons of focused crawlers. 
For more details, please, read the paper. If you have any question regarding the environment, source codes or results, please, contact us.

## Index to Common Crawl 2012
In order to use the environment you need to have our index to Common Crawl 2012. Since the index takes about 300 GB, we could not place it here,
for download information, please contact us by email: tgogar [ at ] gmail.com

Index is available in plain text or as migratable HBase database. 

## Nutch plugins
In this work we use Nutch crawler, for more details, please see: nutch.apache.org
There are actually two Nutch plugins available:

### protocol-s3
This plugin replaces original protocol-http and it allows you to crawl in Amazon S3

### relevancy-meter
This plugin measures relevancy of crawled web pages, it needs topic description (topic.txt) in conf folder.



