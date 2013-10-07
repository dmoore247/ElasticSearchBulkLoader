ElasticSearchBulkLoader
=======================

## Build

# Setup Maven
`mvn eclipse:eclipse`

# Create jar files (this package plus this package and all dependencies)
mvn package


## Usage:

It's important to put the -D parameters first in the command line.
Beyond the hadoop parameters, the first parameter is the input path, and the second is the output path.

# Standalone dev test with Hadoop local mode 
This runs the default 
main class com.thinkbiganalytics.esbulkloader.ElasticsearchBulkLoader
the indexing mapper: com.thinkbiganalytics.esbulkloader.ElasticsearchBulkIndexerMapper
and the com.thinkbiganalytics.esbulkloader.ElasticsearchBulkFormat OutputFormat class.
```
java -jar target/thinkbig-elasticsearch-loader-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -Desbl.index=myindex -Desbl.type=mytype -Desbl.buffer_size=800000 -Desbl.host=localhost -Desbl.port=9200 myinput.json myoutput_directory
```

# Hadoop jar
```
hadoop jar target/thinkbig-elasticsearch-loader-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -Desbl.index=myindex -Desbl.type=mytype -Desbl.buffer_size=800000 hdfs:/out/mytupe/2013/09/24 hdfs:/out/run1
```
