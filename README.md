ElasticSearchBulkLoader
=======================

A package to bulkload and index JSON formatted records, each record in the form of:
``` {"_id":"id1234", "key1":"value1", "key2":"value2"}\n```

Given the index and type parameters, the mapper will create the index command fragment
and append it to one JSON record. This combined value is sent to the OutputFormatter
for buffering and transmittial to Elasticsearch.

Example mapper output:
```
{index:{"_index":"default","_type":"mytype","_id":"id1234"}}\n
{"_id":"id1234", "key1":"value1", "key2":"value2"}\n
```


### Parameters
 - `esbl.host`        : defaults to localhost
 - `esbl.port`        : defaults to 9200
 - `esbl.index`       : defaults to 'default'
 - `esbl.type`        : defaults to 'default'
 - `esbl.index_field` : defaults to '_id'

## Build

### Setup Maven
```
mvn eclipse:eclipse
```

### Create jar files 
For this package plus this package and all dependencies
```
mvn package
```
and jar files will be created in the target/ sub directory.

## Usage

It's important to put the -D parameters first in the command line.
Beyond the hadoop parameters, the first parameter is the input path, and the second is the output path.

### Standalone dev test with Hadoop local mode 
This runs the
 - `com.thinkbiganalytics.esbulkloader.ElasticsearchBulkLoader` for default main class, 
 - `com.thinkbiganalytics.esbulkloader.ElasticsearchBulkIndexerMapper` as the indexing mapper,
 - `com.thinkbiganalytics.esbulkloader.ElasticsearchBulkFormat` as the OutputFormat class.
```
java -jar target/thinkbig-elasticsearch-loader-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -Desbl.index=myindex -Desbl.type=mytype -Desbl.buffer_size=800000 -Desbl.host=localhost -Desbl.port=9200 myinput.json myoutput_directory
```

### Hadoop jar
```
hadoop jar target/thinkbig-elasticsearch-loader-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -Desbl.index=myindex -Desbl.type=mytype -Desbl.buffer_size=800000 hdfs:/out/mytupe/2013/09/24 hdfs:/out/run1
```
