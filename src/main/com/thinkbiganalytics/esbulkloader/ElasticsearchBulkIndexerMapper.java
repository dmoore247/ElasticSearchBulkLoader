package com.thinkbiganalytics.esbulkloader;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

/**
 * Convert JSON records into Elasticsearch bulk index commands
 * 
 */
public class ElasticsearchBulkIndexerMapper extends
	Mapper<LongWritable, Text, NullWritable, Text> {

    private static Log log = LogFactory
	    .getLog(ElasticsearchBulkIndexerMapper.class);

    JSONParser p = new JSONParser(JSONParser.MODE_PERMISSIVE);
    String esIndex;
    String esType;
    String esIndexField;
    String indexFormatString;

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {
	esIndex = context.getConfiguration().get("esbl.index");
	esType = context.getConfiguration().get("esbl.type");
	esIndexField = context.getConfiguration().get("esbl.index_field");

	esIndex = (null == esIndex ? "default" : esIndex);
	esType = (null == esType ? "default" : esType);
	esIndexField = (null == esIndexField ? "_id" : esIndexField);
	
	indexFormatString = "{\"index\":{ \"_index\":\"" + esIndex
		+ "\", \"_type\":\"" + esType + "\", \"_id\":\"%s\" } }\n%s\n";
	log.info("Index format string: " + indexFormatString);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
	    throws IOException {

	try {
	    // parse JSON to get id field
	    String id = (String) ((JSONObject) p.parse(value.toString()))
		    .get(esIndexField);

	    context.write(null,
		    new Text(String.format(indexFormatString, id, value)));
	} catch (Exception e) {
	    log.error(e.toString());
	    throw new IOException(value.toString(), e);
	}

    }

}