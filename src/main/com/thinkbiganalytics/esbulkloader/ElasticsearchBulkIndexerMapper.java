package com.thinkbiganalytics.esbulkloader;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Convert JSON records into Elasticsearch bulk index commands
 * 
 */
public class ElasticsearchBulkIndexerMapper extends
	Mapper<NullWritable, Text, NullWritable, Text> {
    
    static final String  BULK_INDEX_FORMAT_STRING = "{\"index\":{ \"_index\":\"tv_perf_v3\", \"_type\":\"voters\" } }\n%s\n";

    @Override
    public void map(NullWritable key, Text value, Context context)
	    throws IOException {

	try {
	    context.write(null,
		    new Text(String.format(BULK_INDEX_FORMAT_STRING, value)));
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    throw new IOException(value.toString(), e);
	}

    }

}