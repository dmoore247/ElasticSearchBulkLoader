package com.thinkbiganalytics.esbulkloader;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ElasticSearchBulkLoader {
    
    private static Log log = LogFactory.getLog(ElasticSearchBulkLoader.class);
    public static void main(String args[]) throws Exception {
	
	log.info(String.format("%s %s %s", args[0], args[1], args[2]));
	
	Configuration conf = new Configuration();

	Job job = new Job(conf, args[0]);
	job.setJarByClass(ElasticSearchBulkLoader.class);
	job.setMapperClass(IndexFilesMapper.class);
	job.setNumReduceTasks(0);
	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(ElasticsearchBulkFormat.class);
	
	 FileInputFormat.addInputPath(job, new Path(args[1]));
	 FileOutputFormat.setOutputPath(job, new Path(args[2]));

	// actually run the job
	boolean success = job.waitForCompletion(true);

	System.exit(success ? 0 : 1);
    }

    public class IndexFilesMapper extends
	    Mapper<NullWritable, Text, NullWritable, Text> {

	@Override
	public void setup(Context context) throws IOException {

	}

	@Override
	public void map(NullWritable key, Text value, Context context)
		throws IOException {
	    // parse the binary, convert it to JSON, and index that JSON
	    try {
		String id="123";
		String bulkJsonFormat = "{\"index\":{ \"_index\":\"tv_perf_v3\", \"_type\":\"voters\",\"_id\":\"%s\" } }\n%s\n";

		context.write(null, new Text(String.format(bulkJsonFormat, id, value)));

	    } catch (Exception e) {
		throw new IOException(e);
	    }
	}

    }

}
