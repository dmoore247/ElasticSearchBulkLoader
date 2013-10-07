package com.thinkbiganalytics.esbulkloader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ElasticsearchBulkLoader extends Configured implements Tool {

    private static Log log = LogFactory.getLog(ElasticsearchBulkLoader.class);

    public static void main(String[] args) throws Exception {
	// Use generic options tool runner
	int exitCode = ToolRunner.run(new Configuration(),
		new ElasticsearchBulkLoader(), args);
	System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length < 2) {
	    log.error("Need input and outputs for argument 1 and 2");
	    return 1;
	}
	log.info(String.format("Input : %s",otherArgs[0]));
	log.info(String.format("Output: %s",otherArgs[1]));

	Job job = new Job(conf);
	job.setJobName("ElasticsearchBulkLoader - Indexer");

	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	job.setJarByClass(ElasticsearchBulkLoader.class);
	job.setMapperClass(ElasticsearchBulkIndexerMapper.class);
	job.setNumReduceTasks(0);
	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(ElasticsearchBulkFormat.class);
	
	log.info("Input  Paths: " + FileInputFormat.getInputPaths(job)[0].toString());
	log.info("Output Paths: " + FileOutputFormat.getOutputPath(job).toString());

	job.submit();
	log.info(String.format("Job submitted %s",job.getJobID()));
	// actually run the job
	boolean success = job.waitForCompletion(true);
	log.info("Job completed, success="+success);
	return (success ? 0 : 1);
    }

}
