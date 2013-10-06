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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ElasticsearchBulkLoader extends Configured implements Tool {
  
    private static Log log = LogFactory.getLog(ElasticsearchBulkLoader.class);

    public static void main(String[] args) throws Exception {
	// Use generic options tool runner
	int exitCode = ToolRunner.run(new ElasticsearchBulkLoader(), args); 
	System.exit(exitCode); 
    }

    @Override
    public int run(String[] args) throws Exception {
	System.out.println(String.format("%s %s %s", args[0], args[1], args[2]));
	
	Configuration conf = new Configuration();

	Job job = new Job(conf, args[0]);
	job.setJarByClass(ElasticsearchBulkLoader.class);
	job.setMapperClass(ElasticsearchBulkIndexerMapper.class);
	job.setNumReduceTasks(0);
	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(ElasticsearchBulkFormat.class);
	
	 FileInputFormat.addInputPath(job, new Path(args[1]));
	 FileOutputFormat.setOutputPath(job, new Path(args[2]));

	// actually run the job
	boolean success = job.waitForCompletion(true);
	return (success ? 0 : 1);
    }

}
