package com.thinkbiganalytics.esbulkloader;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ElasticsearchBulkFormat extends OutputFormat<Text, Text> {

    @Override
    public void checkOutputSpecs(JobContext arg0) throws IOException,
	    InterruptedException {
	
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
	    throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
	    throws IOException, InterruptedException {
	return new ElasticsearchRecordWriter(context);
    }

}
