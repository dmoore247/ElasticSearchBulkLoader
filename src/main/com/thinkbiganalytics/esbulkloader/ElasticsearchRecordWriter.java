package com.thinkbiganalytics.esbulkloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ElasticsearchRecordWriter extends RecordWriter<Text, Text>{

    public ElasticsearchRecordWriter(TaskAttemptContext context) {
	// TODO Auto-generated constructor stub
    }

    @Override
    public void write(Text key, Text value) throws IOException,
	    InterruptedException {
	PutMethod method = new PutMethod("http://localhost:9200/tv_perf_v3/voters/_bulk");

	method.setRequestEntity(new StringRequestEntity(value.toString(),
		"text/json", "UTF-8"));

	System.out.println(value);

	BufferedReader br = null;
	HttpClient client = new HttpClient();
	client.getParams().setParameter("http.useragent", "ESBL");
	try {
	    int returnCode = client.executeMethod(method);

	    if (returnCode == HttpStatus.SC_NOT_IMPLEMENTED) {
		System.err
			.println("The Post method is not implemented by this URI");
		// still consume the response body
		method.getResponseBodyAsString();
	    } else {
		br = new BufferedReader(new InputStreamReader(
			method.getResponseBodyAsStream()));
		String readLine;
		while (((readLine = br.readLine()) != null)) {
		    System.err.println(readLine);
		}
	    }
	} catch (Exception e) {
	    System.err.println(e);
	} finally {
	    method.releaseConnection();
	    if (br != null) {
		try {
		    br.close();
		} catch (Exception fe) {
		}
	    }
	}
    } 

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
	    InterruptedException {
	// TODO Auto-generated method stub
    }

}