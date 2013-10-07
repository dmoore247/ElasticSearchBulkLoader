package com.thinkbiganalytics.esbulkloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 
 * 
 */
public class ElasticsearchRecordWriter extends RecordWriter<Text, Text> {
    private static Log log = LogFactory.getLog(ElasticsearchRecordWriter.class);

    PostMethod method;
    final String apiUrl;

    public ElasticsearchRecordWriter(TaskAttemptContext context) {
	String host = context.getConfiguration().get("esbl.host");
	String port = context.getConfiguration().get("esbl.port");
	host = (null == host ? "localhost" : host);
	port = (null == port ? "9200" : port);
	apiUrl = String.format("http://%s:%s/_bulk", host, port);

    }

    @Override
    public void write(Text key, Text value) throws IOException,
	    InterruptedException {

	log.info(value.toString());
	method = new PostMethod(apiUrl);
	method.setRequestEntity(new StringRequestEntity(value.toString(),
		"text/json", "UTF-8"));

	BufferedReader br = null;
	HttpClient client = new HttpClient();
	try {
	    int returnCode = client.executeMethod(method);

	    if (returnCode == HttpStatus.SC_NOT_IMPLEMENTED) {
		log.error(String.format(
			"The Post method is not implemented by this URI (%s)",
			apiUrl));
		// still consume the response body
		method.getResponseBodyAsString();
	    } else {
		br = new BufferedReader(new InputStreamReader(
			method.getResponseBodyAsStream()));
		// if (log.isInfoEnabled()) {
		String readLine;
		while (((readLine = br.readLine()) != null)) {
		    System.out.println(readLine);
		}
		// }
	    }
	} catch (Exception e) {
	    log.error(e);
	} finally {
	    if (br != null) {
		try {
		    br.close();
		} catch (Exception fe) {
		    log.error(fe);
		}
	    }
	}
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
	    InterruptedException {
	method.releaseConnection();

    }

}