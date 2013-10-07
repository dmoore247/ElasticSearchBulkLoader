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
    private static final String DEFAULT_ES_PORT = "9200";
    private static final String DEFAULT_ES_HOST = "localhost";
    private static final String ESBL_PORT = "esbl.port";
    private static final String ESBL_HOST = "esbl.host";

    private static Log log = LogFactory.getLog(ElasticsearchRecordWriter.class);

    PostMethod method;
    final String apiUrl;

    public ElasticsearchRecordWriter(TaskAttemptContext context) {
	String host = context.getConfiguration().get(ESBL_HOST);
	String port = context.getConfiguration().get(ESBL_PORT);
	host = (null == host ? DEFAULT_ES_HOST : host);
	port = (null == port ? DEFAULT_ES_PORT : port);
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