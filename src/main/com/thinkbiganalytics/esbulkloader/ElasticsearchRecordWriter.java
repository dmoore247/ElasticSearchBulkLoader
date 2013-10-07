package com.thinkbiganalytics.esbulkloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Buffer up JSON bulk commands passed as Text values to this writer and then
 * transmit them to Elasticsearch. 
 */
public class ElasticsearchRecordWriter extends RecordWriter<Text, Text> {

    private static Log log = LogFactory.getLog(ElasticsearchRecordWriter.class);

    private PostMethod method;
    private final StringBuffer buffer;
    private final ElasticsearchConfig config;

    public ElasticsearchRecordWriter(TaskAttemptContext context) {
	config = new ElasticsearchConfig(context.getConfiguration());

	// Buffer for messages
	buffer = new StringBuffer(config.getBufferSize());
    }

    /**
     * Buffer the Text value and check the buffer for flushing. Check buffer
     * will flush if needed.
     */
    @Override
    public void write(Text key, Text value) throws IOException,
	    InterruptedException {

	buffer.append(value.toString());
	// log.debug(value.toString());

	checkFlush();
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
	    InterruptedException {
	flush();
	method.releaseConnection();
    }

    private void checkFlush() throws UnsupportedEncodingException {
	if (buffer.length() > config.getBufferSize()) {
	    flush();
	    buffer.setLength(0);
	}
    }

    /**
     * Send buffer to server
     * 
     * @throws UnsupportedEncodingException
     */
    private void flush() throws UnsupportedEncodingException {
	method = new PostMethod(config.getApiUrl());
	method.setRequestEntity(new StringRequestEntity(buffer.toString(),
		"text/json", "UTF-8"));

	executeMethod();

    }

    /**
     * Run the HTTP method and transmit results to Elasticsearch
     */
    protected void executeMethod() {
	BufferedReader br = null;

	try {
	    int returnCode = getHttpClient().executeMethod(method);

	    if (returnCode == HttpStatus.SC_NOT_IMPLEMENTED) {
		log.error(String.format(
			"The Post method is not implemented by this URL (%s)",
			method.getURI()));
	    }

	    // Emit server response for debugging
	    if (log.isDebugEnabled()) {
		br = new BufferedReader(new InputStreamReader(
			method.getResponseBodyAsStream()));

		String readLine;
		while (((readLine = br.readLine()) != null)) {
		    System.out.println(readLine);
		}
	    } else {
		// still consume the response body
		// method.getResponseBodyAsString();
	    }

	} catch (IOException ioe) {
	    log.error(ioe);
	} finally {
	    if (null != br) {
		try {
		    br.close();
		} catch (IOException cioe) {
		    log.error(cioe);
		}
	    }
	}
    }

    /**
     * Setup HttpClient with optimal settings
     * 
     * @return HttpClient
     */
    protected HttpClient getHttpClient() {
	HttpClientParams params = new HttpClientParams();
	params.setBooleanParameter("http.tcp.nodelay", true);
	HttpClient client = new HttpClient();
	client.setParams(params);
	return client;
    }

}