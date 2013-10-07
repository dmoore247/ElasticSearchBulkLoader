package com.thinkbiganalytics.esbulkloader;

import org.apache.hadoop.conf.Configuration;

public class ElasticsearchConfig {

    private static final int DEFAULT_BUFFER_SIZE = 100000;
    private static final String ESBL_BUFFER_SIZE = "esbl.buffer_size";
    private static final String DEFAULT_ES_PORT = "9200";
    private static final String DEFAULT_ES_HOST = "localhost";
    private static final String ESBL_PORT = "esbl.port";
    private static final String ESBL_HOST = "esbl.host";

    private final String host;
    private final int port;
    private final String apiUrl;
    private final int bufferSize;

    public ElasticsearchConfig(Configuration configuration) {
	// Host
	String h = configuration.get(ESBL_HOST);
	host = (null == h ? DEFAULT_ES_HOST : h);

	// Port
	String portString = configuration.get(ESBL_PORT);
	port = Integer.parseInt((null == portString ? DEFAULT_ES_PORT
		: portString));

	// Buffer Size
	String size = configuration.get(ESBL_BUFFER_SIZE);
	bufferSize = (null == size ? DEFAULT_BUFFER_SIZE : Integer
		.parseInt(size));

	// URL
	apiUrl = String.format("http://%s:%s/_bulk", host, port);
    }

    /**
     * @return the host
     */
    public String getHost() {
	return host;
    }

    /**
     * @return the port
     */
    public int getPort() {
	return port;
    }

    /**
     * @return the apiUrl
     */
    public String getApiUrl() {
	return apiUrl;
    }

    /**
     * @return the bufferSize
     */
    public int getBufferSize() {
	return bufferSize;
    }

}
