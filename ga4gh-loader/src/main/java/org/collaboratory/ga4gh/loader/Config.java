package org.collaboratory.ga4gh.loader;

public class Config {

	public static final String INDEX_NAME = "dcc-variants";
	public static final String TYPE_NAME = "variants";
	public static final String NODE_ADDRESS = "10.30.128.130";
	public static final int NODE_PORT = 9300;
	public static final String ES_URL = "es://" + NODE_ADDRESS + ":" + NODE_PORT;
	public static final String TOKEN = System.getProperty("token");
	public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
	public static final String PORTAL_API = "https://dcc.icgc.org";

}
