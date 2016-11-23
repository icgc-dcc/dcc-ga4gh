package org.collaboratory.ga4gh.loader;

public class Config {

  public static final String INDEX_NAME = "dcc-variants";
  public static final String TYPE_NAME = "variant";
  public static final String NODE_ADDRESS = System.getProperty("node_address", "localhost");
  public static final int NODE_PORT = Integer.valueOf(System.getProperty("node_port", "9300"));
  public static final String ES_URL = "es://" + NODE_ADDRESS + ":" + NODE_PORT;
  public static final String TOKEN = System.getProperty("token");
  public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
  public static final String PORTAL_API = "https://dcc.icgc.org";

  public static String toConfigString() {
    return "INDEX_NAME: " + INDEX_NAME + "\n"
        + "TYPE_NAME: " + TYPE_NAME + "\n"
        + "NODE_ADDRESS: " + NODE_ADDRESS + "\n"
        + "NODE_PORT: " + NODE_PORT + "\n"
        + "ES_URL: " + ES_URL + "\n"
        + "TOKEN: " + TOKEN + "\n"
        + "STORAGE_API: " + STORAGE_API + "\n"
        + "PORTAL_API: " + PORTAL_API;
  }

}
