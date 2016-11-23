package org.collaboratory.ga4gh.loader;

public class Config {

  public static final String INDEX_NAME = "dcc-variants";
  public static final String TYPE_NAME = "variant";
  public static final String NODE_ADDRESS = System.getProperty("node_address", "ga4gh-elasticsearch-1");
  public static final int NODE_PORT = Integer.valueOf(System.getProperty("node_port", "9300"));
  public static final String ES_URL = "es://" + NODE_ADDRESS + ":" + NODE_PORT;
  public static final String TOKEN = System.getProperty("token");
  public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
  public static final String PORTAL_API = "https://dcc.icgc.org";

  public static String toConfigString() {
    StringBuilder sb = new StringBuilder();
    sb.append("INDEX_NAME: " + INDEX_NAME + "\n");
    sb.append("TYPE_NAME: " + TYPE_NAME + "\n");
    sb.append("NODE_ADDRESS: " + NODE_ADDRESS + "\n");
    sb.append("NODE_PORT: " + NODE_PORT + "\n");
    sb.append("ES_URL: " + ES_URL + "\n");
    sb.append("TOKEN: " + TOKEN + "\n");
    sb.append("STORAGE_API: " + STORAGE_API + "\n");
    sb.append("PORTAL_API: " + PORTAL_API + "\n");
    sb.append("PORTAL_FETCH_SIZE: " + PORTAL_FETCH_SIZE + "\n");
    sb.append("REPOSITORY_NAME: " + REPOSITORY_NAME + "\n");
    sb.append("FILE_FORMAT: " + FILE_FORMAT + "\n");
    return sb.toString();
  }

}
