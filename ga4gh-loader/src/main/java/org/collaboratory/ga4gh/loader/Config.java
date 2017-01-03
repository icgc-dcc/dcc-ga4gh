package org.collaboratory.ga4gh.loader;

public class Config {

  public static final String INDEX_NAME = "dcc-variants";
  public static final String NODE_ADDRESS = System.getProperty("node_address", "localhost");
  public static final int NODE_PORT = Integer.valueOf(System.getProperty("node_port", "9300"));
  public static final String ES_URL = "es://" + NODE_ADDRESS + ":" + NODE_PORT;
  public static final String TOKEN = System.getProperty("token");
  public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
  public static final String PORTAL_API = "https://dcc.icgc.org";

  public static String toConfigString() {
    return String.format("INDEX_NAME: %s"
        + "\nNODE_ADDRESS: %s"
        + "\nNODE_PORT: %s"
        + "\nES_URL: %s"
        + "\nTOKEN: %s"
        + "\nSTORAGE_API: %s"
        + "\nPORTAL_API: %s",
        INDEX_NAME,
        NODE_ADDRESS,
        NODE_PORT,
        ES_URL,
        TOKEN,
        STORAGE_API,
        PORTAL_API);
  }

}
