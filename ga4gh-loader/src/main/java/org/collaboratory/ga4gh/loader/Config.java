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
        + "\nPORTAL_API: %s"
        + "\nNUM_THREADS: %s"
        + "\nBULK_SIZE_MB: %s"
        + "\nPERSIST_MODE: %s"
        + "\nDATA_FETCHER_SHUFFLE: %s"
        + "\nDATA_FETCHER_SOMATIC_SSMS_ONLY: %s"
        + "\nDATA_FETCHER_MAX_FILESIZE_BYTES: %s"
        + "\nDATA_FETCHER_NUM_DONORS: %s"
        + "\nOUTPUT_VCF_STORAGE_DIR: %s",
        INDEX_NAME,
        NODE_ADDRESS,
        NODE_PORT,
        ES_URL,
        TOKEN,
        STORAGE_API,
        PORTAL_API,
        NUM_THREADS,
        BULK_SIZE_MB,
        PERSIST_MODE,
        DATA_FETCHER_SHUFFLE,
        DATA_FETCHER_SOMATIC_SSMS_ONLY,
        DATA_FETCHER_MAX_FILESIZE_BYTES,
        DATA_FETCHER_NUM_DONORS,
        OUTPUT_VCF_STORAGE_DIR);
  }

  public static final int NUM_THREADS = 4;
  public static final int BULK_SIZE_MB = 250;
  public static final boolean PERSIST_MODE = false;
  public static final boolean DATA_FETCHER_SHUFFLE = true;
  public static final boolean DATA_FETCHER_SOMATIC_SSMS_ONLY = true;
  public static final long DATA_FETCHER_MAX_FILESIZE_BYTES = 700000;
  public static final int DATA_FETCHER_NUM_DONORS = 30;
  public static final String OUTPUT_VCF_STORAGE_DIR = "target/storedVCFs";

}
