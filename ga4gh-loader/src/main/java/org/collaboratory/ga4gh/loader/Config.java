package org.collaboratory.ga4gh.loader;

import static java.lang.System.getProperty;
import static org.collaboratory.ga4gh.core.Names.FALSE;
import static org.collaboratory.ga4gh.core.Names.TRUE;

public class Config {

  public static final String INDEX_NAME = getProperty("index_name", "dcc-variants3");
  public static final String NODE_ADDRESS = getProperty("node_address", "localhost");
  public static final int NODE_PORT = Integer.parseInt(getProperty("node_port", "9300"));
  public static final String ES_URL = "es://" + NODE_ADDRESS + ":" + NODE_PORT;
  public static final String TOKEN = getProperty("token");
  public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
  public static final String PORTAL_API = "https://dcc.icgc.org";

  public static final int BULK_NUM_THREADS = Integer.parseInt(getProperty("num_threads", "5"));
  public static final int BULK_SIZE_MB = Integer.parseInt(getProperty("bulk_size_mb", "5"));
  public static final boolean STORAGE_PERSIST_MODE = Boolean.parseBoolean(getProperty("persist_mode", FALSE));
  public static final boolean SORT_MODE = Boolean.parseBoolean(getProperty("sort_mode", TRUE));
  public static final boolean ASCENDING_MODE = Boolean.parseBoolean(getProperty("ascending_mode", FALSE));
  public static final boolean DATA_FETCHER_SHUFFLE = !SORT_MODE;
  public static final boolean DATA_FETCHER_SOMATIC_SSMS_ONLY = true;
  public static final long DATA_FETCHER_MAX_FILESIZE_BYTES = Long.parseLong(getProperty("max_filesize_bytes", "0"));
  public static final int DATA_FETCHER_NUM_DONORS = 30;
  public static final int DATA_FETCHER_LIMIT = 100;
  public static final String STORAGE_OUTPUT_VCF_STORAGE_DIR = "target/storedVCFs";
  public static final String DEFAULT_FILE_META_DATA_STORE_FILENAME = "target/allFileMetaDatas.bin";
  public static final boolean USE_MAP_DB = Boolean.parseBoolean(getProperty("use_map_db", TRUE));
  public static final int MONITOR_INTERVAL_COUNT = 500000;
  public static final boolean STORAGE_BYPASS_MD5_CHECK = Boolean.parseBoolean(getProperty("bypass_md5_check", FALSE));

  public static String toConfigString() {
    return String.format("INDEX_NAME: %s"
        + "\nNODE_ADDRESS: %s"
        + "\nNODE_PORT: %s"
        + "\nES_URL: %s"
        + "\nTOKEN: %s"
        + "\nSTORAGE_API: %s"
        + "\nPORTAL_API: %s"
        + "\nBULK_NUM_THREADS: %s"
        + "\nBULK_SIZE_MB: %s"
        + "\nPERSIST_MODE: %s"
        + "\nSORT_MODE: %s"
        + "\nASCENDING_MODE: %s"
        + "\nDATA_FETCHER_SHUFFLE: %s"
        + "\nDATA_FETCHER_SOMATIC_SSMS_ONLY: %s"
        + "\nDATA_FETCHER_MAX_FILESIZE_BYTES: %s"
        + "\nDATA_FETCHER_NUM_DONORS: %s"
        + "\nOUTPUT_VCF_STORAGE_DIR: %s"
        + "\nFILE_META_DATA_STORE_FILENAME: %s"
        + "\nUSE_MAP_DB: %s"
        + "\nMONITOR_INTERVAL_COUNT: %s"
        + "\nSTORAGE_BYPASS_MD5_CHECK: %s",
        INDEX_NAME,
        NODE_ADDRESS,
        NODE_PORT,
        ES_URL,
        TOKEN,
        STORAGE_API,
        PORTAL_API,
        BULK_NUM_THREADS,
        BULK_SIZE_MB,
        STORAGE_PERSIST_MODE,
        SORT_MODE,
        ASCENDING_MODE,
        DATA_FETCHER_SHUFFLE,
        DATA_FETCHER_SOMATIC_SSMS_ONLY,
        DATA_FETCHER_MAX_FILESIZE_BYTES,
        DATA_FETCHER_NUM_DONORS,
        STORAGE_OUTPUT_VCF_STORAGE_DIR,
        DEFAULT_FILE_META_DATA_STORE_FILENAME,
        USE_MAP_DB,
        MONITOR_INTERVAL_COUNT,
        STORAGE_BYPASS_MD5_CHECK);

  }

}
