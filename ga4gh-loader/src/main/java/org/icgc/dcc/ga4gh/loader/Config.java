/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.icgc.dcc.ga4gh.loader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static org.icgc.dcc.ga4gh.common.MiscNames.FALSE;
import static org.icgc.dcc.ga4gh.common.MiscNames.TRUE;
import static org.icgc.dcc.ga4gh.loader.LoaderModes.parseLoaderMode;

public class Config {

  public static final String CURRENT_TIMESTAMP = (new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date());
  public static final LoaderModes LOADER_MODE = parseLoaderMode(parseInt(getProperty("loader_mode", "3")));
  public static final String PARENT_CHILD_INDEX_NAME = getProperty("parent_child_index_name", "dcc-variants-pc");
  public static final String NESTED_INDEX_NAME = getProperty("nested_index_name", "dcc-variants-nested");
  public static final String INDEX_NAME = getProperty("index_name", "dcc-variants-"+CURRENT_TIMESTAMP);
  public static final int NESTED_SCROLL_SIZE = parseInt(getProperty("nested_scroll_size", "1000"));
  public static final String NODE_ADDRESS = getProperty("node_address", "localhost");
  public static final int NODE_PORT = parseInt(getProperty("node_port", "9300"));
  public static final String ES_URL = "es://" + NODE_ADDRESS + ":" + NODE_PORT;
  public static final String TOKEN = getProperty("token");
  public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
  public static final String PORTAL_API = "https://dcc.icgc.org";

  public static final int BULK_NUM_THREADS = parseInt(getProperty("num_threads", "5"));
  public static final int BULK_SIZE_MB = parseInt(getProperty("bulk_size_mb", "5"));
  public static final boolean STORAGE_PERSIST_MODE = parseBoolean(getProperty("persist_mode", FALSE));
  public static final String STORAGE_OUTPUT_VCF_STORAGE_DIR = getProperty("vcf_dir","target/storedVCFs");
  public static final String DEFAULT_FILE_META_DATA_STORE_FILENAME = "target/allFileMetaDatas.dat";
  public static final boolean USE_MAP_DB = parseBoolean(getProperty("use_map_db", FALSE));
  public static final int MONITOR_INTERVAL_COUNT = 500000;
  public static final boolean STORAGE_BYPASS_MD5_CHECK = parseBoolean(getProperty("bypass_md5_check", FALSE));
  public static final boolean FILTER_VARIANTS = false;

  public static final boolean SORT_MODE = parseBoolean(getProperty("sort_mode", TRUE));
  public static final boolean ASCENDING_MODE = parseBoolean(getProperty("ascending_mode", FALSE));
  public static final long DATA_FETCHER_MAX_FILESIZE_BYTES = parseLong(getProperty("max_filesize_bytes", "0"));
  public static final int DATA_FETCHER_LIMIT = parseInt(getProperty("fetch_limit", "0"));
  public static final boolean INDEX_ONLY = parseBoolean(getProperty("index_only", FALSE));
  public static final Optional<String> VARIANT_MAP_DB_FILENAME = Optional.ofNullable(getProperty("variant_map_db_filename"));

  public static final String PERSISTED_DIRNAME = getProperty("persisted_dir", "persisted");
  public static final Path PERSISTED_DIRPATH = Paths.get(PERSISTED_DIRNAME);
  public static final boolean DEFAULT_PERSIST_MAPDB_FILE = false;
  public static final long DEFAULT_MAPDB_ALLOCATION = 2 * 1024 * 1024;
  public static final long VARIANT_MAPDB_ALLOCATION = 1024 * 1024 * 1024; //1GB


  public static final String INDEX_SETTINGS_JSON_FILENAME = "index.settings.json";
  public static final String DEFAULT_MAPPINGS_DIRNAME = "org/icgc/dcc/ga4gh/resources/mappings";
  public static final String DEFAULT_MAPPING_JSON_EXTENSION = ".mapping.json";

  private static final int MAX_NUM_SEGMENTS = 1;


  public static String toConfigString() {
    return String.format("PARENT_CHILD_INDEX_NAME: %s"
        + "\nNESTED_INDEX_NAME: %s"
        + "\nNESTED_SCROLL_SIZE: %s"
        + "\nLOADER_MODE: %s"
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
        + "\nDATA_FETCHER_MAX_FILESIZE_BYTES: %s"
        + "\nOUTPUT_VCF_STORAGE_DIR: %s"
        + "\nFILE_META_DATA_STORE_FILENAME: %s"
        + "\nUSE_MAP_DB: %s"
        + "\nMONITOR_INTERVAL_COUNT: %s"
        + "\nSTORAGE_BYPASS_MD5_CHECK: %s",
        PARENT_CHILD_INDEX_NAME,
        NESTED_INDEX_NAME,
        NESTED_SCROLL_SIZE,
        LOADER_MODE.name() + " (" + LOADER_MODE.getModeId() + ")",
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
        DATA_FETCHER_MAX_FILESIZE_BYTES,
        STORAGE_OUTPUT_VCF_STORAGE_DIR,
        DEFAULT_FILE_META_DATA_STORE_FILENAME,
        USE_MAP_DB,
        MONITOR_INTERVAL_COUNT,
        STORAGE_BYPASS_MD5_CHECK);

  }

}
