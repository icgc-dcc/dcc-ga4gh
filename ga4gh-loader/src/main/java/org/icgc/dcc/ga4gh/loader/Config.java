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

import lombok.val;
import org.icgc.dcc.ga4gh.common.types.IndexModes;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.icgc.dcc.ga4gh.common.MiscNames.FALSE;

public class Config {

  /**
   * Fixed
   */
  public static final int MONITOR_INTERVAL_COUNT = 500000;
  public static final boolean FILTER_VARIANTS = false;
  public static final String CURRENT_TIMESTAMP = (new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date());
  public static final String STORAGE_API = "https://storage.cancercollaboratory.org";
  public static final String PORTAL_API = "https://dcc.icgc.org";
  public static final long DEFAULT_MAPDB_ALLOCATION = 2 * 1024 * 1024;
  public static final long VARIANT_MAPDB_ALLOCATION = 1024 * 1024 * 1024; //1GB
  public static final Path MAPPINGS_DIR = Paths.get("org/icgc/dcc/ga4gh/resources/mappings");
  public static final Path INDEX_SETTINGS_JSON_FILENAME = MAPPINGS_DIR.resolve("index.settings.json");
  public static final Path CALLSET_INDEX_MAPPING_FILE = MAPPINGS_DIR.resolve("callset.mapping.json");
  public static final Path VARIANT_SET_INDEX_MAPPING_FILE = MAPPINGS_DIR.resolve("variant_set.mapping.json");
  public static final Path NESTED_VARIANT_INDEX_MAPPING_FILE = MAPPINGS_DIR.resolve("variant_nested.mapping.json");
  public static final Path PC_VARIANT_INDEX_MAPPING_FILE = MAPPINGS_DIR.resolve("variant_pc.mapping.json");
  public static final Path PC_CALL_INDEX_MAPPING_FILE = MAPPINGS_DIR.resolve("call_pc.mapping.json");

  /**
   * Required User Input
   */
  public static final String TOKEN = requireStringProperty("token");
  public static final String NODE_ADDRESS = requireStringProperty("node_address");
  public static final int NODE_PORT = requireIntegerProperty("node_port");
  public static final IndexModes INDEX_MODE = requireIndexModeProperty("index_mode");
  public static final boolean STORAGE_PERSIST_MODE = requireBooleanProperty("persist_mode");

  /**
   * Optional User Input
   */
  private static final String DEFAULT_INDEX_NAME = "dcc-variants-"+CURRENT_TIMESTAMP+"-"+INDEX_MODE.name();
  public static final String INDEX_NAME = getProperty("index_name", DEFAULT_INDEX_NAME.toLowerCase());
  public static final String STORAGE_OUTPUT_VCF_STORAGE_DIR = getProperty("vcf_dir","target/storedVCFs");
  public static final boolean STORAGE_BYPASS_MD5_CHECK = parseBoolean(getProperty("bypass_md5_check", FALSE));
  private static final String PERSISTED_DIRNAME = getProperty("persisted_dir", "persisted");
  public static final Path PERSISTED_DIRPATH = Paths.get(PERSISTED_DIRNAME);
  public static final int BULK_NUM_THREADS = parseInt(getProperty("num_threads", "5"));
  public static final int BULK_SIZE_MB = parseInt(getProperty("bulk_size_mb", "5"));

  private static String requireStringProperty(String propName){
    val propValue = getProperty(propName);
    checkNotNull(propValue, "The required config argument [%s] must be defined", propName);
    return propValue;
  }

  private static Boolean requireBooleanProperty(String propName){
    return parseBoolean(requireStringProperty(propName));
  }

  private static Integer requireIntegerProperty(String propName){
    return parseInt(requireStringProperty(propName));
  }

  private static IndexModes requireIndexModeProperty(String propName){
    return IndexModes.valueOf(requireStringProperty(propName));
  }



}
