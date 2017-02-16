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
package org.collaboratory.ga4gh.loader.model.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.val;
import org.collaboratory.ga4gh.loader.Config;
import org.collaboratory.ga4gh.loader.Portal;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;

/*
 * TODO: [rtisma] Temporary untill learn Portal api. Everything here can probably done via portal api
 * and then once get back ObjectNodes can convert to FileMetaData object, and then delete this class
 */
//NOTE: had to add emptyBuilder becuase lombok intellij
// plugin acting up and not working correctly
// (said that FileMetaDataFetcherBuilder constructor was not public)
@Builder(builderMethodName = "emptyBuilder")
@Data
public class FileMetaDataFetcher {

  public static final int DEFAULT_NUM_DONORS = 0;
  public static final int DEFAULT_MAX_FILESIZE_BYTES = 0;
  public static final int DEFAULT_LIMIT_NUM_FILES = 0;
  public static final String DEFAULT_STORAGE_FILENAME = Config.DEFAULT_FILE_META_DATA_STORE_FILENAME;

  private final int numDonors;
  private final long maxFileSizeBytes;
  private final boolean somaticSSMsOnly;
  private final boolean shuffle;
  private final long seed;
  private final int limit;
  private final boolean sort;
  private final boolean ascending;
  private final String storageFilename;
  private final boolean createNewFile;

  @Singular
  private final List<String> specificFilenames;

  public static final long generateSeed() {
    return System.currentTimeMillis();
  }

  public FileMetaDataFetcher(
      int numDonors,
      long maxFileSizeBytes,
      boolean somaticSSMsOnly,
      boolean shuffle,
      long seed,
      int limit,
      boolean sort,
      boolean ascending,
      String storageFilename,
      boolean createNewFile,
      List<String> specificFilenames) {
    this.numDonors = numDonors;
    this.maxFileSizeBytes = maxFileSizeBytes;
    this.somaticSSMsOnly = somaticSSMsOnly;
    this.shuffle = shuffle;
    this.seed = seed;
    this.limit = limit;
    this.sort = sort;
    this.ascending = ascending;
    this.storageFilename = storageFilename;
    this.createNewFile = createNewFile;
    this.specificFilenames = specificFilenames;
    checkConfig();
  }

  // Define default builder

  public static FileMetaDataFetcherBuilder builder() {
    return emptyBuilder()
        .maxFileSizeBytes(DEFAULT_MAX_FILESIZE_BYTES)
        .numDonors(DEFAULT_NUM_DONORS)
        .somaticSSMsOnly(false)
        .seed(generateSeed())
        .limit(DEFAULT_LIMIT_NUM_FILES)
        .sort(false)
        .storageFilename(DEFAULT_STORAGE_FILENAME)
        .ascending(false)
        .shuffle(false);
  }

  /*
   * Return true if set to Non-default fromFile value
   */
  private boolean storageFilenameUpdated() {
    return !storageFilename.equals(DEFAULT_STORAGE_FILENAME);
  }

  /*
   * Return true if set to Non-default numDonor value
   */
  private boolean numDonorsUpdated() {
    return numDonors > DEFAULT_NUM_DONORS;
  }

  /*
   * Return true if set to Non-default maxFileSizeBytes value
   */
  private boolean maxFileSizeUpdated() {
    return maxFileSizeBytes > DEFAULT_MAX_FILESIZE_BYTES;
  }

  /*
   * Return true if set to Non-default limitNumFiles
   */
  private boolean limitNumFilesUpdated() {
    return limit > DEFAULT_LIMIT_NUM_FILES;
  }

  private void checkConfig() {
    val sortAndShuffle = shuffle && sort;
    checkState(!sortAndShuffle,
        "Cannot sort and shuffle the list. These are mutually exclusive configurations");

    val numDonorsAndFromFile = storageFilenameUpdated() && numDonorsUpdated();
    checkState(!numDonorsAndFromFile,
        "Cannot set numDonors and fromFile at the same time as they are mutually exclusive");
  }

  private static FileMetaDataContext getForDonorNum(FileMetaDataContext fileMetaDataContext, final int numDonors) {
    val donorGrouping = fileMetaDataContext.groupFileMetaDatasByDonor();
    val contextBuilder = FileMetaDataContext.builder();
    int count = 0;
    for (val donorId : donorGrouping.keySet()) {
      if (count >= numDonors) {
        break;
      }
      val donorFileMetaContext = donorGrouping.get(donorId);
      contextBuilder.fileMetaDatas(donorFileMetaContext.getFileMetaDatas());
      count++;
    }
    return contextBuilder.build();
  }

  /*
   * Fetch list of FileMetaData object, based on filter configuration
   */
  // TODO: [rtisma] need to update and use Decorator pattern...too much going on
  public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {

    FileMetaDataContext fileMetaDataContext = null;

    val fromFile = Paths.get(storageFilename).toFile();
    val fileExists = fromFile.exists() && fromFile.isFile();
    if (fileExists && !isCreateNewFile()) {
      fileMetaDataContext = FileMetaDataContext.restore(storageFilename);
    } else {
      fileMetaDataContext = Portal.getAllFileMetaDatas();
      fileMetaDataContext.store(storageFilename);
    }

    if (numDonorsUpdated()) {
      fileMetaDataContext = getForDonorNum(fileMetaDataContext, numDonors);
    }

    if (specificFilenames.isEmpty()) {
      // If size > 0, use only files less than or equal to maxFileSizeBytes
      val filteredFileMetaDataContextBySize =
          maxFileSizeUpdated() ? FileMetaDataFilters.filterBySize(fileMetaDataContext,
              maxFileSizeBytes) : fileMetaDataContext;

      val filteredFileMetaDataContext =
          somaticSSMsOnly ? FileMetaDataFilters
              .filterSomaticSSMs(filteredFileMetaDataContextBySize) : filteredFileMetaDataContextBySize;

      FileMetaDataContext outputFileMetaDataContext = null;
      if (shuffle) {
        val rand = new Random();
        rand.setSeed(seed);
        val list = Lists.<FileMetaData> newArrayList(filteredFileMetaDataContext.iterator());
        Collections.shuffle(list, rand);
        outputFileMetaDataContext = FileMetaDataContext.builder().fileMetaDatas(ImmutableList.copyOf(list)).build();
      } else if (sort) {
        outputFileMetaDataContext = filteredFileMetaDataContext.sortByFileSize(ascending);
      }

      if (limitNumFilesUpdated()) {
        val size = outputFileMetaDataContext.size();
        val subFileMetaDataList = outputFileMetaDataContext.getFileMetaDatas().subList(0, Math.min(limit, size));
        return FileMetaDataContext.builder().fileMetaDatas(subFileMetaDataList).build();
      } else {
        return outputFileMetaDataContext;
      }
    } else {
      return FileMetaDataFilters.filterSelectedFilenamesInOrder(fileMetaDataContext, specificFilenames);
    }

  }

  public int getNumDonors() {
    return this.numDonors;
  }

  public long getMaxFileSizeBytes() {
    return this.maxFileSizeBytes;
  }

  public boolean isSomaticSSMsOnly() {
    return this.somaticSSMsOnly;
  }

  public boolean isShuffle() {
    return this.shuffle;
  }

  public long getSeed() {
    return this.seed;
  }

  public int getLimit() {
    return this.limit;
  }

  public boolean isSort() {
    return this.sort;
  }

  public boolean isAscending() {
    return this.ascending;
  }

  public String getStorageFilename() {
    return this.storageFilename;
  }

  public boolean isCreateNewFile() {
    return this.createNewFile;
  }

  public List<String> getSpecificFilenames() {
    return this.specificFilenames;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof FileMetaDataFetcher)) return false;
    final FileMetaDataFetcher other = (FileMetaDataFetcher) o;
    if (!other.canEqual(this)) return false;
    if (this.getNumDonors() != other.getNumDonors()) return false;
    if (this.getMaxFileSizeBytes() != other.getMaxFileSizeBytes()) return false;
    if (this.isSomaticSSMsOnly() != other.isSomaticSSMsOnly()) return false;
    if (this.isShuffle() != other.isShuffle()) return false;
    if (this.getSeed() != other.getSeed()) return false;
    if (this.getLimit() != other.getLimit()) return false;
    if (this.isSort() != other.isSort()) return false;
    if (this.isAscending() != other.isAscending()) return false;
    final Object this$storageFilename = this.getStorageFilename();
    final Object other$storageFilename = other.getStorageFilename();
    if (this$storageFilename == null ? other$storageFilename != null : !this$storageFilename
        .equals(other$storageFilename)) return false;
    if (this.isCreateNewFile() != other.isCreateNewFile()) return false;
    final Object this$specificFilenames = this.getSpecificFilenames();
    final Object other$specificFilenames = other.getSpecificFilenames();
    if (this$specificFilenames == null ? other$specificFilenames != null : !this$specificFilenames
        .equals(other$specificFilenames)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    result = result * PRIME + this.getNumDonors();
    final long $maxFileSizeBytes = this.getMaxFileSizeBytes();
    result = result * PRIME + (int) ($maxFileSizeBytes >>> 32 ^ $maxFileSizeBytes);
    result = result * PRIME + (this.isSomaticSSMsOnly() ? 79 : 97);
    result = result * PRIME + (this.isShuffle() ? 79 : 97);
    final long $seed = this.getSeed();
    result = result * PRIME + (int) ($seed >>> 32 ^ $seed);
    result = result * PRIME + this.getLimit();
    result = result * PRIME + (this.isSort() ? 79 : 97);
    result = result * PRIME + (this.isAscending() ? 79 : 97);
    final Object $storageFilename = this.getStorageFilename();
    result = result * PRIME + ($storageFilename == null ? 43 : $storageFilename.hashCode());
    result = result * PRIME + (this.isCreateNewFile() ? 79 : 97);
    final Object $specificFilenames = this.getSpecificFilenames();
    result = result * PRIME + ($specificFilenames == null ? 43 : $specificFilenames.hashCode());
    return result;
  }

  protected boolean canEqual(Object other) {
    return other instanceof FileMetaDataFetcher;
  }

  @Override
  public String toString() {
    return "org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher(numDonors=" + this.getNumDonors()
        + ", maxFileSizeBytes=" + this.getMaxFileSizeBytes() + ", somaticSSMsOnly=" + this.isSomaticSSMsOnly()
        + ", shuffle=" + this.isShuffle() + ", seed=" + this.getSeed() + ", limit=" + this.getLimit() + ", sort=" + this
            .isSort()
        + ", ascending=" + this.isAscending() + ", storageFilename=" + this.getStorageFilename()
        + ", createNewFile=" + this.isCreateNewFile() + ", specificFilenames=" + this.getSpecificFilenames() + ")";
  }

}
