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
package org.collaboratory.ga4gh.loader.metadata;

import static com.google.common.base.Preconditions.checkState;
import static org.collaboratory.ga4gh.loader.Portal.getAllFileMetaDatas;
import static org.collaboratory.ga4gh.loader.Portal.getFileMetaDatasForNumDonors;
import static org.collaboratory.ga4gh.loader.metadata.FileMetaDataFilters.filterBySize;
import static org.collaboratory.ga4gh.loader.metadata.FileMetaDataFilters.filterSomaticSSMs;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import lombok.Builder;
import lombok.Data;
import lombok.val;

@Data
@Builder
public class FileMetaDataFetcher {

  public static final int DEFAULT_NUM_DONORS = 0;
  public static final int DEFAULT_MAX_FILESIZE_BYTES = 0;
  public static final int DEFAULT_LIMIT_NUM_FILES = 0;

  private final int numDonors;
  private final long maxFileSizeBytes;
  private final boolean somaticSSMsOnly;
  private final boolean shuffle;
  private final long seed;
  private final int limit;
  private final boolean sort;
  private final boolean ascending;

  public static final long generateSeed() {
    return System.currentTimeMillis();
  }

  // Define default builder
  public static FileMetaDataFetcherBuilder builder() {
    return new FileMetaDataFetcherBuilder()
        .maxFileSizeBytes(DEFAULT_MAX_FILESIZE_BYTES)
        .numDonors(DEFAULT_NUM_DONORS)
        .somaticSSMsOnly(false)
        .seed(generateSeed())
        .limit(DEFAULT_LIMIT_NUM_FILES)
        .sort(false)
        .ascending(false)
        .shuffle(false);
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

  }

  /*
   * Fetch list of FileMetaData object, based on filter configuration
   */
  public List<FileMetaData> fetch() {
    checkConfig();

    val fileMetaDatas = numDonorsUpdated() ? getFileMetaDatasForNumDonors(numDonors) : getAllFileMetaDatas();

    // If size > 0, use only files less than or equal to maxFileSizeBytes
    val filteredFileMetaDatasBySize =
        maxFileSizeUpdated() ? filterBySize(fileMetaDatas, maxFileSizeBytes) : fileMetaDatas;

    val filteredFileMetaDatas =
        somaticSSMsOnly ? filterSomaticSSMs(filteredFileMetaDatasBySize) : filteredFileMetaDatasBySize;

    List<FileMetaData> outputList = filteredFileMetaDatas;
    if (shuffle) {
      val rand = new Random();
      rand.setSeed(seed);
      val list = Lists.newArrayList(filteredFileMetaDatas);
      Collections.shuffle(list, rand);
      outputList = ImmutableList.copyOf(list);
    } else if (sort) {
      outputList = FileMetaData.sortByFileSize(filteredFileMetaDatas, ascending);
    }
    return limitNumFilesUpdated() ? outputList.subList(0, Math.min(limit, outputList.size())) : outputList;
  }
}
