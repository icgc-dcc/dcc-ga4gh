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
package org.collaboratory.ga4gh.loader.filemetadata;

import static org.collaboratory.ga4gh.loader.Portal.getAllFileMetaDatas;
import static org.collaboratory.ga4gh.loader.Portal.getFileMetaDatasForNumDonors;
import static org.collaboratory.ga4gh.loader.filemetadata.FileMetaDataFilters.filterBySize;
import static org.collaboratory.ga4gh.loader.filemetadata.FileMetaDataFilters.filterSomaticSSMs;

import java.util.List;

import lombok.Builder;
import lombok.val;

@Builder
public class FileMetaDataFetcher {

  private int numDonors = -1;
  private long maxFileSizeBytes = -1;
  private boolean somaticSSMsOnly = false;

  /*
   * Return true if set to Non-default numDonor value
   */
  private boolean numDonorsUpdated() {
    return numDonors > -1;
  }

  /*
   * Return true if set to Non-default maxFileSizeBytes value
   */
  private boolean maxFileSizeUpdated() {
    return maxFileSizeBytes > -1;
  }

  /*
   * Fetch list of FileMetaData object, based on filter configuration
   */
  public List<FileMetaData> fetch() {
    val fileMetaDatas = numDonorsUpdated() ? getFileMetaDatasForNumDonors(numDonors) : getAllFileMetaDatas();

    // If size > 0, use only files less than or equal to maxFileSizeBytes
    val filteredFileMetaDatasBySize =
        maxFileSizeUpdated() ? filterBySize(fileMetaDatas, maxFileSizeBytes) : fileMetaDatas;

    val filteredFileMetaDatas =
        somaticSSMsOnly ? filterSomaticSSMs(filteredFileMetaDatasBySize) : filteredFileMetaDatasBySize;

    return filteredFileMetaDatas;
  }
}
