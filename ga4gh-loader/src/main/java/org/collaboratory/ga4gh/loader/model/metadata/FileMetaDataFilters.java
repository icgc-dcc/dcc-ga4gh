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

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.util.Collections;
import java.util.List;

import org.collaboratory.ga4gh.loader.PortalVCFFilenameParser;
import org.collaboratory.ga4gh.loader.model.contexts.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.vcf.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.vcf.enums.SubMutationTypes;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

/*
 * Functions for filtering lists of FileMetaDatas
 */
@NoArgsConstructor(access = PRIVATE)
public class FileMetaDataFilters {

  /*
   * Filters input list of FileMetaDatas to be less than specified size
   */
  public static FileMetaDataContext filterBySize(@NonNull FileMetaDataContext fileMetaDataContext,
      final long maxSizeBytes) {
    return fileMetaDataContext.filter(f -> f.getFileSize() < maxSizeBytes);
  }

  /*
   * Filters input list of FileMetaDatas by MutationType==somatic, SubMutationType==(snv_mnv or indel)
   */
  public static FileMetaDataContext filterSomaticSSMs(@NonNull FileMetaDataContext fileMetaDataContext) {
    return fileMetaDataContext.filter(f -> isSomaticSSM(f));
  }

  /*
   * Checks if FileMetaData is classified as somatic and indel or snv_mnv
   */
  public static boolean isSomaticSSM(final FileMetaData f) {
    return f.compare(MutationTypes.somatic)
        && (f.compare(SubMutationTypes.indel)
            || f.compare(SubMutationTypes.snv_mnv));
  }

  public static FileMetaDataContext filterSelectedFilenamesInOrder(@NonNull FileMetaDataContext fileMetaDataContext,
      @NonNull List<String> filenames) {
    // sort filemetadatas based on filename
    // create list of parsers from that sorted list
    // biinary search, and if index found, retrieve from first list
    val fileMetaDataContextSorted = fileMetaDataContext.sortByFilename(false);
    val parsers = stream(fileMetaDataContextSorted)
        .map(f -> f.getVcfFilenameParser())
        .collect(toImmutableList());
    val contextBuilder = FileMetaDataContext.builder();
    val fileMetaDataList = fileMetaDataContextSorted.getFileMetaDatas();
    for (val filename : filenames) {
      val parserKey = new PortalVCFFilenameParser(filename);
      val index = Collections.binarySearch(parsers, parserKey);
      if (index >= 0) {
        contextBuilder.fileMetaData(fileMetaDataList.get(index));
      }
    }
    return contextBuilder.build();
  }
}
