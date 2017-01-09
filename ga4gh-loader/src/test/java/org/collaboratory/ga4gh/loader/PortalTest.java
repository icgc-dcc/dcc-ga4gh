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
package org.collaboratory.ga4gh.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.collaboratory.ga4gh.loader.Portal.getFileMetaDatasForNumDonors;

import java.util.List;
import java.util.Set;

import org.collaboratory.ga4gh.loader.enums.CallerTypes;
import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;
import org.collaboratory.ga4gh.loader.filemetadata.FileMetaData;
import org.collaboratory.ga4gh.loader.filemetadata.FileMetaDataFilters;
import org.icgc.dcc.common.core.util.Joiners;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PortalTest {

  private static List<String> filenameGenerator(final CallerTypes caller, final MutationTypes mutation,
      final SubMutationTypes subMutation) {
    val builder = ImmutableList.<String> builder();
    val objectId = "myObjectId";
    val dateId = "myDataId";
    val ext = "vcf.gz";
    builder.add(Joiners.DOT.join(objectId, caller, dateId, mutation, subMutation, ext));
    builder.add(Joiners.DOT.join(objectId, caller + "_some_extra", dateId, mutation, subMutation, ext));
    builder.add(Joiners.DOT.join(objectId, "some_extra_" + caller, dateId, mutation, subMutation, ext));
    return builder.build();
  }

  @Test
  public void testFileMetaDataFilter() {
    int numDonors = 20;
    val fileMetaDatas = getFileMetaDatasForNumDonors(numDonors);
    int maxFileSizeBytes = 700000;

    // If size > 0, use only files less than or equal to maxFileSizeBytes
    // val filteredFileMetaDatasBySize =
    // (maxFileSizeBytes < 0) ? fileMetaDatas : filterBySize(fileMetaDatas, maxFileSizeBytes);
    // val filteredFileMetaDatas = filterSomaticSSMs(filteredFileMetaDatasBySize);

  }

  private static Set<FileMetaData> expectedFilterSomaticSSMs(final Iterable<FileMetaData> fileMetaDatas) {
    val builder = ImmutableSet.<FileMetaData> builder();
    for (val fileMetaData : fileMetaDatas) {
      if (FileMetaDataFilters.isSomaticSSM(fileMetaData)) {
        builder.add(fileMetaData);
      }
    }
    return builder.build();
  }

  private static boolean isExpectedSomaticSSMClassification(final MutationTypes mutation,
      final SubMutationTypes subMutation) {
    return mutation == MutationTypes.somatic
        && (subMutation == SubMutationTypes.indel || subMutation == SubMutationTypes.snv_mnv);
  }

  private static FileMetaData createDummyFileMetaDataForParserAndSize(final PortalVCFFilenameParser parser,
      final long size) {
    return new FileMetaData("", "", "", "", "", "", "", size, "", parser);
  }

  @Test
  public void testSomaticSSMLogic() {
    for (val caller : CallerTypes.values()) {
      for (val mutation : MutationTypes.values()) {
        for (val subMutation : SubMutationTypes.values()) {
          val isExpectedSomaticSSM = isExpectedSomaticSSMClassification(mutation, subMutation);
          for (val filename : filenameGenerator(caller, mutation, subMutation)) {
            val parser = new PortalVCFFilenameParser(filename);
            val fileMetaData = createDummyFileMetaDataForParserAndSize(parser, 700000);
            val isActualSomaticSSM = FileMetaDataFilters.isSomaticSSM(fileMetaData);
            log.info("Tesing Filename : {}", filename);
            assertThat(isExpectedSomaticSSM == isActualSomaticSSM);
          }
        }
      }
    }

  }

}
