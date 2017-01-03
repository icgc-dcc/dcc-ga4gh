/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.summingInt;
import static org.collaboratory.ga4gh.loader.FileMetaData.buildFileMetaDataList;
import static org.collaboratory.ga4gh.loader.FileMetaData.groupFileMetaDataBySample;
import static org.collaboratory.ga4gh.loader.FileMetaData.groupFileMetaDatasByDonor;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.Value;

@Value
public class DonorData {

  private final String id;

  private final Map<String, List<FileMetaData>> sampleDataListMap;

  public DonorData(@NonNull final String id, @NonNull final Iterable<FileMetaData> fileMetaDatas) {
    this.id = id;
    this.sampleDataListMap = groupFileMetaDataBySample(fileMetaDatas);
  }

  public final Set<String> getSampleIds() {
    return sampleDataListMap.keySet();
  }

  public final int getNumSamples() {
    return getSampleIds().size();
  }

  public final long getTotalFileMetaCount() {
    return getSampleIds().stream()
        .map(this::numFilesForSample)
        .collect(summingInt(Integer::intValue));
  }

  /*
   * Get the list of FileMetaData for a particular sampleId
   */
  public final List<FileMetaData> getSampleFileMetas(final String sampleId) {
    checkState(sampleDataListMap.containsKey(sampleId),
        "The sampleId \"%s\" DNE for donorId: %s", sampleId, id);
    return sampleDataListMap.get(sampleId);
  }

  public final int numFilesForSample(final String sampleId) {
    return getSampleFileMetas(sampleId).size();
  }

  public static DonorData createDonorData(final String donorId, final Iterable<FileMetaData> fileMetaDatas) {
    return new DonorData(donorId, fileMetaDatas);
  }

  public static final List<DonorData> buildDonorDataList(Iterable<ObjectNode> objectNodes) {
    return groupFileMetaDatasByDonor(buildFileMetaDataList(objectNodes))
        .entrySet().stream()
        .map(x -> createDonorData(x.getKey(), x.getValue())) // Key is donorId, Value is List<FileMetaData>
        .collect(toImmutableList());
  }

}
