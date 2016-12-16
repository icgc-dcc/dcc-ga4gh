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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static org.collaboratory.ga4gh.loader.FileMetaData.buildFileMetaDataList;
import static org.collaboratory.ga4gh.loader.FileMetaData.buildFileMetaDatasByDonor;
import static org.collaboratory.ga4gh.loader.FileMetaData.buildFileMetaDatasBySample;
import static org.collaboratory.ga4gh.loader.utils.Gullectors.immutableListCollector;
import static org.collaboratory.ga4gh.loader.utils.Gullectors.immutableSetCollector;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.Value;
import lombok.val;

@Value
public class DonorData {

  private final String id;

  private final Map<String, List<FileMetaData>> sampleDataListMap;

  public DonorData(@NonNull final String id, @NonNull final Iterable<FileMetaData> fileMetaDatas) {
    this.id = id;
    this.sampleDataListMap = buildFileMetaDatasBySample(fileMetaDatas);
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
        String.format("The sampleId \"%s\" DNE for donorId: %s", sampleId, id));
    return sampleDataListMap.get(sampleId);
  }

  /*
   * Get all the names of the callers for a specific dataType and sampleId
   */
  public final Set<String> getCallersForSampleAndDataType(final String sampleId, final String dataType) {
    return collectFileMetaSet(sampleId, dataType, x -> x.getVcfFilenameParser().toString());
  }

  /*
   * Get all the dataTypes for a specific sampleId
   */
  public final Set<String> getDataTypesForSample(final String sampleId) {
    return getSampleFileMetas(sampleId).stream()
        .map(x -> x.getDataType())
        .collect(immutableSetCollector());
  }

  public final Map<String, List<FileMetaData>> getFileMetaDatasByCallerAndDataType(final String sampleId,
      final String dataType) {
    return groupByFileMetaOnSample(sampleId, dataType, x -> x.getVcfFilenameParser().getCallerId());
  }

  public final int numFilesForSample(final String sampleId) {
    return getSampleFileMetas(sampleId).size();
  }

  private final Set<String> collectFileMetaSet(final String sampleId, final String dataType,
      final Function<? super FileMetaData, ? extends String> functor) {
    return getSampleFileMetas(sampleId).stream()
        .filter(f -> f.getDataType().equals(dataType))
        .map(functor)
        .collect(immutableSetCollector());
  }

  private final Map<String, List<FileMetaData>> groupByFileMetaOnSample(final String sampleId,
      final String dataType,
      final Function<? super FileMetaData, ? extends String> functor) {

    return getSampleFileMetas(sampleId).stream()
        .filter(x -> x.getDataType().equals(dataType))
        .collect(groupingBy(functor, immutableListCollector()));
  }

  public static DonorData createDonorData(final String donorId, final Iterable<FileMetaData> fileMetaDatas) {
    return new DonorData(donorId, fileMetaDatas);
  }

  public static final List<DonorData> buildDonorDataList(Iterable<ObjectNode> objectNodes) {
    return buildFileMetaDatasByDonor(buildFileMetaDataList(objectNodes))
        .entrySet().stream()
        .map(x -> createDonorData(x.getKey(), x.getValue())) // Key is donorId, Value is List<FileMetaData>
        .collect(immutableListCollector());
  }

  public File dumpToJson(File file) {
    val mapper = new ObjectMapper();
    try {
      mapper.writerWithDefaultPrettyPrinter().writeValue(file, this);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return file;
  }

}
