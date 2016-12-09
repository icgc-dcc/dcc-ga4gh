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
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticsearch.shaded.jackson.core.JsonGenerationException;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.val;

@RequiredArgsConstructor
@Value
@NotThreadSafe
public class DonorData {

  @RequiredArgsConstructor
  @Value
  public static final class Tuple2<T1, T2> {

    @NonNull
    private final T1 t1;
    @NonNull
    private final T2 t2;

  }

  @NonNull
  private final String id;

  private final Map<String, List<FileMetaData>> sampleDataListMap = new HashMap<>();

  /*
   * Adds fileMetaData to this Donor
   */
  public final void addSample(@NonNull final FileMetaData fileMetaData) {
    addSample(fileMetaData.getSampleId(), fileMetaData);
  }

  public final void addSample(final String sampleId, @NonNull final FileMetaData fileMetaData) {
    if (sampleDataListMap.containsKey(sampleId) == false) {
      sampleDataListMap.put(sampleId, new ArrayList<FileMetaData>());
    }
    sampleDataListMap.get(sampleId).add(fileMetaData);
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
        .collect(Collectors.summingInt(Integer::intValue));
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
        .collect(Collectors.toSet());
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
        .collect(Collectors.toSet());
  }

  private final Map<String, List<FileMetaData>> groupByFileMetaOnSample(final String sampleId, final String dataType,
      final Function<? super FileMetaData, ? extends String> functor) {
    return getSampleFileMetas(sampleId).stream()
        .filter(x -> x.getDataType().equals(dataType))
        .collect(Collectors.groupingBy(functor, Collectors.toList()));
  }

  /*
   * Builds a Map where keys are donorIds and Values are DonorData objects, by converting each input objectNode to a
   * FileMetaData object, and grouping them by donorId and sampleId. Then traversing the grouped structure, and
   * populating the Map appropriately
   */
  public static final Map<String, DonorData> buildDonorDataMap(Iterable<ObjectNode> objectNodes) {
    val map = new HashMap<String, DonorData>();

    // Map< List(donorId, sampleId), List<FileMetaData>>
    final Map<Tuple2<String, String>, List<FileMetaData>> tempMap =
        stream(objectNodes)
            .map(o -> FileMetaData.build(o))
            .collect(Collectors.toList()) // List<FileMetaData>
            .stream()
            .collect(
                Collectors.groupingBy(
                    x -> new Tuple2<String, String>(x.getDonorId(), x.getSampleId()))); // Group by Tuple2 of donorId
                                                                                        // and sampleId
    // Initialize output map with DonorData objects and keys
    tempMap.keySet().stream().forEach(k -> map.put(k.getT1(), new DonorData(k.getT1())));

    // Add FileMetaData
    tempMap.entrySet().stream().forEach(e -> e.getValue().stream().forEach(
        f -> map.get(e.getKey().getT1()).addSample(e.getKey().getT2(), f)));
    return map;
  }

  public static final void writeDonorDataMap(final String outputFn, @NonNull Iterable<ObjectNode> objectNodes) {
    val sb = new StringBuilder();
    buildDonorDataMap(objectNodes)
        .values().stream()
        .forEach(x -> x.getSampleDataListMap().entrySet().stream()
            .forEach(e -> sb.append("donorId: ")
                .append(x.getId())
                .append("\nsampleId: ")
                .append(e.getKey())
                .append(" -> [\n\t\t")
                .append(e.getValue().stream()
                    .map(s -> s.toString())
                    .collect(Collectors.joining("\n\t\t")))
                .append("\n]\n\n")));

    Benchmarks.writeToNewFile(outputFn, sb.toString());
  }

  public File dumpToJson(File file) {
    val mapper = new ObjectMapper();
    try {
      mapper.writerWithDefaultPrettyPrinter().writeValue(file, this);
    } catch (JsonGenerationException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return file;
  }

}
