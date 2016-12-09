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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.val;

@RequiredArgsConstructor
@Value
public class DonorData {

  @RequiredArgsConstructor
  @Value
  public static class Tuple2<T1, T2> {

    @NonNull
    private final T1 t1;
    @NonNull
    private final T2 t2;

  }

  @NonNull
  private final String id;

  private final Map<String, List<FileMetaData>> sampleDataListMap = new HashMap<>();

  public void addSample(String sampleId, @NonNull FileMetaData fileMetaData) {
    if (sampleDataListMap.containsKey(sampleId) == false) {
      sampleDataListMap.put(sampleId, new ArrayList<FileMetaData>());
    }
    sampleDataListMap.get(sampleId).add(fileMetaData);
  }

  public Set<String> getSampleSet() {
    return sampleDataListMap.keySet();
  }

  public int getNumSamples() {
    return getSampleSet().size();
  }

  public long getTotalFileMetaCount() {
    return getSampleSet().stream()
        .map(this::numFilesForSample)
        .collect(Collectors.summingInt(Integer::intValue));
  }

  public List<FileMetaData> getSampleFileMetas(String sampleId) {
    checkState(sampleDataListMap.containsKey(sampleId),
        String.format("The sampleId \"%s\" DNE for donorId: %s", sampleId, id));
    return sampleDataListMap.get(sampleId);
  }

  public Set<String> getCallersForSampleAndDataType(final String sampleId, final String dataType) {
    return collectFileMetaSet(sampleId, dataType, x -> x.getVcfFilenameParser().toString());
  }

  public Set<String> getDataTypesForSample(final String sampleId) {
    return getSampleFileMetas(sampleId).stream()
        .map(x -> x.getDataType())
        .collect(Collectors.toSet());
  }

  public Map<String, List<FileMetaData>> getFileMetaDatasByCallerAndDataType(final String sampleId,
      final String dataType) {
    return groupByFileMetaOnSample(sampleId, dataType, x -> x.getVcfFilenameParser().getCallerId());
  }

  public int numFilesForSample(String sampleId) {
    return getSampleFileMetas(sampleId).size();
  }

  private Set<String> collectFileMetaSet(final String sampleId, final String dataType,
      final Function<? super FileMetaData, ? extends String> functor) {
    return getSampleFileMetas(sampleId).stream()
        .filter(f -> f.getDataType().equals(dataType))
        .map(functor)
        .collect(Collectors.toSet());
  }

  private Map<String, List<FileMetaData>> groupByFileMetaOnSample(final String sampleId, final String dataType,
      Function<? super FileMetaData, ? extends String> functor) {
    return getSampleFileMetas(sampleId).stream()
        .filter(x -> x.getDataType().equals(dataType))
        .collect(Collectors.groupingBy(functor, Collectors.toList()));
  }

  public static Map<String, DonorData> buildDonorDataMap(Iterable<ObjectNode> objectNodes) {
    val map = new HashMap<String, DonorData>();

    // Map< List(donorId, sampleId), List<FileMetaData>>
    Map<Tuple2<String, String>, List<FileMetaData>> tempMap =
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

  public static void writeDonorDataMap(final String outputFn, @NonNull Iterable<ObjectNode> objectNodes) {
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

}
