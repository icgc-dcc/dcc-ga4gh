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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@RequiredArgsConstructor
@Value
public class DonorData {

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

  public List<FileMetaData> getSampleFileMetas(String sampleId) {
    Preconditions.checkState(this.sampleDataListMap.containsKey(sampleId),
        "The sampleId \"" + sampleId + "\" DNE for donorId: " + this.id);
    return sampleDataListMap.get(sampleId);
  }

  public Map<String, List<FileMetaData>> getSampleFileMetaDataByDataType(String sampleId) {
    return groupByFileMetaOnSample(sampleId, x -> x.getDataType());
  }

  public Set<String> getCallersForSample(String sampleId) {
    return collectFileMetaSet(sampleId, x -> x.getVcfFilenameParser().toString());
  }

  public Set<String> getDataTypesForSample(String sampleId) {
    return collectFileMetaSet(sampleId, x -> x.getDataType());
  }

  private Set<String> collectFileMetaSet(String sampleId, Function<? super FileMetaData, ? extends String> functor) {
    return getSampleFileMetas(sampleId).stream().map(functor)
        .collect(Collectors.toSet());
  }

  private Map<String, List<FileMetaData>> groupByFileMetaOnSample(String sampleId,
      Function<? super FileMetaData, ? extends String> functor) {
    return getSampleFileMetas(sampleId).stream()
        .collect(Collectors.groupingBy(functor, Collectors.toList()));
  }

  public Map<String, List<FileMetaData>> getSampleFileMetaDataByCaller(String sampleId) {
    return groupByFileMetaOnSample(sampleId, x -> x.getVcfFilenameParser().getCallerId());
  }

  public int numFilesForSample(String sampleId) {
    return getSampleFileMetas(sampleId).size();
  }

}
