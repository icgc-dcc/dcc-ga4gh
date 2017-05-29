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
package org.icgc.dcc.ga4gh.server.variant;

import ga4gh.Metadata.Dataset;
import ga4gh.MetadataServiceOuterClass.SearchDatasetsRequest;
import ga4gh.MetadataServiceOuterClass.SearchDatasetsResponse;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class MetadataService {

  @NonNull
  private final VariantSetRepository variantSetRepository;

  public SearchDatasetsResponse searchDatasets(@NonNull SearchDatasetsRequest request) {
    val response = variantSetRepository.findAllDataSets(request);
    return buildSearchDatasetsResponse(response);
  }

  private SearchDatasetsResponse buildSearchDatasetsResponse(@NonNull SearchResponse searchResponse) {
    val datasets = (Terms) searchResponse.getAggregations().get(VariantSetRepository.BY_DATA_SET_ID);
    val buckets = datasets.getBuckets();
    val hasBuckets = buckets.size() > 0;
    if (hasBuckets) {
      log.info("Datasets");
      return SearchDatasetsResponse.newBuilder()
          .addAllDatasets(datasets.getBuckets().stream()
              .map(b -> Dataset.newBuilder()
                  .setId(b.getKey().toString())
                  .setName(b.getKey().toString())
                  .build())
              .collect(toImmutableList()))
          .build();
    } else {
      return SearchDatasetsResponse.newBuilder().build();
    }
  }

}
