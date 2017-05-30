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
package org.icgc.dcc.ga4gh.server.variant;

import ga4gh.MetadataServiceOuterClass.SearchDatasetsRequest;
import ga4gh.VariantServiceOuterClass.GetVariantSetRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantSetsRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.icgc.dcc.ga4gh.server.config.ServerConfig;
import org.springframework.stereotype.Repository;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.icgc.dcc.ga4gh.common.PropertyNames.DATA_SET_ID;
import static org.icgc.dcc.ga4gh.common.PropertyNames.getAggNameForProperty;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.server.config.ServerConfig.DEFAULT_SCROLL_TIMEOUT;

/**
 * Perform queries against elasticsearch to find desired variants.
 */
@Repository
@RequiredArgsConstructor
public class VariantSetRepository {

  public static final String BY_DATA_SET_ID = getAggNameForProperty(DATA_SET_ID);

  @NonNull
  private final Client client;

  /** NOTE: No scrolling done here, since number of dataSets assumed to be small.
   * Instead, just aggregating all variantSets by their dataSetName to get all the dataSets.
   * If number of datasets is large or number of variantSets is large,
   * then might have to create a mapping for dataSets in the index
   */
  public SearchResponse findAllDataSets(@NonNull SearchDatasetsRequest request) {
    val searchRequestBuilder = createSearchRequest(request.getPageSize());
    val boolQuery = boolQuery()
        .must(
            matchAllQuery());
    val agg = AggregationBuilders
        .terms(BY_DATA_SET_ID)
        .field(DATA_SET_ID);
    return searchRequestBuilder
        .setQuery(boolQuery)
        .addAggregation(agg)
        .get();
  }

  public SearchResponse findVariantSets(@NonNull SearchVariantSetsRequest request) {
    if (isNewRequest(request)){
      val searchRequestBuilder = createScrollSearchRequest(request.getPageSize(), DEFAULT_SCROLL_TIMEOUT);
      val boolQuery = boolQuery()
              .must(
                  matchQuery(DATA_SET_ID, request.getDatasetId()));
      val boolFilterboolQuery = boolQuery().filter(boolQuery);
      return searchRequestBuilder.setQuery(boolFilterboolQuery).get();
    } else {
      return client.prepareSearchScroll(request.getPageToken()).setScroll(DEFAULT_SCROLL_TIMEOUT).get();
    }
  }

  public GetResponse findVariantSetById(@NonNull GetVariantSetRequest request) {
    return client.prepareGet(ServerConfig.INDEX_NAME, VARIANT_SET, request.getVariantSetId()).get();
  }

  private SearchRequestBuilder createSearchRequest(final int size) {
    return client.prepareSearch(ServerConfig.INDEX_NAME)
        .setTypes(VARIANT_SET)
        .addSort(DATA_SET_ID, SortOrder.ASC)
        .setSize(size);
  }

  private SearchRequestBuilder createScrollSearchRequest(final int size, TimeValue timeValue) {
    return createSearchRequest(size).setScroll(timeValue);
  }


  private static boolean isNewRequest(SearchVariantSetsRequest searchVariantsRequest){
    return "".equals(searchVariantsRequest.getPageToken());
  }


}
