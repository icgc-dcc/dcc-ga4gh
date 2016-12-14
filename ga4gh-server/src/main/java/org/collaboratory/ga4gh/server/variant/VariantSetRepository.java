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
package org.collaboratory.ga4gh.server.variant;

import static org.collaboratory.ga4gh.server.config.ServerConfig.INDEX_NAME;
import static org.collaboratory.ga4gh.server.config.ServerConfig.NODE_ADDRESS;
import static org.collaboratory.ga4gh.server.config.ServerConfig.NODE_PORT;
import static org.collaboratory.ga4gh.server.config.ServerConfig.VARIANT_SET_TYPE_NAME;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.net.InetAddress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.stereotype.Repository;

import ga4gh.MetadataServiceOuterClass.SearchDatasetsRequest;
import ga4gh.VariantServiceOuterClass.GetVariantSetRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantSetsRequest;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Perform queries against elasticsearch to find desired variants.
 */
@Repository
public class VariantSetRepository {

  @NonNull
  private final TransportClient client;

  // TODO: rtisma -- put TransportClient construction into commmon module, this is also applies to the loader
  @SuppressWarnings("resource")
  @SneakyThrows
  public VariantSetRepository() {
    this.client = new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(NODE_ADDRESS), NODE_PORT));
  }

  private SearchRequestBuilder createSearchRequest(final int size) {
    return SearchAction.INSTANCE.newRequestBuilder(client)
        .setIndices(INDEX_NAME)
        .setTypes(VARIANT_SET_TYPE_NAME)
        .addSort("data_set_id", SortOrder.ASC)
        .setSize(size);
  }

  public SearchResponse searchAllDataSets(@NonNull SearchDatasetsRequest request) {
    val searchRequestBuilder = createSearchRequest(request.getPageSize());
    val constantBoolQuery = constantScoreQuery(
        boolQuery()
            .must(
                QueryBuilders.matchAllQuery()));

    val agg = AggregationBuilders.terms("by_data_set_id").field("data_set_id");
    return searchRequestBuilder.setQuery(constantBoolQuery).addAggregation(agg).get();
  }

  public SearchResponse findVariantSets(@NonNull SearchVariantSetsRequest request) {
    val searchRequestBuilder = createSearchRequest(request.getPageSize());
    val constantBoolQuery = constantScoreQuery(
        boolQuery()
            .must(
                matchQuery("data_set_id", request.getDatasetId())));
    return searchRequestBuilder.setQuery(constantBoolQuery).get();
  }

  public GetResponse findVariantSetById(@NonNull GetVariantSetRequest request) {
    return client.prepareGet(INDEX_NAME, VARIANT_SET_TYPE_NAME, request.getVariantSetId()).get();
  }

}
