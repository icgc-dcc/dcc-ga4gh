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

import static org.collaboratory.ga4gh.server.config.ServerConfig.NODE_ADDRESS;
import static org.collaboratory.ga4gh.server.config.ServerConfig.NODE_PORT;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.stereotype.Repository;

import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Perform queries against elasticsearch to find desired variants.
 */
@Repository
public class CallsetRepository {

  @NonNull
  private final TransportClient client;

  // TODO: rtisma -- put TransportClient construction into commmon module, this is also applies to the loader
  @SuppressWarnings("resource")
  @SneakyThrows
  public CallsetRepository() {
    this.client = new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(NODE_ADDRESS), NODE_PORT));
  }

  public SearchResponse findVariants(@NonNull SearchVariantsRequest request) {
    val searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(client)
        .setIndices("dcc-variants")
        .setTypes("variant")
        .addSort("start", SortOrder.ASC)
        .setSize(request.getPageSize());

    val boolQuery = boolQuery()
        .must(matchQuery("variant_set_id", request.getVariantSetId()))
        .must(matchQuery("reference_name", request.getReferenceName()))
        .must(rangeQuery("start").gte(request.getStart()))
        .must(rangeQuery("end").lt(request.getEnd()));
    for (String callSetId : request.getCallSetIdsList()) {
      boolQuery.should(matchQuery("call_set_id", callSetId));
    }

    val constantScoreQuery = constantScoreQuery(boolQuery);

    return searchRequestBuilder.setQuery(constantScoreQuery).get();
  }

}
