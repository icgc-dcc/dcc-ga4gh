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
import static org.collaboratory.ga4gh.server.config.ServerConfig.VARIANT_TYPE_NAME;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Repository;

import ga4gh.VariantServiceOuterClass.GetVariantRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Perform queries against elasticsearch to find desired variants.
 */
@Repository
@RequiredArgsConstructor
public class VariantRepository {

  @NonNull
  private final Client client;

  private SearchRequestBuilder createSearchRequest(final int size) {
    return client.prepareSearch(INDEX_NAME)
        .setTypes(VARIANT_TYPE_NAME)
        .addSort("start", SortOrder.ASC)
        .setSize(size);
  }

  public SearchResponse findVariants(@NonNull SearchVariantsRequest request) {
    val searchRequestBuilder = createSearchRequest(request.getPageSize());
    val childBoolQuery = boolQuery().must(matchQuery("variant_set_id", request.getVariantSetId()));
    request.getCallSetIdsList().stream().forEach(x -> childBoolQuery.should(matchQuery("call_set_id", x.toString())));
    val constChildBoolQuery = constantScoreQuery(childBoolQuery);

    val boolQuery = boolQuery()
        .must(matchQuery("reference_name", request.getReferenceName()))
        .must(rangeQuery("start").gte(request.getStart()))
        .must(rangeQuery("end").lt(request.getEnd()))
        .must(hasChildQuery("call", constChildBoolQuery, ScoreMode.None).innerHit(new InnerHitBuilder()));

    val constantScoreQuery = constantScoreQuery(boolQuery);

    return searchRequestBuilder.setQuery(constantScoreQuery).get();
  }

  public SearchResponse findVariantById(@NonNull GetVariantRequest request) {
    val searchRequestBuilder = createSearchRequest(1); // only want one variant
    val childQuery = hasChildQuery("call", matchAllQuery(), ScoreMode.None)
        .innerHit(new InnerHitBuilder());
    val boolQuery = boolQuery()
        .must(idsQuery().addIds(request.getVariantId()))
        .must(childQuery);
    val constantScoreQuery = constantScoreQuery(boolQuery);
    return searchRequestBuilder.setQuery(constantScoreQuery).get();
  }

}
