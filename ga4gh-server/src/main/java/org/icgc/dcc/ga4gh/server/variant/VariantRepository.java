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

import ga4gh.VariantServiceOuterClass.GetVariantRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Repository;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.icgc.dcc.common.core.util.Joiners.DOT;
import static org.icgc.dcc.ga4gh.common.PropertyNames.CALL_SET_ID;
import static org.icgc.dcc.ga4gh.common.PropertyNames.END;
import static org.icgc.dcc.ga4gh.common.PropertyNames.REFERENCE_NAME;
import static org.icgc.dcc.ga4gh.common.PropertyNames.START;
import static org.icgc.dcc.ga4gh.common.PropertyNames.VARIANT_SET_ID;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALLS;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.server.config.ServerConfig.INDEX_NAME;

/**
 * Perform queries against elasticsearch to find desired variants.
 */
@Repository
@RequiredArgsConstructor
public class VariantRepository {

  @NonNull
  private final Client client;

  private static final String NESTED_TYPE = CALLS;
  private SearchRequestBuilder createSearchRequest(final int size) {
    return client.prepareSearch(INDEX_NAME)
        .setTypes(VARIANT)
        .addSort(START, SortOrder.ASC)
        .setSize(size);
  }

  private static String getNestedFieldName(String fieldName){
    return DOT.join(NESTED_TYPE, fieldName);
  }

  public SearchResponse findVariants(@NonNull SearchVariantsRequest request) {
    val searchRequestBuilder = createSearchRequest(request.getPageSize());
    val childBoolQuery = boolQuery().must(matchQuery(getNestedFieldName(VARIANT_SET_ID), request.getVariantSetId()));
    request.getCallSetIdsList().forEach(id -> childBoolQuery.should(matchQuery(getNestedFieldName(CALL_SET_ID), id)));
    val constChildBoolQuery = constantScoreQuery(childBoolQuery);

    val boolQuery = boolQuery()
        .must(matchQuery(REFERENCE_NAME, request.getReferenceName()))
        .must(rangeQuery(START).gte(request.getStart()))
        .must(rangeQuery(END).lt(request.getEnd()))
        .must(nestedQuery(NESTED_TYPE,constChildBoolQuery,ScoreMode.None));
    val constantScoreQuery = constantScoreQuery(boolQuery);

    return searchRequestBuilder.setQuery(constantScoreQuery).get();
  }

  public GetResponse findVariantById(@NonNull GetVariantRequest request) {
    return client.prepareGet(INDEX_NAME, VARIANT, request.getVariantId()).get();
  }

}
