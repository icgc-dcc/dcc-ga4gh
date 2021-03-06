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

import com.google.protobuf.Descriptors.FieldDescriptor;
import ga4gh.VariantServiceOuterClass.GetCallSetRequest;
import ga4gh.VariantServiceOuterClass.SearchCallSetsRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.sort.SortOrder;
import org.icgc.dcc.ga4gh.server.config.ServerConfig;
import org.springframework.stereotype.Repository;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.icgc.dcc.ga4gh.common.PropertyNames.BIO_SAMPLE_ID;
import static org.icgc.dcc.ga4gh.common.PropertyNames.NAME;
import static org.icgc.dcc.ga4gh.common.PropertyNames.VARIANT_SET_IDS;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.server.config.ServerConfig.DEFAULT_PAGE_SIZE;
import static org.icgc.dcc.ga4gh.server.config.ServerConfig.DEFAULT_SCROLL_TIMEOUT;

/**
 * Perform queries against elasticsearch to find desired variants.
 */
@Repository
@RequiredArgsConstructor
public class CallSetRepository {

  private static final FieldDescriptor CALLSET_PAGE_SIZE_FIELDDESCRIPTOR = SearchCallSetsRequest.getDescriptor().findFieldByNumber(SearchCallSetsRequest.PAGE_SIZE_FIELD_NUMBER);

  @NonNull
  private final Client client;

  private SearchRequestBuilder createSearchRequest(final int size) {
    return client.prepareSearch(ServerConfig.INDEX_NAME)
        .setTypes(CALL_SET)
        .addSort(NAME, SortOrder.ASC)
        .setSize(size);
  }
  private SearchRequestBuilder createScrollSearchRequest(final int size, TimeValue timeValue) {
    return createSearchRequest(size).setScroll(timeValue);
  }

  private static boolean isNewRequest(SearchCallSetsRequest request){
    return "".equals(request.getPageToken());
  }

  public SearchResponse findCallSets(@NonNull SearchCallSetsRequest request) {
    val pageSize = resolvePageSize(request);
    if(isNewRequest(request)){
      val searchRequestBuilder = createScrollSearchRequest(pageSize, DEFAULT_SCROLL_TIMEOUT);
      val query = boolQuery()
          .filter(
              boolQuery()
                  .must(matchQuery(VARIANT_SET_IDS, request.getVariantSetId()))
                  .must(matchQuery(NAME, request.getName()))
                  .must(matchQuery(BIO_SAMPLE_ID, request.getBioSampleId())));
      return searchRequestBuilder.setQuery(query).get();
    } else {
      return client.prepareSearchScroll(request.getPageToken()).setScroll(DEFAULT_SCROLL_TIMEOUT).get();
    }
  }

  public GetResponse findCallSetById(@NonNull GetCallSetRequest request) {
    return client.prepareGet(ServerConfig.INDEX_NAME, CALL_SET, request.getCallSetId()).get();
  }

  private static int resolvePageSize(SearchCallSetsRequest request){
    return request.hasField(CALLSET_PAGE_SIZE_FIELDDESCRIPTOR) ? request.getPageSize() : DEFAULT_PAGE_SIZE;
  }

}
