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

import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.server.config.ServerConfig.CALLSET_TYPE_NAME;
import static org.collaboratory.ga4gh.server.config.ServerConfig.INDEX_NAME;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Repository;

import ga4gh.VariantServiceOuterClass.GetCallSetRequest;
import ga4gh.VariantServiceOuterClass.SearchCallSetsRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Perform queries against elasticsearch to find desired variants.
 */
@Repository
@RequiredArgsConstructor
public class CallSetRepository {

  @NonNull
  private final Client client;

  private SearchRequestBuilder createSearchRequest(final int size) {
    return client.prepareSearch(INDEX_NAME)
        .setTypes(CALLSET_TYPE_NAME)
        .addSort(NAME, SortOrder.ASC)
        .setSize(size);
  }

  public SearchResponse findCallSets(@NonNull SearchCallSetsRequest request) {
    val searchRequestBuilder = createSearchRequest(request.getPageSize());
    val constBoolQuery =
        constantScoreQuery(
            boolQuery()
                .must(matchQuery(VARIANT_SET_IDS, request.getVariantSetId()))
                .must(matchQuery(NAME, request.getName()))
                .must(matchQuery(BIO_SAMPLE_ID, request.getBioSampleId())));
    return searchRequestBuilder.setQuery(constBoolQuery).get();
  }

  public GetResponse findCallSetById(@NonNull GetCallSetRequest request) {
    return client.prepareGet(INDEX_NAME, CALLSET_TYPE_NAME, request.getCallSetId()).get();
  }

}
