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
package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.SearchHits;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair;

import java.util.Map;
import java.util.Set;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALLS;

@Builder
@RequiredArgsConstructor
public class EsVariantCallPairConverterJson implements
    JsonObjectNodeConverter<EsVariantCallPair>,
    SearchHitConverter<EsVariantCallPair> {

    private static final String NESTED_TYPE = CALLS;


  @NonNull
  private final SearchHitConverter<EsVariant> variantSearchHitConverter;

  @NonNull
  private final SearchHitConverter<EsConsensusCall> callSearchHitConverter;

  @NonNull
  private final JsonObjectNodeConverter<EsVariant> variantJsonObjectNodeConverter;

  @NonNull
  private final JsonObjectNodeConverter<EsConsensusCall> callJsonObjectNodeConverter;

  /**
   * Converts ALL calls from the source to a EsVariantCallPair
   */
  @Override
  public EsVariantCallPair convertFromSource(Map<String, Object> source) {
    val calls = SearchHits.convertSourceToObjectList(source, NESTED_TYPE);

    val pair = EsVariantCallPair.builder()
        .variant(variantSearchHitConverter.convertFromSource(source));

    calls.stream()
        .map(x -> (Map<String, Object>)x)
        .map(callSearchHitConverter::convertFromSource)
        .forEach(pair::call);

    return pair.build();
  }

  /**
   * Only converts calls that have the specified allowedCallSetIds, and constructs a EsVariantCallPair
   */
  public EsVariantCallPair convertFromSource(Map<String, Object> source, Set<String> allowedCallsetIds) {
    val calls = SearchHits.convertSourceToObjectList(source, NESTED_TYPE);

    val pair = EsVariantCallPair.builder()
        .variant(variantSearchHitConverter.convertFromSource(source));

    calls.stream()
        .map(x -> (Map<String, Object>)x)
        .filter(x -> sourceHasCallSet(x, allowedCallsetIds)) //Filtering on server side
        .map(callSearchHitConverter::convertFromSource)
        .forEach(pair::call);

    return pair.build();
  }

  private static boolean sourceHasCallSet(Map<String, Object> source, Set<String> allowedCallsetIds){
    if (allowedCallsetIds.isEmpty()){
      return false;
    }

    val callSetId = SearchHits.convertSourceToString(source, PropertyNames.CALL_SET_ID);
    return allowedCallsetIds.contains(callSetId);
  }


  @Override
  public ObjectNode convertToObjectNode(EsVariantCallPair t) {
    val array = array();
    for (val call : t.getCalls()) {
      array.with(callJsonObjectNodeConverter.convertToObjectNode(call));
    }
    return object()
        .with(variantJsonObjectNodeConverter.convertToObjectNode(t.getVariant()))
        .with(NESTED_TYPE, array)
        .end();
  }

}
