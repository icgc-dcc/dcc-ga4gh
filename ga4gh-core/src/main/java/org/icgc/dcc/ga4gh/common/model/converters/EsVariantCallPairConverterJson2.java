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
import org.elasticsearch.search.SearchHit;
import org.icgc.dcc.ga4gh.common.TypeNames;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@Builder
@RequiredArgsConstructor
public class EsVariantCallPairConverterJson2 implements
    JsonObjectNodeConverter<EsVariantCallPair2>,
    SearchHitConverter<EsVariantCallPair2> {

    private static final String NESTED_TYPE = TypeNames.CALLS;


  @NonNull
  private final SearchHitConverter<EsVariant> variantSearchHitConverter;

  @NonNull
  private final SearchHitConverter<EsCall> callSearchHitConverter;

  @NonNull
  private final JsonObjectNodeConverter<EsVariant> variantJsonObjectNodeConverter;

  @NonNull
  private final JsonObjectNodeConverter<EsCall> callJsonObjectNodeConverter;

  @Override
  public EsVariantCallPair2 convertFromSearchHit(SearchHit hit) {
    val pair = EsVariantCallPair2.builder()
        .variant(variantSearchHitConverter.convertFromSearchHit(hit));

    for (val innerHit : hit.getInnerHits().get(NESTED_TYPE)) {
      pair.call(callSearchHitConverter.convertFromSearchHit(innerHit));
    }
    return pair.build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsVariantCallPair2 t) {
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
