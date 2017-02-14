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
package org.collaboratory.ga4gh.loader.model.es.converters;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.model.es.EsVariantCallPair;
import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Builder;
import lombok.NonNull;
import lombok.val;

@Builder
public class EsVariantCallPairConverter implements
    ObjectNodeConverter<EsVariantCallPair>,
    SearchHitConverter<EsVariantCallPair> {

  private static final String CHILD_TYPE = "call";

  @NonNull
  private final SearchHitConverter<EsVariant> variantSearchHitConverter;

  @NonNull
  private final SearchHitConverter<EsCall> callSearchHitConverter;

  @NonNull
  private final ObjectNodeConverter<EsVariant> variantObjectNodeConverter;

  @NonNull
  private final ObjectNodeConverter<EsCall> callObjectNodeConverter;

  @Override
  public EsVariantCallPair convertFromSearchHit(SearchHit hit) {
    val pair = EsVariantCallPair.builder()
        .variant(variantSearchHitConverter.convertFromSearchHit(hit));

    for (val innerHit : hit.getInnerHits().get(CHILD_TYPE)) {
      pair.call(callSearchHitConverter.convertFromSearchHit(innerHit));
    }
    return pair.build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsVariantCallPair t) {
    val array = array();
    for (val call : t.getCalls()) {
      array.with(callObjectNodeConverter.convertToObjectNode(call));
    }
    return object()
        .with(variantObjectNodeConverter.convertToObjectNode(t.getVariant()))
        .with(CHILD_TYPE, array)
        .end();
  }

}
