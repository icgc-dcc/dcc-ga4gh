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
package org.collaboratory.ga4gh.core.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.val;
import org.collaboratory.ga4gh.core.model.es.EsVariantSet;
import org.elasticsearch.search.SearchHit;

import java.util.Map;

import static org.collaboratory.ga4gh.core.PropertyNames.DATA_SET_ID;
import static org.collaboratory.ga4gh.core.PropertyNames.NAME;
import static org.collaboratory.ga4gh.core.PropertyNames.REFERENCE_SET_ID;
import static org.collaboratory.ga4gh.core.SearchHits.convertSourceToString;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

public class EsVariantSetConverter
    implements ObjectNodeConverter<EsVariantSet>,
    SearchHitConverter<EsVariantSet>,
    SourceConverter<EsVariantSet>{

  @Override
  public EsVariantSet convertFromSource(Map<String, Object> source) {
    val name = convertSourceToString(source, NAME);
    val dataSetId = convertSourceToString(source, DATA_SET_ID);
    val referenceSetId = convertSourceToString(source, REFERENCE_SET_ID);
    return EsVariantSet.builder()
        .name(name)
        .dataSetId(dataSetId)
        .referenceSetId(referenceSetId)
        .build();
  }

  @Override
  public EsVariantSet convertFromSearchHit(SearchHit hit) {
    return convertFromSource(hit.getSource());
  }

  @Override
  public ObjectNode convertToObjectNode(EsVariantSet variantSet) {
    return object()
        .with(NAME, variantSet.getName())
        .with(DATA_SET_ID, variantSet.getDataSetId())
        .with(REFERENCE_SET_ID, variantSet.getReferenceSetId())
        .end();
  }


}
