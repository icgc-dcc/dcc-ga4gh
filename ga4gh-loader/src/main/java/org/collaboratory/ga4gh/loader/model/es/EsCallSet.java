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
package org.collaboratory.ga4gh.loader.model.es;

import static org.collaboratory.ga4gh.core.Names.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.core.Names.NAME;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToString;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToStringList;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertStrings;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.val;

// ObjectNode is a bit heavy, this is just to minimize memory usage
@Builder
@Value
public final class EsCallSet implements EsModel {

  private String name;
  private String bioSampleId;

  @Singular
  private Iterable<String> variantSetIds;

  @Override
  public ObjectNode toDocument() {
    return object()
        .with(NAME, name)
        .with(VARIANT_SET_IDS, convertStrings(variantSetIds))
        .with(BIO_SAMPLE_ID, bioSampleId)
        .end();
  }

  public static SpecialEsCallSetBuilder builder() {
    return new SpecialEsCallSetBuilder();
  }

  public static class SpecialEsCallSetBuilder extends EsCallSetBuilder {

    public SpecialEsCallSetBuilder fromSearchHit(final SearchHit hit) {
      val name = convertHitToString(hit, NAME);
      val bioSampleId = convertHitToString(hit, BIO_SAMPLE_ID);
      val variantSetIds = convertHitToStringList(hit, VARIANT_SET_IDS);
      return (SpecialEsCallSetBuilder) name(name)
          .bioSampleId(bioSampleId)
          .variantSetIds(variantSetIds);
    }

  }
}
