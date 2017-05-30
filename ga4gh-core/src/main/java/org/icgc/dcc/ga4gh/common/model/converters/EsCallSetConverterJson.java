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
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;

import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.common.JsonNodeConverters.convertIntegers;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToIntegerSet;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToString;

@RequiredArgsConstructor
public class EsCallSetConverterJson
    implements JsonObjectNodeConverter<EsCallSet>,
    SearchHitConverter<EsCallSet> {

  @Override
  public EsCallSet convertFromSource(Map<String, Object> source) {
    val name = convertSourceToString(source, PropertyNames.NAME);
    val bioSampleId = convertSourceToString(source, PropertyNames.BIO_SAMPLE_ID);
    val variantSetIds = convertSourceToIntegerSet(source, PropertyNames.VARIANT_SET_IDS);
    return EsCallSet.builder()
        .name(name)
        .bioSampleId(bioSampleId)
        .variantSetIds(variantSetIds)
        .build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsCallSet callSet) {
    return object()
        .with(PropertyNames.NAME, callSet.getName())
        .with(PropertyNames.BIO_SAMPLE_ID, callSet.getBioSampleId())
        .with(PropertyNames.VARIANT_SET_IDS, convertIntegers(callSet.getVariantSetIds()))
        .end();
  }

  public static String extractCallSetName(PortalMetadata portalMetadata){
    return portalMetadata.getSampleId();
  }

  public static String extractBioSampleId(PortalMetadata portalMetadata){
    return portalMetadata.getSampleId();
  }


}
