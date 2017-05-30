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
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.JsonNodeConverters;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;

import java.util.List;
import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToInteger;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToString;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToStringList;

@RequiredArgsConstructor
public class EsVariantConverterJson
    implements JsonObjectNodeConverter<EsVariant>,
    SearchHitConverter<EsVariant> {

  @Override
  public EsVariant convertFromSource(Map<String, Object> source) {
    val start = convertSourceToInteger(source, PropertyNames.START);
    val end = convertSourceToInteger(source, PropertyNames.END);
    val referenceName = convertSourceToString(source, PropertyNames.REFERENCE_NAME);
    val referenceBases = convertSourceToString(source, PropertyNames.REFERENCE_BASES);
    val alternateBases = convertSourceToStringList(source, PropertyNames.ALTERNATIVE_BASES);
    return EsVariant.builder()
        .start(start)
        .end(end)
        .referenceName(referenceName)
        .referenceBases(referenceBases)
        .alternativeBases(alternateBases)
        .build();

  }

  public EsVariant convertFromVariantContext(VariantContext variantContext) {
    val referenceBases = convertAlleleToByteArray(variantContext.getReference());
    val alternativeBases = convertAlleles(variantContext.getAlternateAlleles());
    val start = variantContext.getStart();
    val end = variantContext.getEnd();
    val referenceName = variantContext.getContig();

    return EsVariant.builder()
        .start(start)
        .end(end)
        .referenceName(referenceName)
        .referenceBasesAsBytes(referenceBases)
        .allAlternativeBasesAsBytes(alternativeBases)
        .build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsVariant variant) {
    return object()
        .with(PropertyNames.START, variant.getStart())
        .with(PropertyNames.END, variant.getEnd())
        .with(PropertyNames.REFERENCE_NAME, variant.getReferenceName())
        .with(PropertyNames.REFERENCE_BASES, variant.getReferenceBases())
        .with(PropertyNames.ALTERNATIVE_BASES, JsonNodeConverters.convertStrings(variant.getAlternativeBases()))
        .end();
  }

  public byte[] convertAlleleToByteArray(final Allele allele) {
    return allele.getBases();
  }

  public byte[][] convertAlleles(final List<Allele> alleles) {
    byte[][] bytes = new byte[alleles.size()][];
    int count = 0;
    for (val allele : alleles) {
      bytes[count] = convertAlleleToByteArray(allele);
      count++;
    }
    return bytes;
  }

}
