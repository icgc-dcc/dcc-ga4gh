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

import static org.collaboratory.ga4gh.core.Names.CALL_SET_ID;
import static org.collaboratory.ga4gh.core.Names.GENOTYPE_LIKELIHOOD;
import static org.collaboratory.ga4gh.core.Names.GENOTYPE_PHASESET;
import static org.collaboratory.ga4gh.core.Names.INFO;
import static org.collaboratory.ga4gh.core.Names.NON_REFERENCE_ALLELES;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertIntegers;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertMap;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.Joiners.COLON;

import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import htsjdk.variant.variantcontext.Genotype;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.val;

/*
 * ObjectNode is a bit heavy, this is just to minimize memory usage
 */
@Builder
@Value
public class EsCall implements EsModel {

  private String variantSetId;
  private String callSetId;
  private Map<String, Object> info;
  private String sampleName;
  private double genotypeLikelihood;
  private boolean isGenotypePhased;
  private List<Integer> nonReferenceAlleles;

  @Override
  public ObjectNode toDocument() {
    val nonRefAlleles = convertIntegers(nonReferenceAlleles);
    return object()
        .with(VARIANT_SET_ID, variantSetId)
        .with(CALL_SET_ID, callSetId)
        .with(INFO, convertMap(info))
        .with(GENOTYPE_LIKELIHOOD, Double.toString(genotypeLikelihood))
        .with(GENOTYPE_PHASESET, isGenotypePhased)
        .with(NON_REFERENCE_ALLELES, nonRefAlleles)
        .end();
  }

  public static SpecialEsCallBuilder builder() {
    return new SpecialEsCallBuilder();
  }

  public static class SpecialEsCallBuilder extends EsCallBuilder {

    // TODO: Implement ME
    public SpecialEsCallBuilder fromSearchHit(final SearchHit hit) {
      return null;

    }

    /*
     * Copies the input EsCall
     */
    public SpecialEsCallBuilder fromEsCall(final EsCall call) {
      return (SpecialEsCallBuilder) builder()
          .callSetId(call.getCallSetId())
          .genotypeLikelihood(call.getGenotypeLikelihood())
          .info(call.getInfo())
          .isGenotypePhased(call.isGenotypePhased())
          .nonReferenceAlleles(call.getNonReferenceAlleles())
          .sampleName(call.getSampleName())
          .variantSetId(call.getVariantSetId());
    }

  }

  // TODO: not unique. Need to make unique or change the rule
  @Override
  public String getName() {
    return COLON.join(
        variantSetId, callSetId, sampleName);
  }

  public static List<Integer> convertNonRefAlleles(@NonNull final Genotype genotype) {
    // TODO: [rtisma] -- verify this logic is correct. Allele has some other states that might need to be considered
    val allelesBuilder = ImmutableList.<Integer> builder();
    for (int i = 0; i < genotype.getAlleles().size(); i++) {
      val allele = genotype.getAllele(i);
      if (allele.isNonReference()) {
        allelesBuilder.add(i);
      }
    }
    allelesBuilder.add(99);// TODO: HACK just testing mutliple alleles translation to arraynode
    return allelesBuilder.build();
  }

}
