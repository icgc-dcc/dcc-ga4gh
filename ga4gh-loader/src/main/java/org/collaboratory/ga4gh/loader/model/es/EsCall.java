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

import static com.google.common.collect.Maps.newHashMap;
import static java.util.Objects.requireNonNull;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.CALL_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.GENOTYPE_LIKELIHOOD;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.GENOTYPE_PHASESET;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.INFO;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.NON_REFERENCE_ALLELES;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_ID;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.variant.variantcontext.CommonInfo;
import htsjdk.variant.variantcontext.Genotype;
import lombok.Builder;
import lombok.Value;
import lombok.val;

/*
 * ObjectNode is a bit heavy, this is just to minimize memory usage
 */
@Builder
@Value
public class EsCall implements EsModel {

  private EsVariant parentVariant;
  private String variantSetId;
  private String callSetId;
  private CommonInfo info;
  private Genotype genotype;

  public EsCall(EsVariant parentVariant, String variantSetId, String callSetId, CommonInfo info, Genotype genotype) {
    this.parentVariant = requireNonNull(parentVariant);
    this.variantSetId = variantSetId;
    this.callSetId = callSetId;
    this.info = requireNonNull(info);
    this.genotype = requireNonNull(genotype);
  }

  @Override
  public ObjectNode toDocument() {
    val infoMap = newHashMap(info.getAttributes());
    infoMap.putAll(genotype.getExtendedAttributes());
    val nonRefAlleles = EsModel.createIntegerArrayNode(EsModel.convertNonRefAlleles(genotype));
    val likelihood = genotype.getLog10PError();
    val isPhaseset = genotype.isPhased();

    return object()
        .with(VARIANT_SET_ID, variantSetId)
        .with(CALL_SET_ID, callSetId)
        .with(INFO, EsModel.convertMapToObjectNode(infoMap))
        .with(GENOTYPE_LIKELIHOOD, Double.toString(likelihood))
        .with(GENOTYPE_PHASESET, isPhaseset)
        .with(NON_REFERENCE_ALLELES, nonRefAlleles)
        .end();
  }

  // TODO: [rtisma] when have time, add a fromDocument converter. Then move models to core and use this class as a
  // conversion from Es to Model, and back
  /*
   * public static EsCall fromDocument(ObjectNode callNode){
   * 
   * 
   * }
   */

  @Override
  public String getName() {
    return Joiners.COLON.join(
        variantSetId, callSetId, parentVariant.getName(), genotype.getSampleName());
  }

}
