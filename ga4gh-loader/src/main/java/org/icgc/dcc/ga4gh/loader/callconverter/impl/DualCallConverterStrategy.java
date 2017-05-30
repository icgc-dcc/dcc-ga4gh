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

package org.icgc.dcc.ga4gh.loader.callconverter.impl;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall.EsBasicCallBuilder;
import org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy;
import org.icgc.dcc.ga4gh.loader.callconverter.TumorGenotypeClassifier;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy.buildTumorCall;
import static org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy.checkNumCalls;

@Slf4j
public class DualCallConverterStrategy implements CallConverterStrategy {

  private static final int EXPECTED_NUM_CALLS = 2;

  private final TumorGenotypeClassifier tumorGenotypeClassifier;
  private final int candidateTumorPos;
  private final int candidateNormalPos;

  public DualCallConverterStrategy(TumorGenotypeClassifier tumorGenotypeClassifier, boolean isTumorPos0) {
    this.candidateTumorPos = isTumorPos0 ? 0 : 1;
    this.candidateNormalPos = isTumorPos0 ? 1 : 0;
    this.tumorGenotypeClassifier = tumorGenotypeClassifier;
  }

  private int calcTumorPos(VariantContext variantContext, int expectedNumCalls){
    val genotypes = variantContext.getGenotypes();
    val actualNumCalls = genotypes.size();
    checkNumCalls(actualNumCalls, expectedNumCalls);

    val tumorGenotype = genotypes.get(candidateTumorPos);
    val tumorSampleName = tumorGenotype.getSampleName();
    if (tumorGenotypeClassifier.classify(tumorGenotype)){ //Try first guess for tumorPosition
      return candidateTumorPos;
    } else { // Otherwise try the other position
      val normalGenotype = genotypes.get(candidateNormalPos);
      val normalSampleName = normalGenotype.getSampleName();

      checkState(tumorGenotypeClassifier.classify(normalGenotype),
          "The tumorGenotype [%s] was negatively classified by the [%s], and similarly for normalGenotype [%s]",
           tumorSampleName, tumorGenotypeClassifier.getClass().getSimpleName(), normalSampleName);

      log.warn("The tumorGenotype [{}] was negatively classified by the [{}], but the normalGenotype [{}] was positively classified so using that", tumorSampleName,  tumorGenotypeClassifier.getClass().getSimpleName(), normalSampleName);
      return candidateNormalPos;
    }

  }

  @Override public List<EsBasicCall> convertBasic(EsBasicCallBuilder callBuilder, VariantContext variantContext) {
    val tumorPos = calcTumorPos(variantContext, EXPECTED_NUM_CALLS);
    return buildTumorCall(callBuilder, variantContext, tumorPos);
  }

  public static DualCallConverterStrategy createDualCallConverterStrategy(
      TumorGenotypeClassifier tumorGenotypeClassifier, boolean isTumorPos0) {
    return new DualCallConverterStrategy(tumorGenotypeClassifier, isTumorPos0);
  }


}
