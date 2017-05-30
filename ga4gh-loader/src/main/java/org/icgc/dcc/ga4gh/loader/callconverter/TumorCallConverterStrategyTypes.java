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

package org.icgc.dcc.ga4gh.loader.callconverter;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall.EsBasicCallBuilder;
import org.icgc.dcc.ga4gh.loader.callconverter.impl.DualCallConverterStrategy;

import java.util.List;
import java.util.function.BiFunction;

import static org.icgc.dcc.ga4gh.loader.callconverter.impl.DualCallConverterStrategy.createDualCallConverterStrategy;
import static org.icgc.dcc.ga4gh.loader.callconverter.impl.FunctorTumorGenotypeClassifier.createFunctorTumorGenotypeClassifier;

@Slf4j
public enum TumorCallConverterStrategyTypes implements CallConverterStrategy {
  TUMOR_CALL_CONVERTER_STRATEGY("TUMOR", (exp,act) -> exp.equals(act.trim()), false ), //Check they are equal
  TUMOUR_CALL_CONVERTER_STRATEGY("TUMOUR", (exp,act) -> exp.equals(act.trim()), false ), //Check they are equal
  NT_CALL_CONVERTER_STRATEGY("T", (exp,act) -> act.trim().endsWith(exp) ,false) ; //Just check that it ends with T

  private final DualCallConverterStrategy dualCallConverterStrategy;

  private TumorCallConverterStrategyTypes(String expectedTumorSampleName, BiFunction<String, String, Boolean> functor,  boolean isTumorPos0) {
    val tumorClassifier = createFunctorTumorGenotypeClassifier(expectedTumorSampleName, functor);
    this.dualCallConverterStrategy = createDualCallConverterStrategy(tumorClassifier, isTumorPos0);
  }

  @Override public List<EsBasicCall> convertBasic(EsBasicCallBuilder callBuilder, VariantContext variantContext) {
    return dualCallConverterStrategy.convertBasic(callBuilder,variantContext);
  }
}
