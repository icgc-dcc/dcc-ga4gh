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

import com.google.common.collect.ImmutableList;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall.EsBasicCallBuilder;
import org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy;

import java.util.List;

import static org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy.checkNumCalls;

public class EmptyCallConverterStrategy implements CallConverterStrategy {

  private static final int EXPECTED_NUMBER_OF_CALLS = 0;
  private static final double DEFAULT_DUMMY_GENOTYPE_LIKELIHOOD = 0.0;
  private static final boolean DEFAULT_DUMMY_GENOTYPE_PHASED = false;
  private static final List<Integer> DEFAULT_DUMMY_NON_REFERENCE_ALLELES = ImmutableList.of(-1);

  @Override
  public List<EsBasicCall> convertBasic(EsBasicCallBuilder callBuilder, VariantContext variantContext) {
    val actualNumCalls = variantContext.getGenotypes().size();
    checkNumCalls(actualNumCalls, EXPECTED_NUMBER_OF_CALLS);
    return buildDummyCall(callBuilder, variantContext);
  }

  private static List<EsBasicCall>  buildDummyCall(EsBasicCallBuilder callBuilder, VariantContext variantContext){
    val commonInfoMap = variantContext.getCommonInfo().getAttributes();
    return ImmutableList.of(callBuilder
        .info(commonInfoMap)
        .genotypeLikelihood(DEFAULT_DUMMY_GENOTYPE_LIKELIHOOD)
        .isGenotypePhased(DEFAULT_DUMMY_GENOTYPE_PHASED)
        .nonReferenceAlleles(DEFAULT_DUMMY_NON_REFERENCE_ALLELES)
        .build());
  }

}
