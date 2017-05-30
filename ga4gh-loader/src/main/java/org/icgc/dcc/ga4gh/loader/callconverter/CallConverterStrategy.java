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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall.EsBasicCallBuilder;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.joining;

public interface CallConverterStrategy {

  int DEFAULT_REFERENCE_ALLELE_POSITION = 0;
  int ALTERNATIVE_ALLELE_INDEX_OFFSET = 1;
  int UNKNOWN_ALLELE_INDEX = -1;
  String UNKNOWN_ALLELE_STRING = ".";

  List<EsBasicCall> convertBasic(EsBasicCallBuilder callBuilder, VariantContext variantContext);

  default List<EsBasicCall> convertBasic(final int variantSetId, final int callSetId, String callSetName, VariantContext variantContext){
    val esCallBuilder = EsBasicCall.builder()
        .variantSetId(variantSetId)
        .callSetId(callSetId)
        .callSetName(callSetName);
    return convertBasic(esCallBuilder, variantContext);
  }

  static void checkNumCalls(int actualNumCalls, int expectedNumCalls){
    checkState(actualNumCalls == expectedNumCalls, "[CALL_CONVERTER_ERROR] -- Expected NumCalls: %s, Actual NumCalls: %s", expectedNumCalls, actualNumCalls);

  }

  static boolean isAlleleUnknown(final Allele allele) {
    return allele.getBaseString().equals(UNKNOWN_ALLELE_STRING);
  }

  static List<Integer> convertGenotypeAlleles(final List<Allele> alternativeAlleles, final List<Allele> genotypeAlleles) {
    val allelesBuilder = ImmutableList.<Integer> builder();
    for (val allele : genotypeAlleles) {
      if (allele.isNonReference()) {
        val indexAltAllele = alternativeAlleles.indexOf(allele);
        val foundIndex = indexAltAllele > -1;
        if (foundIndex) {
          allelesBuilder.add(ALTERNATIVE_ALLELE_INDEX_OFFSET + indexAltAllele);
        } else if (isAlleleUnknown(allele)) {
          allelesBuilder.add(UNKNOWN_ALLELE_INDEX);
        } else {
          checkState(foundIndex, "Could not find the allele [%s] in the alternative alleles list [%s] ",
              allele.getBaseString(),
              alternativeAlleles.stream()
                  .map(Allele::getBaseString)
                  .collect(joining(",")));
        }
      } else {
        allelesBuilder.add(DEFAULT_REFERENCE_ALLELE_POSITION);
      }
    }

    val alleles = allelesBuilder.build();
    val hasCorrectNumberOfAlleles = alleles.size() == genotypeAlleles.size();

    checkState(hasCorrectNumberOfAlleles,
        "There was an error with creating the allele index list. AlternateAlleles: [%s], GenotypesAlleles: [%s]",
        alternativeAlleles.stream().map(Allele::getBaseString).collect(joining(",")),
        genotypeAlleles.stream().map(Allele::getBaseString).collect(joining(",")));
    return alleles;
  }

  static List<EsBasicCall>  buildTumorCall(EsBasicCallBuilder callBuilder, VariantContext variantContext, int tumorPos){
    val genotypes = variantContext.getGenotypes();
    val commonInfoMap = variantContext.getCommonInfo().getAttributes();
    val altAlleles = variantContext.getAlternateAlleles();
    val genotype = genotypes.get(tumorPos);
    val callInfo = genotype.getExtendedAttributes();
    val info = Maps.<String, Object>newHashMap();
    info.putAll(commonInfoMap);
    info.putAll(callInfo);
    val genotypeAlleles  = genotype.getAlleles();
    return ImmutableList.of(callBuilder
        .info(info)
        .genotypeLikelihood(genotype.getLog10PError())
        .isGenotypePhased(genotype.isPhased())
        .nonReferenceAlleles(convertGenotypeAlleles(altAlleles, genotypeAlleles))
        .build());
  }




}
