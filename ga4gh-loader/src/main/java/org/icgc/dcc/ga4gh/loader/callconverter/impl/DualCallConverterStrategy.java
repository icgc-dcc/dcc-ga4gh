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

  public static DualCallConverterStrategy createDualCallConverterStrategy(
      TumorGenotypeClassifier tumorGenotypeClassifier, boolean isTumorPos0) {
    return new DualCallConverterStrategy(tumorGenotypeClassifier, isTumorPos0);
  }

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
    String tumorSampleName = tumorGenotype.getSampleName();
    if (tumorGenotypeClassifier.classify(tumorGenotype)){ //Try first guess for tumorPosition
      return candidateTumorPos;
    } else { // Otherwise try the other position
      val normalGenotype = genotypes.get(candidateNormalPos);
      String normalSampleName = normalGenotype.getSampleName();

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

}
