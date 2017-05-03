package org.icgc.dcc.ga4gh.loader2.callconverter;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.loader2.callconverter.impl.DualCallConverterStrategy;

import java.util.List;
import java.util.function.BiFunction;

import static org.icgc.dcc.ga4gh.loader2.callconverter.impl.DualCallConverterStrategy.createDualCallConverterStrategy;
import static org.icgc.dcc.ga4gh.loader2.callconverter.impl.FunctorTumorGenotypeClassifier.createFunctorTumorGenotypeClassifier;

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

  @Override public List<EsCall> convert(EsCall.EsCallBuilder callBuilder, VariantContext variantContext) {
    return dualCallConverterStrategy.convert(callBuilder,variantContext);
  }
}
