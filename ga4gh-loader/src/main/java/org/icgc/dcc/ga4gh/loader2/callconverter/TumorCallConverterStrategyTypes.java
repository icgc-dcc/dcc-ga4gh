package org.icgc.dcc.ga4gh.loader2.callconverter;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.loader2.callconverter.impl.DualCallConverterStrategy;

import java.util.List;
import java.util.regex.Pattern;

import static org.icgc.dcc.ga4gh.loader2.callconverter.impl.DualCallConverterStrategy.createDualCallConverterStrategy;
import static org.icgc.dcc.ga4gh.loader2.callconverter.impl.RegexTumorGenotypeClassifier.createRegexTumorGenotypeClassifier;

@Slf4j
public enum TumorCallConverterStrategyTypes implements CallConverterStrategy {
  TUMOR_CALL_CONVERTER_STRATEGY("^TUMOR$", false),
  TUMOUR_CALL_CONVERTER_STRATEGY("^TUMOUR$", false),
  NT_CALL_CONVERTER_STRATEGY("^.*T", false) ;

  private final DualCallConverterStrategy dualCallConverterStrategy;

  private TumorCallConverterStrategyTypes(String tumorRegex, boolean isTumorPos0) {
    val tumorPattern = Pattern.compile(tumorRegex);
    val tumorClassifier = createRegexTumorGenotypeClassifier(tumorPattern);
    this.dualCallConverterStrategy = createDualCallConverterStrategy(tumorClassifier, isTumorPos0);
  }

  @Override public List<EsCall> convert(EsCall.EsCallBuilder callBuilder, VariantContext variantContext) {
    return dualCallConverterStrategy.convert(callBuilder,variantContext);
  }
}
