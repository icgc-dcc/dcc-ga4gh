package org.icgc.dcc.ga4gh.loader.callconverter.impl;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy;

import java.util.List;

import static org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy.buildTumorCall;
import static org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategy.checkNumCalls;

public class SingleCallConverterStrategy implements CallConverterStrategy {

  private static final int EXPECTED_NUMBER_OF_CALLS = 1;
  private static final int TUMOR_POS = 0;

  @Override public List<EsCall> convert(EsCall.EsCallBuilder callBuilder, VariantContext variantContext) {
    val actualNumCalls = variantContext.getGenotypes().size();
    checkNumCalls(actualNumCalls, EXPECTED_NUMBER_OF_CALLS);
    return buildTumorCall(callBuilder,variantContext, TUMOR_POS);
  }


}
