package org.icgc.dcc.ga4gh.loader2.callconverter.impl;

import com.google.common.collect.ImmutableList;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategy;

import java.util.List;

import static org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategy.checkNumCalls;

public class EmptyCallConverterStrategy implements CallConverterStrategy {

  private static final List<EsCall> EMPTY_CALL_LIST = ImmutableList.of();
  private static final int EXPECTED_NUMBER_OF_CALLS = 0;

  @Override
  public List<EsCall> convert(EsCall.EsCallBuilder callBuilder, VariantContext variantContext) {
    val actualNumCalls = variantContext.getGenotypes().size();
    checkNumCalls(actualNumCalls, EXPECTED_NUMBER_OF_CALLS);
    return EMPTY_CALL_LIST;
  }

}
