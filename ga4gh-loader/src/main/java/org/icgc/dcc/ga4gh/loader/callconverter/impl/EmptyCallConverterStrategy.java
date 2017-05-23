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
