package org.icgc.dcc.ga4gh.loader.vcf.callprocessors.impl;

import com.google.common.collect.ImmutableList;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.loader.vcf.callprocessors.CallProcessor;
import org.icgc.dcc.ga4gh.loader.vcf.callprocessors.AbstractCallProcessor;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Implementation that creates a DummyCall for a specific callSet and variantSet, and uses the  variantContext info field.
 */
@RequiredArgsConstructor
public class DummyCallProcessor implements CallProcessor {

  private static final EsCall.EsCallBuilder DUMMY_CALL_BUILDER = EsCall.builder()
      .genotypeLikelihood(0.0)
      .isGenotypePhased(false)
      .nonReferenceAlleles(
          newArrayList(AbstractCallProcessor.UNKNOWN_ALLELE_INDEX, AbstractCallProcessor.UNKNOWN_ALLELE_INDEX));

  public static DummyCallProcessor newDummyCallProcessor() {
    return new DummyCallProcessor();
  }

  @Override
  public List<EsCall> createEsCallList(int variantSetId, int callSetId, String callSetName, VariantContext variantContext) {
    return ImmutableList.of(createDummyEsCall(variantSetId, callSetId, callSetName, variantContext.getCommonInfo().getAttributes()));
  }

  @Override
  public EsCall createEsCall(int variantSetId, int callSetId, String callSetName, Map<String, Object> commonInfoMap,
      final List<Allele> alternativeAlleles,
      final Genotype genotype) {
    return createDummyEsCall(variantSetId, callSetId, callSetName, commonInfoMap);
  }

  public EsCall createDummyEsCall(int variantSetId, int callSetId, String callSetName, Map<String, Object> commonInfoMap) {
    return DUMMY_CALL_BUILDER
        .callSetId(callSetId)
        .callSetName(callSetName)
        .variantSetId(variantSetId)
        .info(commonInfoMap)
        .build();
  }
}
