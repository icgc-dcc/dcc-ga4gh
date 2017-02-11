package org.collaboratory.ga4gh.loader.vcf.processors;

import static com.google.common.collect.Lists.newArrayList;
import static org.collaboratory.ga4gh.loader.vcf.AbstractCallProcessor.UNKNOWN_ALLELE_INDEX;

import java.util.List;
import java.util.Map;

import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsCall.EsCallBuilder;
import org.collaboratory.ga4gh.loader.vcf.CallProcessor;

import com.google.common.collect.ImmutableList;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DummyCallProcessor implements CallProcessor {

  private static final EsCall.EsCallBuilder DUMMY_CALL_BUILDER = EsCall.builder()
      .genotypeLikelihood(0.0)
      .isGenotypePhased(false)
      .nonReferenceAlleles(
          newArrayList(UNKNOWN_ALLELE_INDEX, UNKNOWN_ALLELE_INDEX));

  public static DummyCallProcessor newDummyCallProcessor() {
    return new DummyCallProcessor();
  }

  @Override
  public List<EsCall> createEsCallList(int variantSetId, int callSetId, VariantContext variantContext) {
    return ImmutableList.of(createDummyEsCall(variantSetId, callSetId, variantContext.getCommonInfo().getAttributes()));
  }

  @Override
  public EsCall createEsCall(int variantSetId, int callSetId, Map<String, Object> commonInfoMap,
      final List<Allele> alternativeAlleles,
      final Genotype genotype) {
    return createDummyEsCall(variantSetId, callSetId, commonInfoMap);
  }

  public EsCall createDummyEsCall(int variantSetId, int callSetId, Map<String, Object> commonInfoMap) {
    return DUMMY_CALL_BUILDER
        .callSetId(callSetId)
        .variantSetId(variantSetId)
        .info(commonInfoMap)
        .build();
  }
}
