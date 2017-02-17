package org.collaboratory.ga4gh.loader.vcf.callprocessors.impl;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.val;
import org.collaboratory.ga4gh.core.model.es.EsCall;
import org.collaboratory.ga4gh.loader.vcf.callprocessors.AbstractCallProcessor;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

/**
 * Basic implementation of CallProcessor that allows you to create Calls based on their SampleName.
 */
@Builder
@RequiredArgsConstructor
public class BasicCallProcessor extends AbstractCallProcessor {

  public static BasicCallProcessor newFilteredBasicCallProcessor(Collection<? extends String> sampleNames) {
    return BasicCallProcessor.builder()
        .sampleNames(sampleNames)
        .sampleFilteringEnabled(true)
        .build();
  }

  public static BasicCallProcessor newUnFilteredBasicCallProcessor() {
    return BasicCallProcessor.builder()
        .sampleFilteringEnabled(false)
        .build();
  }

  @Singular
  private final List<String> sampleNames;

  /*
   * If this is false, then all calls are used. Otherwise, filtering will happen
   */
  private final boolean sampleFilteringEnabled;

  /*
   * returns a list of EsCall objects filtered by their SampleNames.
   */
  @Override
  public List<EsCall> createEsCallList(final int variantSetId, final int callSetId,
      final String callSetName,
      final VariantContext variantContext) {
    val genotypesContext = variantContext.getGenotypes();
    val commonInfoMap = variantContext.getCommonInfo().getAttributes();
    val altAlleles = variantContext.getAlternateAlleles();
    // TODO: if list is empty, then throw exception,
    // becuase was probably not expecting that, regardless of sampleFilteringEnabled's value
    return genotypesContext.stream()
        .filter(this::selectThisSample)
        .map(g -> createEsCall(variantSetId, callSetId, callSetName, commonInfoMap, altAlleles, g))
        .collect(toImmutableList());
  }

  protected boolean selectThisSample(Genotype genotype) {
    if (sampleFilteringEnabled) {
      return sampleNames.contains(genotype.getSampleName());
    } else {
      return true;
    }
  }

  @Override
  public EsCall createEsCall(final int variantSetId, final int callSetId, final String callSetName, final Map<String, Object> commonInfoMap,
      final List<Allele> alternativeAlleles, final Genotype genotype) {

    val info = genotype.getExtendedAttributes();
    info.putAll(commonInfoMap);
    return EsCall.builder()
        .variantSetId(variantSetId)
        .callSetId(callSetId)
        .callSetName(callSetName)
        .info(info)
        .genotypeLikelihood(genotype.getLog10PError())
        .isGenotypePhased(genotype.isPhased())
        .nonReferenceAlleles(convertGenotypeAlleles(alternativeAlleles, genotype))
        .build();
  }

}
