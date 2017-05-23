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

  default List<EsBasicCall> convertBasic(final int variantSetId, final int callSetId, String callSetName, VariantContext variantContext){
    val esCallBuilder = EsBasicCall.builder()
        .variantSetId(variantSetId)
        .callSetId(callSetId)
        .callSetName(callSetName);
    return convertBasic(esCallBuilder, variantContext);
  }

  List<EsBasicCall> convertBasic(EsBasicCallBuilder callBuilder, VariantContext variantContext);



}
