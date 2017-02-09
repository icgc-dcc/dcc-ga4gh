package org.collaboratory.ga4gh.loader.model.es.converters;

import static org.collaboratory.ga4gh.core.Names.CALL_SET_ID;
import static org.collaboratory.ga4gh.core.Names.GENOTYPE_LIKELIHOOD;
import static org.collaboratory.ga4gh.core.Names.GENOTYPE_PHASESET;
import static org.collaboratory.ga4gh.core.Names.INFO;
import static org.collaboratory.ga4gh.core.Names.NON_REFERENCE_ALLELES;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToBoolean;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToDouble;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToInteger;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToIntegerList;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToObjectMap;
import static org.collaboratory.ga4gh.loader.VCF.convertGenotypeAlleles;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertIntegers;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertMap;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.util.List;

import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;

public class EsCallConverter
    implements ObjectNodeConverter<EsCall>,
    SearchHitConverter<EsCall> {

  @Override
  public EsCall convertFromSearchHit(SearchHit hit) {
    val callSetId = convertHitToInteger(hit, CALL_SET_ID);
    val genotypeLikelihood = convertHitToDouble(hit, GENOTYPE_LIKELIHOOD);
    val info = convertHitToObjectMap(hit, INFO);
    val isGenotypePhased = convertHitToBoolean(hit, GENOTYPE_PHASESET);
    val nonReferenceAlleles = convertHitToIntegerList(hit, NON_REFERENCE_ALLELES);
    val variantSetId = convertHitToInteger(hit, VARIANT_SET_ID);

    return EsCall.builder()
        .callSetId(callSetId)
        .genotypeLikelihood(genotypeLikelihood)
        .info(info)
        .isGenotypePhased(isGenotypePhased)
        .nonReferenceAlleles(nonReferenceAlleles)
        .variantSetId(variantSetId)
        .build();

  }

  public List<EsCall> convertFromVariantContext(final VariantContext variantContext, final int variantSetId,
      final int callSetId) {
    val genotypesContext = variantContext.getGenotypes();
    val commonInfoMap = variantContext.getCommonInfo().getAttributes();
    val altAlleles = variantContext.getAlternateAlleles();

    val callsBuilder = ImmutableList.<EsCall> builder();
    for (val genotype : genotypesContext) {
      val info = genotype.getExtendedAttributes();
      info.putAll(commonInfoMap);
      callsBuilder.add(
          EsCall.builder()
              .variantSetId(variantSetId)
              .callSetId(callSetId)
              .info(info)
              .genotypeLikelihood(genotype.getLog10PError())
              .isGenotypePhased(genotype.isPhased())
              .nonReferenceAlleles(convertGenotypeAlleles(altAlleles, genotype))
              .build());
    }
    return callsBuilder.build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsCall call) {
    val nonRefAlleles = convertIntegers(call.getNonReferenceAlleles());
    return object()
        .with(VARIANT_SET_ID, call.getVariantSetId())
        .with(CALL_SET_ID, call.getCallSetId())
        .with(INFO, convertMap(call.getInfo()))
        .with(GENOTYPE_LIKELIHOOD, Double.toString(call.getGenotypeLikelihood()))
        .with(GENOTYPE_PHASESET, call.isGenotypePhased())
        .with(NON_REFERENCE_ALLELES, nonRefAlleles)
        .end();
  }

}
