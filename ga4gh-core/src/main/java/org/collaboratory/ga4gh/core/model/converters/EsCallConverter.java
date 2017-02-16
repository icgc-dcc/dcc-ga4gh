package org.collaboratory.ga4gh.core.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.val;
import org.collaboratory.ga4gh.core.model.es.EsCall;
import org.elasticsearch.search.SearchHit;

import static org.collaboratory.ga4gh.core.JsonNodeConverters.convertIntegers;
import static org.collaboratory.ga4gh.core.JsonNodeConverters.convertMap;
import static org.collaboratory.ga4gh.core.PropertyNames.GENOTYPE_PHASESET;
import static org.collaboratory.ga4gh.core.PropertyNames.INFO;
import static org.collaboratory.ga4gh.core.PropertyNames.NON_REFERENCE_ALLELES;
import static org.collaboratory.ga4gh.core.PropertyNames.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.core.PropertyNames.CALL_SET_ID;
import static org.collaboratory.ga4gh.core.PropertyNames.GENOTYPE_LIKELIHOOD;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToBoolean;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToDouble;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToInteger;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToIntegerList;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToObjectMap;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

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
