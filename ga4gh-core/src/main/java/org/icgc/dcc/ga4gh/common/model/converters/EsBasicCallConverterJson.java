package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.val;
import org.icgc.dcc.ga4gh.common.JsonNodeConverters;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.SearchHits;
import org.icgc.dcc.ga4gh.common.model.es.EsBasicCall;

import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

public class EsBasicCallConverterJson
    implements JsonObjectNodeConverter<EsBasicCall>,
    SearchHitConverter<EsBasicCall> {

  @Override public EsBasicCall convertFromSource(Map<String, Object> source) {
    val variantSetId = SearchHits.convertSourceToInteger(source, PropertyNames.VARIANT_SET_ID);
    val callSetId = SearchHits.convertSourceToInteger(source, PropertyNames.CALL_SET_ID);
    val callSetName = SearchHits.convertSourceToString(source, PropertyNames.CALL_SET_NAME);
    val info = SearchHits.convertSourceToObjectMap(source, PropertyNames.INFO);
    val genotypeLikelihood = SearchHits.convertSourceToDouble(source, PropertyNames.GENOTYPE_LIKELIHOOD);
    val isGenotypePhased = SearchHits.convertSourceToBoolean(source, PropertyNames.GENOTYPE_PHASESET);
    val nonReferenceAlleles = SearchHits.convertSourceToIntegerList(source, PropertyNames.NON_REFERENCE_ALLELES);

    return EsBasicCall.builder()
        .variantSetId(variantSetId)
        .callSetId(callSetId)
        .callSetName(callSetName)
        .info(info)
        .genotypeLikelihood(genotypeLikelihood)
        .isGenotypePhased(isGenotypePhased)
        .nonReferenceAlleles(nonReferenceAlleles)
        .build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsBasicCall call) {
    val nonRefAlleles = JsonNodeConverters.convertIntegers(call.getNonReferenceAlleles());
    return object()
        .with(PropertyNames.VARIANT_SET_ID, call.getVariantSetId())
        .with(PropertyNames.CALL_SET_ID, call.getCallSetId())
        .with(PropertyNames.CALL_SET_NAME, call.getCallSetName())
        .with(PropertyNames.INFO, JsonNodeConverters.convertMap(call.getInfo()))
        .with(PropertyNames.GENOTYPE_LIKELIHOOD, Double.toString(call.getGenotypeLikelihood()))
        .with(PropertyNames.GENOTYPE_PHASESET, call.isGenotypePhased())
        .with(PropertyNames.NON_REFERENCE_ALLELES, nonRefAlleles)
        .end();
  }

}
