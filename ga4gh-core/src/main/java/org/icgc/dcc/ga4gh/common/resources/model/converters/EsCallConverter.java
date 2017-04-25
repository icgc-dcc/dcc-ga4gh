package org.icgc.dcc.ga4gh.common.resources.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.val;
import org.icgc.dcc.ga4gh.common.resources.JsonNodeConverters;
import org.icgc.dcc.ga4gh.common.resources.PropertyNames;
import org.icgc.dcc.ga4gh.common.resources.SearchHits;
import org.icgc.dcc.ga4gh.common.resources.model.es.EsCall;
import org.elasticsearch.search.SearchHit;

import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

public class EsCallConverter
    implements ObjectNodeConverter<EsCall>,
    SearchHitConverter<EsCall> ,
    SourceConverter<EsCall> {

  @Override public EsCall convertFromSource(Map<String, Object> source) {
    val variantSetId = SearchHits.convertSourceToInteger(source, PropertyNames.VARIANT_SET_ID);
    val callSetId = SearchHits.convertSourceToInteger(source, PropertyNames.CALL_SET_ID);
    val callSetName = SearchHits.convertSourceToString(source, PropertyNames.CALL_SET_NAME);
    val info = SearchHits.convertSourceToObjectMap(source, PropertyNames.INFO);
    val genotypeLikelihood = SearchHits.convertSourceToDouble(source, PropertyNames.GENOTYPE_LIKELIHOOD);
    val isGenotypePhased = SearchHits.convertSourceToBoolean(source, PropertyNames.GENOTYPE_PHASESET);
    val nonReferenceAlleles = SearchHits.convertSourceToIntegerList(source, PropertyNames.NON_REFERENCE_ALLELES);

    return EsCall.builder()
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
  public EsCall convertFromSearchHit(SearchHit hit) {
    return convertFromSource(hit.getSource());
  }

  @Override
  public ObjectNode convertToObjectNode(EsCall call) {
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
