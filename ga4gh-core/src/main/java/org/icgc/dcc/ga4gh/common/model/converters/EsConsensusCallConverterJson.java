package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.val;
import org.icgc.dcc.ga4gh.common.JsonNodeConverters;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.SearchHits;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall;

import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

public class EsConsensusCallConverterJson
    implements JsonObjectNodeConverter<EsConsensusCall>,
    SearchHitConverter<EsConsensusCall> {

  @Override public EsConsensusCall convertFromSource(Map<String, Object> source) {
    val variantSetIds = SearchHits.convertSourceToIntegerList(source, PropertyNames.VARIANT_SET_IDS);
    val callSetId = SearchHits.convertSourceToInteger(source, PropertyNames.CALL_SET_ID);
    val callSetName = SearchHits.convertSourceToString(source, PropertyNames.CALL_SET_NAME);
    val info = SearchHits.convertSourceToObjectMap(source, PropertyNames.INFO);

    return EsConsensusCall.builder()
        .variantSetIds(variantSetIds)
        .callSetId(callSetId)
        .callSetName(callSetName)
        .info(info)
        .build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsConsensusCall call) {
    return object()
        .with(PropertyNames.VARIANT_SET_IDS, JsonNodeConverters.convertIntegers(call.getVariantSetIds()))
        .with(PropertyNames.CALL_SET_ID, call.getCallSetId())
        .with(PropertyNames.CALL_SET_NAME, call.getCallSetName())
        .with(PropertyNames.INFO, JsonNodeConverters.convertMap(call.getInfo()))
        .end();
  }

}
