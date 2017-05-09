package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;

import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.common.JsonNodeConverters.convertIntegers;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToIntegerList;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToString;

@RequiredArgsConstructor
public class EsCallSetConverterJson
    implements JsonObjectNodeConverter<EsCallSet>,
    SearchHitConverter<EsCallSet> {

  @Override
  public EsCallSet convertFromSource(Map<String, Object> source) {
    val name = convertSourceToString(source, PropertyNames.NAME);
    val bioSampleId = convertSourceToString(source, PropertyNames.BIO_SAMPLE_ID);
    val variantSetIds = convertSourceToIntegerList(source, PropertyNames.VARIANT_SET_IDS);
    return EsCallSet.builder()
        .name(name)
        .bioSampleId(bioSampleId)
        .variantSetIds(variantSetIds)
        .build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsCallSet callSet) {
    return object()
        .with(PropertyNames.NAME, callSet.getName())
        .with(PropertyNames.BIO_SAMPLE_ID, callSet.getBioSampleId())
        .with(PropertyNames.VARIANT_SET_IDS, convertIntegers(callSet.getVariantSetIds()))
        .end();
  }


}
