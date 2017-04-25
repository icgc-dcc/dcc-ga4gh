package org.icgc.dcc.ga4gh.common.resources.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.resources.PropertyNames;
import org.icgc.dcc.ga4gh.common.resources.model.es.EsCallSet;
import org.elasticsearch.search.SearchHit;

import java.util.Map;

import static org.icgc.dcc.ga4gh.common.resources.JsonNodeConverters.convertIntegers;
import static org.icgc.dcc.ga4gh.common.resources.SearchHits.convertSourceToIntegerList;
import static org.icgc.dcc.ga4gh.common.resources.SearchHits.convertSourceToString;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@RequiredArgsConstructor
public class EsCallSetConverter
    implements ObjectNodeConverter<EsCallSet>,
    SearchHitConverter<EsCallSet> ,
    SourceConverter<EsCallSet> {

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
  public EsCallSet convertFromSearchHit(SearchHit hit) {
    return convertFromSource(hit.getSource());
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
