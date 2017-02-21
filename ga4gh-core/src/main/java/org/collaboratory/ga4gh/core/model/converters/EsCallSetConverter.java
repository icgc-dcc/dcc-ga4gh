package org.collaboratory.ga4gh.core.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.collaboratory.ga4gh.core.model.es.EsCallSet;
import org.elasticsearch.search.SearchHit;

import java.util.Map;

import static org.collaboratory.ga4gh.core.JsonNodeConverters.convertIntegers;
import static org.collaboratory.ga4gh.core.PropertyNames.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.core.PropertyNames.NAME;
import static org.collaboratory.ga4gh.core.PropertyNames.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.core.SearchHits.convertSourceToIntegerList;
import static org.collaboratory.ga4gh.core.SearchHits.convertSourceToString;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@RequiredArgsConstructor
public class EsCallSetConverter
    implements ObjectNodeConverter<EsCallSet>,
    SearchHitConverter<EsCallSet> ,
    SourceConverter<EsCallSet> {

  @Override
  public EsCallSet convertFromSource(Map<String, Object> source) {
    val name = convertSourceToString(source, NAME);
    val bioSampleId = convertSourceToString(source, BIO_SAMPLE_ID);
    val variantSetIds = convertSourceToIntegerList(source, VARIANT_SET_IDS);
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
        .with(NAME, callSet.getName())
        .with(BIO_SAMPLE_ID, callSet.getBioSampleId())
        .with(VARIANT_SET_IDS, convertIntegers(callSet.getVariantSetIds()))
        .end();
  }


}
