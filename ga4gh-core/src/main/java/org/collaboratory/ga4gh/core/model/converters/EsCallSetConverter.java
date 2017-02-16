package org.collaboratory.ga4gh.core.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.collaboratory.ga4gh.core.model.es.EsCallSet;
import org.elasticsearch.search.SearchHit;

import static org.collaboratory.ga4gh.core.JsonNodeConverters.convertIntegers;
import static org.collaboratory.ga4gh.core.Names.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.core.Names.NAME;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToIntegerList;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToString;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@RequiredArgsConstructor
public class EsCallSetConverter
    implements ObjectNodeConverter<EsCallSet>,
    SearchHitConverter<EsCallSet> {

  @Override
  public EsCallSet convertFromSearchHit(SearchHit hit) {
    val name = convertHitToString(hit, NAME);
    val bioSampleId = convertHitToString(hit, BIO_SAMPLE_ID);
    val variantSetIds = convertHitToIntegerList(hit, VARIANT_SET_IDS);
    return EsCallSet.builder()
        .name(name)
        .bioSampleId(bioSampleId)
        .variantSetIds(variantSetIds)
        .build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsCallSet callSet) {
    return object()
        .with(NAME, callSet.getName())
        .with(VARIANT_SET_IDS, convertIntegers(callSet.getVariantSetIds()))
        .with(BIO_SAMPLE_ID, callSet.getBioSampleId())
        .end();
  }


}
