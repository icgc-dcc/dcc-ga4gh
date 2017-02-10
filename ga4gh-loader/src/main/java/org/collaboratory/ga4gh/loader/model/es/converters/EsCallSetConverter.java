package org.collaboratory.ga4gh.loader.model.es.converters;

import static org.collaboratory.ga4gh.core.Names.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.core.Names.NAME;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToIntegerList;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToString;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertIntegers;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

import java.util.Set;

import org.collaboratory.ga4gh.loader.model.contexts.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.model.es.EsCallSet;
import org.collaboratory.ga4gh.loader.utils.cache.IdCache;
import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;

import lombok.RequiredArgsConstructor;
import lombok.val;

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

  public Set<EsCallSet> convertFromFileMetaDataContext(FileMetaDataContext fileMetaDataContext,
      IdCache<String, Integer> variantSetIdCache) {
    val groupedBySampleMap = fileMetaDataContext.groupFileMetaDataBySample();
    val setBuilder = ImmutableSet.<EsCallSet> builder();
    val converter = new EsVariantSetConverter();
    for (val entry : groupedBySampleMap.entrySet()) {
      val sampleName = entry.getKey();
      val fileMetaDataContextForSample = entry.getValue();
      val variantSetIds = converter.convertFromFileMetaDataContext(fileMetaDataContextForSample).stream()
          .map(vs -> variantSetIdCache.getId(vs.getName()))
          .collect(toImmutableSet());
      setBuilder.add(
          EsCallSet.builder()
              .name(sampleName)
              .variantSetIds(variantSetIds)
              .bioSampleId(sampleName) // bio_sample_id == call_set_name
              .build());
    }
    return setBuilder.build();
  }

}
