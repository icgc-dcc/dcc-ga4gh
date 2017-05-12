package org.icgc.dcc.ga4gh.loader;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataDao;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.IdStorage;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

@RequiredArgsConstructor
public class PreProcessor {

  public static PreProcessor createPreProcessor(PortalMetadataDao portalMetadataDao,
      IdStorage<EsCallSet, Integer> callSetIdStorage,
      IdStorage<EsVariantSet, Integer> variantSetIdStorage) {
    return new PreProcessor(portalMetadataDao, callSetIdStorage, variantSetIdStorage);
  }

  @NonNull private final PortalMetadataDao portalMetadataDao;
  @NonNull private final IdStorage<EsCallSet, Integer> callSetIdStorage;
  @NonNull private final IdStorage<EsVariantSet, Integer> variantSetIdStorage;

  /**
   * State
   */
  @Getter private boolean initialized = false;


  private void checkProcessed(){
    checkState(isInitialized(), "The PreProcessor was not initiallized. Call the init() method first");
  }

  public IdStorage<EsCallSet, Integer> getCallSetIdStorage() {
    checkProcessed();
    return callSetIdStorage;
  }

  public IdStorage<EsVariantSet, Integer> getVariantSetIdStorage() {
    checkProcessed();
    return variantSetIdStorage;
  }

  public void init(){

    // Populate VariantSetIdStorage
    portalMetadataDao
        .findAll()
        .stream()
        .map(EsVariantSetConverterJson::convertFromPortalMetadata)
        .collect(toImmutableSet())
        .forEach(variantSetIdStorage::add);

    // Traverse all sampleIds, and for eachone, create an esCallSet
    val sampleMap = portalMetadataDao.groupBySampleId();
    for(val sampleId : sampleMap.keySet()){
      val portalMetadataList = sampleMap.get(sampleId);

      // Build unique set of variantSetIds for this SampleId
      val variantSetIds = portalMetadataList.stream()
          .map(EsVariantSetConverterJson::convertFromPortalMetadata)
          .map(variantSetIdStorage::getId)
          .collect(toImmutableSet());

      // Build EsCallSet object
      val esCallSet = EsCallSet.builder()
          .bioSampleId(sampleId)
          .name(sampleId)
          .variantSetIds(variantSetIds)
          .build();

      // Store EsCallSet in IdStorage2
      callSetIdStorage.add(esCallSet);
      initialized = true;
    }
  }


}
