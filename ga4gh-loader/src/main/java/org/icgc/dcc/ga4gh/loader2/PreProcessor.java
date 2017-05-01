package org.icgc.dcc.ga4gh.loader2;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDao;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorageFactory;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

@RequiredArgsConstructor
public class PreProcessor {

  public static PreProcessor createPreProcessor(PortalMetadataDao portalMetadataDao,
      IdStorageFactory<EsVariantSet> variantSetIdStorageFactory,
      IdStorageFactory<EsCallSet> callSetIdStorageFactory) {
    return new PreProcessor(portalMetadataDao, variantSetIdStorageFactory, callSetIdStorageFactory);
  }

  @NonNull private final PortalMetadataDao portalMetadataDao;
  @NonNull final private IdStorageFactory<EsVariantSet> variantSetIdStorageFactory;
  @NonNull final private IdStorageFactory<EsCallSet> callSetIdStorageFactory;

  @Getter private boolean initialized = false;
  private IdStorage<EsCallSet, Integer> callSetIdStorage;
  private IdStorage<EsVariantSet, Integer> variantSetIdStorage;

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
    this.variantSetIdStorage = variantSetIdStorageFactory.createIntegerIdStorage();
    this.callSetIdStorage = callSetIdStorageFactory.createIntegerIdStorage();

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

      // Store EsCallSet in IdStorage
      callSetIdStorage.add(esCallSet);
      initialized = true;
    }
  }


}
