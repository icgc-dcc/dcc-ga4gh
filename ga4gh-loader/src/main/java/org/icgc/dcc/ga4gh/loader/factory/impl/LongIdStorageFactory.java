package org.icgc.dcc.ga4gh.loader.factory.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.factory.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.LongIdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory;

@RequiredArgsConstructor
public class LongIdStorageFactory implements IdStorageFactory<Long> {

  @NonNull private final MapStorageFactory<EsVariantSet, Long>     variantSetLongMapStorageFactory;
  @NonNull private final MapStorageFactory<EsCallSet, Long>     callSetLongMapStorageFactory;

  @Override public AbstractIdStorageTemplate<EsVariantSet, Long> createVariantSetIdStorage(boolean useDisk) {
    val mapStorage = variantSetLongMapStorageFactory.createNewMapStorage(useDisk);
    return LongIdStorage.<EsVariantSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Long> createCallSetIdStorage(boolean useDisk) {
    val mapStorage = callSetLongMapStorageFactory.createNewMapStorage(useDisk);
    return LongIdStorage.<EsCallSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsVariantSet, Long> persistVariantSetIdStorage() {
    val mapStorage = variantSetLongMapStorageFactory.persistMapStorage();
    return LongIdStorage.<EsVariantSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Long> persistCallSetIdStorage() {
    val mapStorage = callSetLongMapStorageFactory.persistMapStorage();
    return LongIdStorage.<EsCallSet>createLongIdStorage(mapStorage, 0L);
  }

  public static LongIdStorageFactory createLongIdStorageFactory(
      MapStorageFactory<EsVariantSet, Long> variantSetLongMapStorageFactory,
      MapStorageFactory<EsCallSet, Long> callSetLongMapStorageFactory) {
    return new LongIdStorageFactory( variantSetLongMapStorageFactory,
        callSetLongMapStorageFactory);
  }

}
