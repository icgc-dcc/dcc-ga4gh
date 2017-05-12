package org.icgc.dcc.ga4gh.loader.factory.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.factory.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.LongIdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantIdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory;

import static org.icgc.dcc.ga4gh.loader.utils.counting.LongCounter.createLongCounter;

@RequiredArgsConstructor
public class LongIdStorageFactory implements IdStorageFactory<Long> {

  public static LongIdStorageFactory createLongIdStorageFactory(
      MapStorageFactory<EsVariant, IdStorageContext<Long, EsCall>> variantLongMapStorageFactory,
      MapStorageFactory<EsVariantSet, Long> variantSetLongMapStorageFactory,
      MapStorageFactory<EsCallSet, Long> callSetLongMapStorageFactory) {
    return new LongIdStorageFactory(variantLongMapStorageFactory,
        variantSetLongMapStorageFactory,
        callSetLongMapStorageFactory);
  }

  @NonNull private final MapStorageFactory<EsVariant, IdStorageContext<Long, EsCall>>     variantLongMapStorageFactory;
  @NonNull private final MapStorageFactory<EsVariantSet, Long>     variantSetLongMapStorageFactory;
  @NonNull private final MapStorageFactory<EsCallSet, Long>     callSetLongMapStorageFactory;

  @Override public VariantIdStorage<Long> createVariantIdStorage(boolean useDisk) {
    val mapStorage = variantLongMapStorageFactory.createNewMapStorage(useDisk);
    val counter = createLongCounter(0);
    return VariantIdStorage.<Long>createVariantIdStorage(counter,mapStorage);
  }

  @Override public AbstractIdStorageTemplate<EsVariantSet, Long> createVariantSetIdStorage(boolean useDisk) {
    val mapStorage = variantSetLongMapStorageFactory.createNewMapStorage(useDisk);
    return LongIdStorage.<EsVariantSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Long> createCallSetIdStorage(boolean useDisk) {
    val mapStorage = callSetLongMapStorageFactory.createNewMapStorage(useDisk);
    return LongIdStorage.<EsCallSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public VariantIdStorage<Long> persistVariantIdStorage() {
    val mapStorage = variantLongMapStorageFactory.persistMapStorage();
    val counter = createLongCounter(0);
    return VariantIdStorage.<Long>createVariantIdStorage(counter,mapStorage);
  }

  @Override public AbstractIdStorageTemplate<EsVariantSet, Long> persistVariantSetIdStorage() {
    val mapStorage = variantSetLongMapStorageFactory.persistMapStorage();
    return LongIdStorage.<EsVariantSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Long> persistCallSetIdStorage() {
    val mapStorage = callSetLongMapStorageFactory.persistMapStorage();
    return LongIdStorage.<EsCallSet>createLongIdStorage(mapStorage, 0L);
  }

}
