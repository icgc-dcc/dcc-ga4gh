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
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IntegerIdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantIdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory;

import static org.icgc.dcc.ga4gh.loader.utils.counting.LongCounter.createLongCounter;

@RequiredArgsConstructor
public class IntegerIdStorageFactory implements IdStorageFactory<Integer> {

  public static IntegerIdStorageFactory createIntegerIdStorageFactory(
      MapStorageFactory<EsVariant, IdStorageContext<Integer, EsCall>> variantIntegerMapStorageFactory,
      MapStorageFactory<EsVariantSet, Integer> variantSetIntegerMapStorageFactory,
      MapStorageFactory<EsCallSet, Integer> callSetIntegerMapStorageFactory) {
    return new IntegerIdStorageFactory(variantIntegerMapStorageFactory,
        variantSetIntegerMapStorageFactory,
        callSetIntegerMapStorageFactory);
  }

  @NonNull private final MapStorageFactory<EsVariant, IdStorageContext<Integer, EsCall>>  variantIntegerMapStorageFactory;
  @NonNull private final MapStorageFactory<EsVariantSet, Integer>  variantSetIntegerMapStorageFactory;
  @NonNull private final MapStorageFactory<EsCallSet, Integer>  callSetIntegerMapStorageFactory;

  @Override public VariantIdStorage<Integer> createVariantIdStorage(boolean useDisk) {
    val mapStorage = variantIntegerMapStorageFactory.createNewMapStorage(useDisk);
    val counter = createLongCounter(0);
    return VariantIdStorage.<Integer>createVariantIdStorage(counter,mapStorage);
  }

  @Override public AbstractIdStorageTemplate<EsVariantSet, Integer> createVariantSetIdStorage(boolean useDisk) {
    val mapStorage = variantSetIntegerMapStorageFactory.createNewMapStorage(useDisk);
    return IntegerIdStorage.<EsVariantSet>createIntegerIdStorage(mapStorage, 0);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Integer> createCallSetIdStorage(boolean useDisk) {
    val mapStorage = callSetIntegerMapStorageFactory.createNewMapStorage(useDisk);
    return IntegerIdStorage.<EsCallSet>createIntegerIdStorage(mapStorage, 0);
  }

  @Override public VariantIdStorage<Integer> persistVariantIdStorage() {
    val mapStorage = variantIntegerMapStorageFactory.persistMapStorage();
    val counter = createLongCounter(0);
    return VariantIdStorage.<Integer>createVariantIdStorage(counter,mapStorage);
  }

  @Override public AbstractIdStorageTemplate<EsVariantSet, Integer> persistVariantSetIdStorage() {
    val mapStorage = variantSetIntegerMapStorageFactory.persistMapStorage();
    return IntegerIdStorage.<EsVariantSet>createIntegerIdStorage(mapStorage, 0);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Integer> persistCallSetIdStorage() {
    val mapStorage = callSetIntegerMapStorageFactory.persistMapStorage();
    return IntegerIdStorage.<EsCallSet>createIntegerIdStorage(mapStorage, 0);
  }

}
