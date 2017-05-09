package org.icgc.dcc.ga4gh.loader2.factory.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader2.factory.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IntegerIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.VariantIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory;

import java.nio.file.Path;

import static java.lang.Boolean.TRUE;
import static org.icgc.dcc.ga4gh.loader.Config.DEFAULT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.Config.DEFAULT_PERSIST_MAPDB_FILE;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader2.utils.IntegerCounter2.createIntegerCounter2;
import static org.mapdb.Serializer.INTEGER;

@RequiredArgsConstructor
public class IntegerIdStorageFactory implements IdStorageFactory<Integer> {

  public static IntegerIdStorageFactory createIntegerIdStorageFactory(Path outputDir) {
    return new IntegerIdStorageFactory(outputDir);
  }

  @NonNull private final Path outputDir;

  private MapStorageFactory<EsVariant, IdStorageContext<Integer, EsCall>> createVariantMapStorageFactory(boolean persist){
    return MapStorageFactory.<EsVariant, IdStorageContext<Integer, EsCall>>createMapStorageFactory("variantIntegerMapStorage",
        ES_VARIANT_SERIALIZER, ID_STORAGE_CONTEXT_INTEGER_SERIALIZER,outputDir, VARIANT_MAPDB_ALLOCATION,
        persist);
  }

  private MapStorageFactory<EsVariantSet, Integer> createVariantSetMapStorageFactory(boolean persist){
    return MapStorageFactory.<EsVariantSet, Integer>createMapStorageFactory("variantSetIntegerMapStorage",
        ES_VARIANT_SET_SERIALIZER, INTEGER,outputDir, DEFAULT_MAPDB_ALLOCATION,
        persist);
  }

  private MapStorageFactory<EsCallSet, Integer> createCallSetMapStorageFactory(boolean persist){
    return MapStorageFactory.<EsCallSet, Integer>createMapStorageFactory("callSetIntegerMapStorage",
        ES_CALL_SET_SERIALIZER, INTEGER,outputDir, DEFAULT_MAPDB_ALLOCATION,
        persist);
  }

  @Override public IdStorage<EsVariantCallPair2, IdStorageContext<Integer, EsCall>> createVariantIdStorage(boolean useDisk) {
    val factory = createVariantMapStorageFactory(DEFAULT_PERSIST_MAPDB_FILE);
    val mapStorage = factory.createMapStorage(useDisk);
    val counter = createIntegerCounter2(0);
    return VariantIdStorage.<Integer>createVariantIdStorage(counter,mapStorage);
  }

  @Override public IdStorage<EsVariantSet, Integer> createVariantSetIdStorage(boolean useDisk) {
    val factory =  createVariantSetMapStorageFactory(DEFAULT_PERSIST_MAPDB_FILE);
    val mapStorage = factory.createMapStorage(useDisk);
    return IntegerIdStorage.<EsVariantSet>createIntegerIdStorage(mapStorage, 0);
  }

  @Override public IdStorage<EsCallSet, Integer> createCallSetIdStorage(boolean useDisk) {
    val factory    =  createCallSetMapStorageFactory(DEFAULT_PERSIST_MAPDB_FILE);
    val mapStorage = factory.createMapStorage(useDisk);
    return IntegerIdStorage.<EsCallSet>createIntegerIdStorage(mapStorage, 0);
  }

  @Override public IdStorage<EsVariantCallPair2, IdStorageContext<Integer, EsCall>> persistVariantIdStorage() {
    val factory = createVariantMapStorageFactory(TRUE);
    val mapStorage = factory.persistMapStorage();
    val counter = createIntegerCounter2(0);
    return VariantIdStorage.<Integer>createVariantIdStorage(counter,mapStorage);
  }

  @Override public IdStorage<EsVariantSet, Integer> persistVariantSetIdStorage() {
    val factory = createVariantSetMapStorageFactory(TRUE);
    val mapStorage = factory.persistMapStorage();
    return IntegerIdStorage.<EsVariantSet>createIntegerIdStorage(mapStorage, 0);
  }

  @Override public IdStorage<EsCallSet, Integer> persistCallSetIdStorage() {
    val factory = createCallSetMapStorageFactory(TRUE);
    val mapStorage = factory.persistMapStorage();
    return IntegerIdStorage.<EsCallSet>createIntegerIdStorage(mapStorage, 0);
  }


}
