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
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.LongIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.VariantIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory;
import org.mapdb.Serializer;

import java.nio.file.Path;

import static org.icgc.dcc.ga4gh.loader.Config.DEFAULT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.Config.DEFAULT_PERSIST_MAPDB_FILE;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader2.utils.LongCounter2.createLongCounter2;
import static org.mapdb.Serializer.LONG;

@RequiredArgsConstructor
public class LongIdStorageFactory implements IdStorageFactory<Long> {

  public static LongIdStorageFactory createLongIdStorageFactory(Path outputDir) {
    return new LongIdStorageFactory(outputDir);
  }

  @NonNull private final Path outputDir;

  private <K,V> MapStorage<K,V> createMapStorage(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean useDisk){
    val factory = MapStorageFactory.<K, V>createMapStorageFactory(name,
        keySerializer, valueSerializer,outputDir, DEFAULT_MAPDB_ALLOCATION, DEFAULT_PERSIST_MAPDB_FILE);
    return factory.createMapStorage(useDisk);
  }

  @Override public IdStorage<EsVariantCallPair2, IdStorageContext<Long, EsCall>> createVariantIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsVariant, IdStorageContext<Long, EsCall>>createMapStorageFactory("variantLongMapStorage",
        ES_VARIANT_SERIALIZER, ID_STORAGE_CONTEXT_LONG_SERIALIZER,outputDir,
        VARIANT_MAPDB_ALLOCATION, DEFAULT_PERSIST_MAPDB_FILE);
    val mapStorage = factory.createMapStorage(useDisk);
    val counter = createLongCounter2(0L);
    return VariantIdStorage.<Long>createVariantIdStorage(counter,mapStorage);
  }

  @Override public IdStorage<EsVariantSet, Long> createVariantSetIdStorage(boolean useDisk) {
    val mapStorage = createMapStorage("variantSetLongMapStorage", ES_VARIANT_SET_SERIALIZER,LONG, useDisk);
    return LongIdStorage.<EsVariantSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public IdStorage<EsCallSet, Long> createCallSetIdStorage(boolean useDisk) {
    val mapStorage = createMapStorage("callSetLongMapStorage", ES_CALL_SET_SERIALIZER,LONG, useDisk);
    return LongIdStorage.<EsCallSet>createLongIdStorage(mapStorage, 0L);
  }

}