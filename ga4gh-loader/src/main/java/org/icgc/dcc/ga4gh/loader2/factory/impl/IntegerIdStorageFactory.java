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
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext.IdStorageContextSerializer;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IntegerIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.VariantIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory;
import org.mapdb.Serializer;

import java.nio.file.Path;

import static org.icgc.dcc.ga4gh.loader2.utils.IntegerCounter2.createIntegerCounter2;
import static org.mapdb.Serializer.INTEGER;

@RequiredArgsConstructor
public class IntegerIdStorageFactory implements IdStorageFactory<Integer> {

  private static final EsVariant.EsVariantSerializer ES_VARIANT_SERIALIZER = new EsVariant.EsVariantSerializer();
  private static final EsVariantSet.EsVariantSetSerializer ES_VARIANT_SET_SERIALIZER = new EsVariantSet.EsVariantSetSerializer();
  private static final EsCallSet.EsCallSetSerializer ES_CALL_SET_SERIALIZER = new EsCallSet.EsCallSetSerializer();
  private static final EsCall.EsCallSerializer ES_CALL_SERIALIZER = new EsCall.EsCallSerializer();
  private static final IdStorageContextSerializer<Integer, EsCall> ID_STORAGE_CONTEXT_SERIALIZER = new IdStorageContextSerializer<>(Serializer.INTEGER,ES_CALL_SERIALIZER);
  private static final boolean DEFAULT_PERSIST_FILE = true;

  public static IntegerIdStorageFactory createIntegerIdStorageFactory(Path outputDir) {
    return new IntegerIdStorageFactory(outputDir);
  }

  @NonNull private final Path outputDir;

  private <K,V> MapStorage<K,V> createMapStorage(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean useDisk){
    val factory = MapStorageFactory.<K, V>createMapStorageFactory(name,
        keySerializer, valueSerializer,outputDir,DEFAULT_PERSIST_FILE);
    return factory.createMapStorage(useDisk);
  }

  @Override public IdStorage<EsVariantCallPair2, IdStorageContext<Integer, EsCall>> createVariantIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsVariant, IdStorageContext<Integer, EsCall>>createMapStorageFactory("variantIntegerMapStorage",
        ES_VARIANT_SERIALIZER, ID_STORAGE_CONTEXT_SERIALIZER,outputDir,DEFAULT_PERSIST_FILE);
    val mapStorage = factory.createMapStorage(useDisk);
    val counter = createIntegerCounter2(0);
    return VariantIdStorage.<Integer>createVariantIdStorage(counter,mapStorage);
  }

  @Override public IdStorage<EsVariantSet, Integer> createVariantSetIdStorage(boolean useDisk) {
    val mapStorage = createMapStorage("variantSetIntegerMapStorage", ES_VARIANT_SET_SERIALIZER,INTEGER, useDisk);
    return IntegerIdStorage.<EsVariantSet>createIntegerIdStorage(mapStorage, 0);
  }

  @Override public IdStorage<EsCallSet, Integer> createCallSetIdStorage(boolean useDisk) {
    val mapStorage = createMapStorage("callSetIntegerMapStorage", ES_CALL_SET_SERIALIZER,INTEGER, useDisk);
    return IntegerIdStorage.<EsCallSet>createIntegerIdStorage(mapStorage, 0);
  }


}
