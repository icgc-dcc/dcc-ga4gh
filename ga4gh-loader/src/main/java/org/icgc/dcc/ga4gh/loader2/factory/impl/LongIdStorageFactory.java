package org.icgc.dcc.ga4gh.loader2.factory.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader2.factory.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.LongIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory;

import java.nio.file.Path;

import static org.mapdb.Serializer.LONG;

@RequiredArgsConstructor
public class LongIdStorageFactory implements IdStorageFactory<Long> {

  private static final EsVariant2.EsVariantSerializer ES_VARIANT2_SERIALIZER = new EsVariant2.EsVariantSerializer();
  private static final EsVariantSet.EsVariantSetSerializer ES_VARIANT_SET_SERIALIZER = new EsVariantSet.EsVariantSetSerializer();
  private static final EsCallSet.EsCallSetSerializer ES_CALL_SET_SERIALIZER = new EsCallSet.EsCallSetSerializer();
  private static final boolean DEFAULT_PERSIST_FILE = true;

  public static LongIdStorageFactory createLongIdStorageFactory(Path outputDir) {
    return new LongIdStorageFactory(outputDir);
  }

  @NonNull private final Path outputDir;

  @Override public IdStorage<EsVariant2, Long> createVariantIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsVariant2, Long>createMapStorageFactory("variantMapLongStorage",
        ES_VARIANT2_SERIALIZER, LONG,outputDir,DEFAULT_PERSIST_FILE);
    return LongIdStorage.<EsVariant2>newLongIdStorage(factory.createMapStorage(useDisk), 0L);
  }

  @Override public IdStorage<EsVariantSet, Long> createVariantSetIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsVariantSet, Long>createMapStorageFactory("variantSetMapLongStorage",
        ES_VARIANT_SET_SERIALIZER, LONG,outputDir,DEFAULT_PERSIST_FILE);
    return LongIdStorage.<EsVariantSet>newLongIdStorage(factory.createMapStorage(useDisk), 0L);
  }

  @Override public IdStorage<EsCallSet, Long> createCallSetIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsCallSet, Long>createMapStorageFactory("callSetMapLongStorage",
        ES_CALL_SET_SERIALIZER, LONG,outputDir,DEFAULT_PERSIST_FILE);
    return LongIdStorage.<EsCallSet>newLongIdStorage(factory.createMapStorage(useDisk), 0L);
  }
}
