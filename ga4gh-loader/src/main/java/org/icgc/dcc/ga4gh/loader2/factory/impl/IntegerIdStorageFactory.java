package org.icgc.dcc.ga4gh.loader2.factory.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVcfHeader;
import org.icgc.dcc.ga4gh.loader2.factory.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IntegerIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory;

import java.nio.file.Path;

import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IntegerIdStorage.newIntegerIdStorage;
import static org.mapdb.Serializer.INTEGER;

@RequiredArgsConstructor
public class IntegerIdStorageFactory implements IdStorageFactory<Integer> {

  private static final EsVariant2.EsVariantSerializer ES_VARIANT2_SERIALIZER = new EsVariant2.EsVariantSerializer();
  private static final EsVariantSet.EsVariantSetSerializer ES_VARIANT_SET_SERIALIZER = new EsVariantSet.EsVariantSetSerializer();
  private static final EsCallSet.EsCallSetSerializer ES_CALL_SET_SERIALIZER = new EsCallSet.EsCallSetSerializer();
  private static final boolean DEFAULT_PERSIST_FILE = true;

  public static IntegerIdStorageFactory createIntegerIdStorageFactory(Path outputDir) {
    return new IntegerIdStorageFactory(outputDir);
  }

  @NonNull private final Path outputDir;

  @Override public IdStorage<EsVariant2, Integer> createVariantIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsVariant2, Integer>createMapStorageFactory("variantMapIntegerStorage",
        ES_VARIANT2_SERIALIZER, INTEGER,outputDir,DEFAULT_PERSIST_FILE);
    return newIntegerIdStorage(factory.createMapStorage(useDisk), 0);
  }

  @Override public IdStorage<EsVariantSet, Integer> createVariantSetIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsVariantSet, Integer>createMapStorageFactory("variantSetMapIntegerStorage",
        ES_VARIANT_SET_SERIALIZER, INTEGER,outputDir,DEFAULT_PERSIST_FILE);
    return IntegerIdStorage.<EsVariantSet>newIntegerIdStorage(factory.createMapStorage(useDisk), 0);
  }

  @Override public IdStorage<EsCallSet, Integer> createCallSetIdStorage(boolean useDisk) {
    val factory = MapStorageFactory.<EsCallSet, Integer>createMapStorageFactory("callSetMapIntegerStorage",
        ES_CALL_SET_SERIALIZER, INTEGER,outputDir,DEFAULT_PERSIST_FILE);
    return IntegerIdStorage.<EsCallSet>newIntegerIdStorage(factory.createMapStorage(useDisk), 0);
  }

}
