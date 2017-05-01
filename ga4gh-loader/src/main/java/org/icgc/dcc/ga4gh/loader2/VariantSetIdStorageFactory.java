package org.icgc.dcc.ga4gh.loader2;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet.EsVariantSetSerializer;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IntegerIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.LongIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory;

import java.nio.file.Path;

import static org.mapdb.Serializer.INTEGER;
import static org.mapdb.Serializer.LONG;

@RequiredArgsConstructor
public class VariantSetIdStorageFactory implements IdStorageFactory<EsVariantSet> {

  private static final EsVariantSetSerializer ES_VARIANT_SET_SERIALIZER = new EsVariantSetSerializer();
  private static final boolean DEFAULT_PERSIST_FILE = false;

  public static VariantSetIdStorageFactory createVariantSetIdStorageFactory(Path outputDir, boolean useDisk) {
    return new VariantSetIdStorageFactory(outputDir, useDisk);
  }

  @NonNull private final Path outputDir;
  private final boolean useDisk;

  @Override public IntegerIdStorage<EsVariantSet> createIntegerIdStorage() {
    val factory = MapStorageFactory.<EsVariantSet, Integer>createMapStorageFactory("variantSetMapIntegerStorage",
        ES_VARIANT_SET_SERIALIZER, INTEGER,outputDir,DEFAULT_PERSIST_FILE);
     return IntegerIdStorage.<EsVariantSet>newIntegerIdStorage(factory.createMapStorage(useDisk), 0);
  }

  @Override public LongIdStorage<EsVariantSet> createLongIdStorage() {
    val factory = MapStorageFactory.<EsVariantSet, Long>createMapStorageFactory("variantSetMapLongStorage",
        ES_VARIANT_SET_SERIALIZER, LONG,outputDir,DEFAULT_PERSIST_FILE);
    return LongIdStorage.<EsVariantSet>newLongIdStorage(factory.createMapStorage(useDisk), 0L);
  }

}
