package org.icgc.dcc.ga4gh.loader2;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet.EsCallSetSerializer;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IntegerIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.LongIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory;

import java.nio.file.Path;

import static org.mapdb.Serializer.INTEGER;
import static org.mapdb.Serializer.LONG;

@RequiredArgsConstructor
public class CallSetIdStorageFactory implements IdStorageFactory<EsCallSet> {

  private static final EsCallSetSerializer ES_CALL_SET_SERIALIZER = new EsCallSetSerializer();
  private static final boolean DEFAULT_PERSIST_FILE = false;

  public static CallSetIdStorageFactory createCallSetIdStorageFactory(Path outputDir, boolean useDisk) {
    return new CallSetIdStorageFactory(outputDir, useDisk);
  }

  @NonNull private final Path outputDir;
  private final boolean useDisk;

  @Override public IntegerIdStorage<EsCallSet> createIntegerIdStorage() {
    val factory = MapStorageFactory.<EsCallSet, Integer>createMapStorageFactory("callSetMapIntegerStorage",
        ES_CALL_SET_SERIALIZER, INTEGER,outputDir,DEFAULT_PERSIST_FILE);
     return IntegerIdStorage.<EsCallSet>newIntegerIdStorage(factory.createMapStorage(useDisk), 0);
  }

  @Override public LongIdStorage<EsCallSet> createLongIdStorage() {
    val factory = MapStorageFactory.<EsCallSet, Long>createMapStorageFactory("callSetMapLongStorage",
        ES_CALL_SET_SERIALIZER, LONG,outputDir,DEFAULT_PERSIST_FILE);
    return LongIdStorage.<EsCallSet>newLongIdStorage(factory.createMapStorage(useDisk), 0L);
  }

}
