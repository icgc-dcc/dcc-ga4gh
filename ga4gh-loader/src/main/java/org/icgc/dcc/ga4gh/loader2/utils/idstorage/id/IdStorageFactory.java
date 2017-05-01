package org.icgc.dcc.ga4gh.loader2.utils.idstorage.id;

import lombok.SneakyThrows;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IntegerIdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.LongIdStorage;

public interface IdStorageFactory<K> {

  @SneakyThrows IntegerIdStorage<K> createIntegerIdStorage();

  @SneakyThrows LongIdStorage<K> createLongIdStorage();
}
