package org.collaboratory.ga4gh.loader.factory;

import static com.google.common.collect.Maps.newHashMap;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.utils.IdCache;
import org.collaboratory.ga4gh.loader.utils.IdRamCache;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

@RequiredArgsConstructor
@Value
public class IdRamCacheFactory<T extends EsVariant> implements IdCacheFactory<T> {

  private final long initId;

  // State
  @NonFinal
  private IdCache<T> variantIdCache;
  @NonFinal
  private IdCache<String> variantSetIdCache;
  @NonFinal
  private IdCache<String> callSetIdCache;

  @Override
  public void build() throws IOException {
    variantIdCache = newIdRamCache(initId);
    variantSetIdCache = newIdRamCache(initId);
    callSetIdCache = newIdRamCache(initId);
  }

  private static <E> IdCache<E> newIdRamCache(final long initialId) {
    return IdRamCache.newIdCache(newHashMap(), initialId);
  }

  @Override
  public void close() throws IOException {
    // Empty since in ram
  }

  @Override
  public void purge() {
    variantIdCache.purge();
    variantSetIdCache.purge();
    callSetIdCache.purge();
  }
}
