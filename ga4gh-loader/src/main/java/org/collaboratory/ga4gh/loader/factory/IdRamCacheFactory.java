package org.collaboratory.ga4gh.loader.factory;

import static com.google.common.collect.Maps.newHashMap;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.utils.IdCache;
import org.collaboratory.ga4gh.loader.utils.IdRamCache;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

@RequiredArgsConstructor
@Value
public class IdRamCacheFactory implements IdCacheFactory {

  private final long initId;

  // State
  @NonFinal
  private IdCache<String> variantIdCache;
  @NonFinal
  private IdCache<String> variantSetIdCache;
  @NonFinal
  private IdCache<String> callSetIdCache;
  @NonFinal
  private IdCache<String> callIdCache;

  @Override
  public IdCacheFactory init() throws IOException {
    variantIdCache = newIdRamCache();
    variantSetIdCache = newIdRamCache();
    callSetIdCache = newIdRamCache();
    callIdCache = newIdRamCache();
    return this;
  }

  private IdCache<String> newIdRamCache() {
    return IdRamCache.newIdCache(newHashMap(), initId);
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
    callIdCache.purge();
  }
}
