package org.collaboratory.ga4gh.loader.factory;

import static org.collaboratory.ga4gh.loader.utils.IdRamCache.newIdRamCache;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.utils.IdCache;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

/*
 * All IdCaches are stored in memory
 */
@RequiredArgsConstructor
@Value
public class IdRamCacheFactory implements IdCacheFactory {

  private final long initId;

  // State
  @NonFinal
  private IdCache<EsVariant> variantIdCache;
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
