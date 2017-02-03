package org.collaboratory.ga4gh.loader.factory;

import static org.collaboratory.ga4gh.loader.utils.IdDiskCache.newIdDiskCache;
import static org.collaboratory.ga4gh.loader.utils.IdRamCache.newIdRamCache;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.utils.IdCache;
import org.collaboratory.ga4gh.loader.utils.IdDiskCache;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

/*
 * Mix of disk cache and ram caches. 
 * EsVariant is diskCached, and the rest are ramCached  (since they dont need that much memory)
 */
@RequiredArgsConstructor
@Value
public class IdMixedCacheFactory implements IdCacheFactory {

  private final String storageDirname;
  private final long initId;

  // State
  @NonFinal
  private IdDiskCache<EsVariant> variantIdCache;

  @NonFinal
  private IdCache<String> variantSetIdCache;

  @NonFinal
  private IdCache<String> callSetIdCache;

  @Override
  public void build() throws IOException {
    variantIdCache =
        newIdDiskCache("variantIdCache", new EsVariant.EsVariantSerializer(), storageDirname,
            initId);
    variantSetIdCache = newIdRamCache(initId);
    callSetIdCache = newIdRamCache(initId);
  }

  @Override
  public void close() throws IOException {
    variantIdCache.close();
  }

  @Override
  public void purge() {
    variantSetIdCache.purge();
    variantIdCache.purge();
    callSetIdCache.purge();
  }

}
