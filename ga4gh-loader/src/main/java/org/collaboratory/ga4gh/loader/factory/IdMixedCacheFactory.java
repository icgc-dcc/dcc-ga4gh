package org.collaboratory.ga4gh.loader.factory;

import static org.collaboratory.ga4gh.loader.utils.cache.impl.DiskCacheStorage.newDiskCacheStorage;
import static org.collaboratory.ga4gh.loader.utils.cache.impl.RamCacheStorage.newRamCacheStorage;

import java.io.IOException;

import org.collaboratory.ga4gh.core.model.es.EsVariant;
import org.mapdb.Serializer;

/*
 * Mix of disk cache and ram caches. 
 * EsVariant is diskCached, and the rest are ramCached  (since they dont need that much memory)
 */
public final class IdMixedCacheFactory extends AbstractIdCacheFactory {

  private final String storageDirname;

  public IdMixedCacheFactory(final int initId, final String storageDirname) {
    super(initId);
    this.storageDirname = storageDirname;
  }

  @Override
  protected void buildCacheStorage() throws IOException {
    variantCacheStorage = newDiskCacheStorage("variantIdCache", new EsVariant.EsVariantSerializer(), Serializer.LONG,
        storageDirname, false);
    variantSetCacheStorage = newRamCacheStorage();
    callSetCacheStorage = newRamCacheStorage();
  }

  @Override
  public void close() throws IOException {
    variantCacheStorage.close();
    variantSetCacheStorage.close();
    callSetCacheStorage.close();
  }

}
