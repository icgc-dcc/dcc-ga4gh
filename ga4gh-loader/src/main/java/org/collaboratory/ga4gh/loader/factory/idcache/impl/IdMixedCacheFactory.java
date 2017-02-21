package org.collaboratory.ga4gh.loader.factory.idcache.impl;

import org.collaboratory.ga4gh.core.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.factory.idcache.AbstractIdCacheFactoryTemplate;
import org.mapdb.Serializer;

import java.io.IOException;

import static org.collaboratory.ga4gh.loader.utils.cache.storage.impl.DiskCacheStorage.newDiskCacheStorage;
import static org.collaboratory.ga4gh.loader.utils.cache.storage.impl.RamCacheStorage.newRamCacheStorage;

/*
 * Mix of disk cache and ram caches. 
 * EsVariant is diskCached, and the rest are ramCached  (since they dont need that much memory)
 */
public final class IdMixedCacheFactory extends AbstractIdCacheFactoryTemplate {

  private final String storageDirname;

  public IdMixedCacheFactory(final int initId, final String storageDirname) {
    super(initId);
    this.storageDirname = storageDirname;
  }

  @Override
  protected void buildCacheStorage() throws IOException {
    setVariantCacheStorage(newDiskCacheStorage("variantIdCache", new EsVariant.EsVariantSerializer(), Serializer.LONG,
        storageDirname, false));
    setVariantSetCacheStorage(newRamCacheStorage());
    setCallSetCacheStorage(newRamCacheStorage());
  }

}
