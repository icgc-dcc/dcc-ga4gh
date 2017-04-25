package org.icgc.dcc.ga4gh.loader.factory.idcache.impl;

import org.icgc.dcc.ga4gh.common.resources.model.es.EsVariant;
import org.icgc.dcc.ga4gh.loader.factory.idcache.AbstractIdCacheFactoryTemplate;
import org.icgc.dcc.ga4gh.loader.utils.cache.storage.impl.DiskCacheStorage;
import org.icgc.dcc.ga4gh.loader.utils.cache.storage.impl.RamCacheStorage;
import org.mapdb.Serializer;

import java.io.IOException;

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
    setVariantCacheStorage(
        DiskCacheStorage.newDiskCacheStorage("variantIdCache", new EsVariant.EsVariantSerializer(), Serializer.LONG,
        storageDirname, false));
    setVariantSetCacheStorage(RamCacheStorage.newRamCacheStorage());
    setCallSetCacheStorage(RamCacheStorage.newRamCacheStorage());
  }

}
